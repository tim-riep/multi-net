package mux

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/shared/management/proto"
)

// Upstream is the connection to one management server.
type Upstream struct {
	cfg    Management
	hub    *Hub
	log    *log.Entry
	conn   *grpc.ClientConn
	client proto.ProxyServiceClient
}

// DialUpstream creates the gRPC client for a management. Connecting is lazy.
func DialUpstream(cfg Management, hub *Hub, logger *log.Entry) (*Upstream, error) {
	u, err := url.Parse(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("parse address: %w", err)
	}
	useTLS := u.Scheme == "https"
	creds := insecure.NewCredentials()
	if useTLS {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system cert pool: %w", err)
		}
		creds = credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient(u.Host,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithPerRPCCredentials(bearerToken{token: cfg.Token, requireTLS: useTLS}),
	)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	return &Upstream{
		cfg:    cfg,
		hub:    hub,
		log:    logger.WithField("management", cfg.Name),
		conn:   conn,
		client: proto.NewProxyServiceClient(conn),
	}, nil
}

// Client returns the management's ProxyService client for unary calls.
func (u *Upstream) Client() proto.ProxyServiceClient { return u.client }

// Close closes the gRPC connection.
func (u *Upstream) Close() error { return u.conn.Close() }

// Run keeps the mapping stream to the management alive until ctx ends.
func (u *Upstream) Run(ctx context.Context) {
	bo := &backoff.ExponentialBackOff{
		InitialInterval:     800 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.7,
		MaxInterval:         10 * time.Second,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	syncSupported := true

	op := func() error {
		init, gen, changed := u.hub.ProxyInit()
		for init == nil {
			// Managements need the proxy's identity, so wait for it to connect.
			select {
			case <-changed:
			case <-ctx.Done():
				return backoff.Permanent(ctx.Err())
			}
			init, gen, changed = u.hub.ProxyInit()
		}

		connected := false
		var err error
		if syncSupported {
			err = u.syncMappings(ctx, init, gen, &connected)
			if status.Code(err) == codes.Unimplemented {
				syncSupported = false
				u.log.Info("management does not support SyncMappings, falling back to GetMappingUpdate")
				err = u.getMappingUpdate(ctx, init, gen, &connected)
			}
		} else {
			err = u.getMappingUpdate(ctx, init, gen, &connected)
		}
		u.hub.UpstreamDisconnected(u.cfg.Name, err)
		if connected {
			bo.Reset()
		}
		if ctx.Err() != nil {
			return backoff.Permanent(ctx.Err())
		}
		if err == nil {
			return errors.New("stream closed by management")
		}
		return err
	}
	notify := func(err error, next time.Duration) {
		u.log.Warnf("mapping stream ended, reconnecting in %s: %v", next.Truncate(time.Millisecond), err)
	}
	_ = backoff.RetryNotify(op, backoff.WithContext(bo, ctx), notify)
}

// streamContext is canceled when the resync generation changes, so every
// proxy session gets a fresh snapshot with unused one-time tokens.
func (u *Upstream) streamContext(ctx context.Context, gen uint64) (context.Context, context.CancelFunc) {
	sctx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			_, cur, changed := u.hub.ProxyInit()
			if cur != gen {
				cancel()
				return
			}
			select {
			case <-changed:
			case <-sctx.Done():
				return
			}
		}
	}()
	return sctx, cancel
}

func (u *Upstream) syncMappings(ctx context.Context, init *proto.SyncMappingsInit, gen uint64, connected *bool) error {
	sctx, cancel := u.streamContext(ctx, gen)
	defer cancel()

	stream, err := u.client.SyncMappings(sctx)
	if err != nil {
		return fmt.Errorf("open sync stream: %w", err)
	}
	if err := stream.Send(&proto.SyncMappingsRequest{Msg: &proto.SyncMappingsRequest_Init{Init: init}}); err != nil {
		// Send only reports io.EOF; the real status (e.g. Unimplemented) comes from Recv.
		if _, rerr := stream.Recv(); rerr != nil && !errors.Is(rerr, io.EOF) {
			return rerr
		}
		return fmt.Errorf("send init: %w", err)
	}

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		u.markConnected(connected, gen)
		u.hub.ApplyBatch(u.cfg.Name, msg.GetMapping(), msg.GetInitialSyncComplete())
		if err := stream.Send(&proto.SyncMappingsRequest{Msg: &proto.SyncMappingsRequest_Ack{Ack: &proto.SyncMappingsAck{}}}); err != nil {
			return fmt.Errorf("send ack: %w", err)
		}
	}
}

func (u *Upstream) getMappingUpdate(ctx context.Context, init *proto.SyncMappingsInit, gen uint64, connected *bool) error {
	sctx, cancel := u.streamContext(ctx, gen)
	defer cancel()

	stream, err := u.client.GetMappingUpdate(sctx, &proto.GetMappingUpdateRequest{
		ProxyId:      init.GetProxyId(),
		Version:      init.GetVersion(),
		StartedAt:    init.GetStartedAt(),
		Address:      init.GetAddress(),
		Capabilities: init.GetCapabilities(),
	})
	if err != nil {
		return fmt.Errorf("open mapping stream: %w", err)
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		u.markConnected(connected, gen)
		u.hub.ApplyBatch(u.cfg.Name, msg.GetMapping(), msg.GetInitialSyncComplete())
	}
}

// markConnected starts the snapshot phase on the first message of a stream;
// gRPC streams open lazily, so this is the first proof the management is up.
func (u *Upstream) markConnected(connected *bool, gen uint64) {
	if *connected {
		return
	}
	*connected = true
	u.hub.UpstreamConnected(u.cfg.Name, gen)
	u.log.Info("mapping stream established")
}

type bearerToken struct {
	token      string
	requireTLS bool
}

func (t bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token}, nil
}

func (t bearerToken) RequireTransportSecurity() bool { return t.requireTLS }
