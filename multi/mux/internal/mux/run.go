package mux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/netbirdio/netbird/shared/management/proto"
)

// Run starts the multiplexer and blocks until ctx is canceled.
func Run(ctx context.Context, cfg *Config, logger *log.Entry) error {
	hub := NewHub(logger, cfg.Managements, cfg.InitialSyncTimeout)

	upstreams := make(map[string]*Upstream, len(cfg.Managements))
	for _, m := range cfg.Managements {
		u, err := DialUpstream(m, hub, logger)
		if err != nil {
			return fmt.Errorf("management %q: %w", m.Name, err)
		}
		defer u.Close()
		upstreams[m.Name] = u
	}

	unary, stream := TokenAuth(cfg.ProxyToken)
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(stream),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if cfg.TLSCertFile != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("load tls: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	srv := grpc.NewServer(opts...)
	proto.RegisterProxyServiceServer(srv, NewService(hub, upstreams, cfg.Managements, logger))

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, u := range upstreams {
		wg.Go(func() { u.Run(ctx) })
	}

	var statusSrv *http.Server
	if cfg.StatusListen != "" {
		statusSrv = &http.Server{Addr: cfg.StatusListen, Handler: statusHandler(hub), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := statusSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.WithError(err).Error("status server failed")
			}
		}()
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	logger.Infof("listening on %s for the proxy, %d managements", cfg.Listen, len(cfg.Managements))

	select {
	case <-ctx.Done():
	case err = <-serveErr:
	}
	cancel()
	if statusSrv != nil {
		_ = statusSrv.Close()
	}
	srv.Stop()
	wg.Wait()
	return err
}

func statusHandler(hub *Hub) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"managements": hub.Status()})
	})
	return mux
}
