package mux

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/shared/management/proto"
)

// fakeMgmt is a management server with a fixed snapshot and a push channel
// for later updates. legacy disables SyncMappings.
type fakeMgmt struct {
	proto.UnimplementedProxyServiceServer

	snapshot []*proto.ProxyMapping
	legacy   bool
	push     chan *proto.ProxyMapping

	mu       sync.Mutex
	tokens   []string
	authReqs []*proto.AuthenticateRequest
	peerReqs []*proto.CreateProxyPeerRequest
	inits    []string
}

func (f *fakeMgmt) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.tokens = append(f.tokens, md.Get("authorization")...)
	f.mu.Unlock()
}

func (f *fakeMgmt) SyncMappings(stream proto.ProxyService_SyncMappingsServer) error {
	if f.legacy {
		return status.Error(codes.Unimplemented, "legacy")
	}
	f.record(stream.Context())
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.inits = append(f.inits, first.GetInit().GetAddress())
	f.mu.Unlock()

	send := func(resp *proto.SyncMappingsResponse) error {
		if err := stream.Send(resp); err != nil {
			return err
		}
		ack, err := stream.Recv()
		if err != nil {
			return err
		}
		if ack.GetAck() == nil {
			return status.Error(codes.InvalidArgument, "expected ack")
		}
		return nil
	}
	if err := send(&proto.SyncMappingsResponse{Mapping: f.snapshot, InitialSyncComplete: true}); err != nil {
		return err
	}
	for {
		select {
		case m := <-f.push:
			if err := send(&proto.SyncMappingsResponse{Mapping: []*proto.ProxyMapping{m}}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (f *fakeMgmt) GetMappingUpdate(req *proto.GetMappingUpdateRequest, stream proto.ProxyService_GetMappingUpdateServer) error {
	f.record(stream.Context())
	f.mu.Lock()
	f.inits = append(f.inits, req.GetAddress())
	f.mu.Unlock()
	if err := stream.Send(&proto.GetMappingUpdateResponse{Mapping: f.snapshot, InitialSyncComplete: true}); err != nil {
		return err
	}
	for {
		select {
		case m := <-f.push:
			if err := stream.Send(&proto.GetMappingUpdateResponse{Mapping: []*proto.ProxyMapping{m}}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (f *fakeMgmt) Authenticate(ctx context.Context, req *proto.AuthenticateRequest) (*proto.AuthenticateResponse, error) {
	f.record(ctx)
	f.mu.Lock()
	f.authReqs = append(f.authReqs, req)
	f.mu.Unlock()
	return &proto.AuthenticateResponse{Success: true}, nil
}

func (f *fakeMgmt) CreateProxyPeer(ctx context.Context, req *proto.CreateProxyPeerRequest) (*proto.CreateProxyPeerResponse, error) {
	f.record(ctx)
	f.mu.Lock()
	f.peerReqs = append(f.peerReqs, req)
	f.mu.Unlock()
	return &proto.CreateProxyPeerResponse{Success: true}, nil
}

func (f *fakeMgmt) ValidateSession(ctx context.Context, req *proto.ValidateSessionRequest) (*proto.ValidateSessionResponse, error) {
	f.record(ctx)
	return &proto.ValidateSessionResponse{Valid: true, UserId: "user@" + req.GetDomain()}, nil
}

func serveFake(t *testing.T, f *fakeMgmt) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	proto.RegisterProxyServiceServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return "http://" + ln.Addr().String()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

type testToken string

func (t testToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}
func (testToken) RequireTransportSecurity() bool { return false }

func dialMux(t *testing.T, addr, token string) proto.ProxyServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(testToken(token)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewProxyServiceClient(conn)
}

func TestEndToEnd(t *testing.T) {
	a := &fakeMgmt{
		snapshot: []*proto.ProxyMapping{mapping("svc-a1", "acc-a", "app.kunde-a.de")},
		push:     make(chan *proto.ProxyMapping, 1),
	}
	b := &fakeMgmt{
		snapshot: []*proto.ProxyMapping{mapping("svc-b1", "acc-b", "app.kunde-b.de"), mapping("svc-b2", "acc-b", "git.kunde-b.de")},
		legacy:   true,
		push:     make(chan *proto.ProxyMapping, 1),
	}

	cfg := &Config{
		Listen:             freeAddr(t),
		ProxyToken:         "proxy-secret",
		InitialSyncTimeout: 5 * time.Second,
		Managements: []Management{
			{Name: "a", Address: serveFake(t, a), Token: "token-a", PeerURL: "https://mgmt-a.example.com:443"},
			{Name: "b", Address: serveFake(t, b), Token: "token-b", PeerURL: "https://mgmt-b.example.com:443"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- Run(ctx, cfg, log.NewEntry(log.StandardLogger())) }()
	t.Cleanup(func() {
		cancel()
		<-runDone
	})

	client := dialMux(t, cfg.Listen, "proxy-secret")

	// The proxy connects with SyncMappings and acks every batch.
	var stream proto.ProxyService_SyncMappingsClient
	require.Eventually(t, func() bool {
		var err error
		stream, err = client.SyncMappings(ctx)
		if err != nil {
			return false
		}
		return stream.Send(&proto.SyncMappingsRequest{Msg: &proto.SyncMappingsRequest_Init{
			Init: &proto.SyncMappingsInit{ProxyId: "proxy-1", Address: "proxy.example.com"},
		}}) == nil
	}, 3*time.Second, 50*time.Millisecond)

	recv := func() *proto.SyncMappingsResponse {
		t.Helper()
		msg, err := stream.Recv()
		require.NoError(t, err)
		require.NoError(t, stream.Send(&proto.SyncMappingsRequest{Msg: &proto.SyncMappingsRequest_Ack{Ack: &proto.SyncMappingsAck{}}}))
		return msg
	}

	snap := recv()
	assert.True(t, snap.GetInitialSyncComplete())
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"svc-a1": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"svc-b1": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"svc-b2": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
	}, ids(snap.GetMapping()))

	// Managements received the proxy's own init and their own token.
	a.mu.Lock()
	assert.Equal(t, []string{"proxy.example.com"}, a.inits)
	assert.Contains(t, a.tokens, "Bearer token-a")
	a.mu.Unlock()
	b.mu.Lock()
	assert.Equal(t, []string{"proxy.example.com"}, b.inits, "legacy management via GetMappingUpdate")
	assert.Contains(t, b.tokens, "Bearer token-b")
	b.mu.Unlock()

	// Unary calls are routed by account.
	_, err := client.Authenticate(ctx, &proto.AuthenticateRequest{Id: "svc-b1", AccountId: "acc-b"})
	require.NoError(t, err)
	b.mu.Lock()
	require.Len(t, b.authReqs, 1)
	b.mu.Unlock()
	a.mu.Lock()
	assert.Empty(t, a.authReqs)
	a.mu.Unlock()

	// CreateProxyPeer announces the account's management URL.
	var hdr metadata.MD
	_, err = client.CreateProxyPeer(ctx, &proto.CreateProxyPeerRequest{ServiceId: "svc-a1", AccountId: "acc-a"}, grpc.Header(&hdr))
	require.NoError(t, err)
	assert.Equal(t, []string{"https://mgmt-a.example.com:443"}, hdr.Get(ManagementURLHeader))
	a.mu.Lock()
	require.Len(t, a.peerReqs, 1)
	a.mu.Unlock()

	// Domain-routed calls.
	resp, err := client.ValidateSession(ctx, &proto.ValidateSessionRequest{Domain: "git.kunde-b.de"})
	require.NoError(t, err)
	assert.Equal(t, "user@git.kunde-b.de", resp.GetUserId())

	_, err = client.Authenticate(ctx, &proto.AuthenticateRequest{AccountId: "acc-unknown"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	// Incremental updates flow through.
	a.push <- removed("svc-a1")
	upd := recv()
	assert.False(t, upd.GetInitialSyncComplete())
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{"svc-a1": proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED}, ids(upd.GetMapping()))

	b.push <- mapping("svc-b3", "acc-b", "wiki.kunde-b.de")
	upd = recv()
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{"svc-b3": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED}, ids(upd.GetMapping()))

	// Wrong proxy token is rejected.
	_, err = dialMux(t, cfg.Listen, "wrong").Authenticate(ctx, &proto.AuthenticateRequest{AccountId: "acc-b"})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
