package mux

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/shared/management/proto"
)

// ManagementURLHeader must match roundtrip.ManagementURLHeader in the
// patched proxy.
const ManagementURLHeader = "x-nb-management-url"

// Service implements the management ProxyService towards the proxy and
// routes every call to the management owning the account or domain.
type Service struct {
	proto.UnimplementedProxyServiceServer

	hub       *Hub
	upstreams map[string]*Upstream
	peerURLs  map[string]string
	log       *log.Entry
}

// NewService creates the proxy-facing service.
func NewService(hub *Hub, upstreams map[string]*Upstream, mgmts []Management, logger *log.Entry) *Service {
	peerURLs := make(map[string]string, len(mgmts))
	for _, m := range mgmts {
		peerURLs[m.Name] = m.PeerURL
	}
	return &Service{hub: hub, upstreams: upstreams, peerURLs: peerURLs, log: logger}
}

func (s *Service) byAccount(accountID string) (*Upstream, error) {
	name, ok := s.hub.MgmtForAccount(accountID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no management serves account %q", accountID)
	}
	return s.upstreams[name], nil
}

func (s *Service) byDomain(domain string) (*Upstream, error) {
	name, ok := s.hub.MgmtForDomain(domain)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no management serves domain %q", domain)
	}
	return s.upstreams[name], nil
}

// SyncMappings streams the merged mappings with per-batch acks.
func (s *Service) SyncMappings(stream proto.ProxyService_SyncMappingsServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "first message must be init")
	}
	s.hub.SetProxyInit(init)
	s.log.WithFields(log.Fields{"proxy_id": init.GetProxyId(), "version": init.GetVersion()}).Info("proxy connected (SyncMappings)")

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	acks := make(chan struct{})
	go func() {
		defer cancel()
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			if msg.GetAck() == nil {
				continue
			}
			select {
			case acks <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()

	err = s.hub.RunSession(ctx, func(batch []*proto.ProxyMapping, complete bool) error {
		if err := stream.Send(&proto.SyncMappingsResponse{Mapping: batch, InitialSyncComplete: complete}); err != nil {
			return err
		}
		select {
		case <-acks:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	return streamResult(stream.Context(), err)
}

// GetMappingUpdate streams the merged mappings for proxies without SyncMappings.
func (s *Service) GetMappingUpdate(req *proto.GetMappingUpdateRequest, stream proto.ProxyService_GetMappingUpdateServer) error {
	s.hub.SetProxyInit(&proto.SyncMappingsInit{
		ProxyId:      req.GetProxyId(),
		Version:      req.GetVersion(),
		StartedAt:    req.GetStartedAt(),
		Address:      req.GetAddress(),
		Capabilities: req.GetCapabilities(),
	})
	s.log.WithFields(log.Fields{"proxy_id": req.GetProxyId(), "version": req.GetVersion()}).Info("proxy connected (GetMappingUpdate)")

	err := s.hub.RunSession(stream.Context(), func(batch []*proto.ProxyMapping, complete bool) error {
		return stream.Send(&proto.GetMappingUpdateResponse{Mapping: batch, InitialSyncComplete: complete})
	})
	return streamResult(stream.Context(), err)
}

// streamResult hides the cancellation caused by a newer proxy session.
func streamResult(streamCtx context.Context, err error) error {
	if errors.Is(err, context.Canceled) && streamCtx.Err() == nil {
		return status.Error(codes.Aborted, "superseded by a newer proxy session")
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (s *Service) SendAccessLog(ctx context.Context, req *proto.SendAccessLogRequest) (*proto.SendAccessLogResponse, error) {
	u, err := s.byAccount(req.GetLog().GetAccountId())
	if err != nil {
		return nil, err
	}
	return u.Client().SendAccessLog(ctx, req)
}

func (s *Service) Authenticate(ctx context.Context, req *proto.AuthenticateRequest) (*proto.AuthenticateResponse, error) {
	u, err := s.byAccount(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	return u.Client().Authenticate(ctx, req)
}

func (s *Service) SendStatusUpdate(ctx context.Context, req *proto.SendStatusUpdateRequest) (*proto.SendStatusUpdateResponse, error) {
	u, err := s.byAccount(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	return u.Client().SendStatusUpdate(ctx, req)
}

// CreateProxyPeer additionally tells the proxy which management the
// account's embedded client must connect to.
func (s *Service) CreateProxyPeer(ctx context.Context, req *proto.CreateProxyPeerRequest) (*proto.CreateProxyPeerResponse, error) {
	name, ok := s.hub.MgmtForAccount(req.GetAccountId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no management serves account %q", req.GetAccountId())
	}
	if err := grpc.SetHeader(ctx, metadata.Pairs(ManagementURLHeader, s.peerURLs[name])); err != nil {
		return nil, fmt.Errorf("set management url header: %w", err)
	}
	return s.upstreams[name].Client().CreateProxyPeer(ctx, req)
}

func (s *Service) GetOIDCURL(ctx context.Context, req *proto.GetOIDCURLRequest) (*proto.GetOIDCURLResponse, error) {
	u, err := s.byAccount(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	return u.Client().GetOIDCURL(ctx, req)
}

func (s *Service) ValidateSession(ctx context.Context, req *proto.ValidateSessionRequest) (*proto.ValidateSessionResponse, error) {
	u, err := s.byDomain(req.GetDomain())
	if err != nil {
		return nil, err
	}
	return u.Client().ValidateSession(ctx, req)
}

func (s *Service) ValidateTunnelPeer(ctx context.Context, req *proto.ValidateTunnelPeerRequest) (*proto.ValidateTunnelPeerResponse, error) {
	u, err := s.byDomain(req.GetDomain())
	if err != nil {
		return nil, err
	}
	return u.Client().ValidateTunnelPeer(ctx, req)
}

func (s *Service) CheckLLMPolicyLimits(ctx context.Context, req *proto.CheckLLMPolicyLimitsRequest) (*proto.CheckLLMPolicyLimitsResponse, error) {
	u, err := s.byAccount(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	return u.Client().CheckLLMPolicyLimits(ctx, req)
}

func (s *Service) RecordLLMUsage(ctx context.Context, req *proto.RecordLLMUsageRequest) (*proto.RecordLLMUsageResponse, error) {
	u, err := s.byAccount(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	return u.Client().RecordLLMUsage(ctx, req)
}

// TokenAuth returns interceptors that require the proxy's bearer token.
// An empty token disables the check.
func TokenAuth(token string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	check := func(ctx context.Context) error {
		if token == "" {
			return nil
		}
		md, _ := metadata.FromIncomingContext(ctx)
		for _, v := range md.Get("authorization") {
			got, ok := strings.CutPrefix(v, "Bearer ")
			if ok && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
				return nil
			}
		}
		return status.Error(codes.Unauthenticated, "invalid proxy token")
	}
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := check(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}
