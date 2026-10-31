package mux

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/netbirdio/netbird/shared/management/proto"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// TestSmokeRealProxy runs a real (patched) proxy binary against the
// multiplexer and two fake managements that hand out one-time tokens, then
// restarts only the proxy, which must create its peers again:
//
//	SMOKE_PROXY_BIN=/path/to/netbird-proxy SMOKE_CERT_DIR=/dir/with/tls.crt+tls.key go test -run Smoke ./internal/mux/
func TestSmokeRealProxy(t *testing.T) {
	bin := os.Getenv("SMOKE_PROXY_BIN")
	if bin == "" {
		t.Skip("SMOKE_PROXY_BIN not set")
	}
	a := &fakeMgmt{snapshot: []*proto.ProxyMapping{withPath(mapping("svc-a1", "acc-a", "app.kunde-a.de"))}, oneTime: true, push: make(chan *proto.ProxyMapping)}
	b := &fakeMgmt{snapshot: []*proto.ProxyMapping{withPath(mapping("svc-b1", "acc-b", "app.kunde-b.de"))}, oneTime: true, push: make(chan *proto.ProxyMapping)}
	cfg := &Config{
		Listen: freeAddr(t), ProxyToken: "proxy-secret", InitialSyncTimeout: 5 * time.Second,
		Managements: []Management{
			{Name: "a", Address: serveFake(t, a), Token: "token-a", PeerURL: "https://mgmt-a.invalid:443"},
			{Name: "b", Address: serveFake(t, b), Token: "token-b", PeerURL: "https://mgmt-b.invalid:443"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, cfg, log.NewEntry(log.StandardLogger())) }()

	var all strings.Builder
	for run := 1; run <= 2; run++ {
		logs := runSmokeProxy(ctx, t, bin, cfg.Listen)
		all.WriteString(logs)

		assert.Contains(t, logs, "Initial mapping sync complete", "run %d", run)
		assert.Contains(t, logs, "management_url: https://mgmt-a.invalid:443", "run %d", run)
		assert.Contains(t, logs, "management_url: https://mgmt-b.invalid:443", "run %d", run)
		assert.NotContains(t, logs, "invalid token", "run %d: proxy got a spent one-time token", run)
	}
	if p := os.Getenv("SMOKE_LOG"); p != "" {
		_ = os.WriteFile(p, []byte(all.String()), 0o644)
	}

	for name, f := range map[string]*fakeMgmt{"a": a, "b": b} {
		f.mu.Lock()
		assert.Len(t, f.peerReqs, 2, "%s: one peer per proxy start", name)
		assert.Contains(t, f.tokens, "Bearer token-"+name)
		f.mu.Unlock()
	}
}

// runSmokeProxy starts the proxy, waits until both accounts got their
// embedded client, then kills it and returns its logs.
func runSmokeProxy(ctx context.Context, t *testing.T, bin, muxAddr string) string {
	t.Helper()
	out := &syncBuf{}
	cmd := exec.CommandContext(ctx, bin, "--mgmt=http://"+muxAddr, "--addr="+freeAddr(t), "--health-addr="+freeAddr(t),
		"--cert-dir="+os.Getenv("SMOKE_CERT_DIR"), "--domain=proxy.example.com", "--log-level=debug")
	cmd.Env = append(os.Environ(), "NB_PROXY_TOKEN=proxy-secret", "NB_PROXY_ALLOW_INSECURE=true", "HOME="+t.TempDir())
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		s := out.String()
		done := strings.Contains(s, "mgmt-a.invalid") && strings.Contains(s, "mgmt-b.invalid")
		if done || strings.Contains(s, "invalid token") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return out.String()
}

func withPath(m *proto.ProxyMapping) *proto.ProxyMapping {
	m.Path = []*proto.PathMapping{{Path: "/", Target: "http://100.64.0.10:8080"}}
	return m
}
