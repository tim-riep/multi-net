package mux

import (
	"context"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/shared/management/proto"
)

func mapping(id, account, domain string) *proto.ProxyMapping {
	return &proto.ProxyMapping{Id: id, AccountId: account, Domain: domain, Mode: "http"}
}

func removed(id string) *proto.ProxyMapping {
	return &proto.ProxyMapping{Type: proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED, Id: id}
}

func newTestHub(t *testing.T, names ...string) *Hub {
	t.Helper()
	var mgmts []Management
	for _, n := range names {
		mgmts = append(mgmts, Management{Name: n, PeerURL: "https://" + n + ".example.com"})
	}
	return NewHub(log.NewEntry(log.StandardLogger()), mgmts, time.Second)
}

type batch struct {
	mappings []*proto.ProxyMapping
	complete bool
}

// startSession runs a downstream session and returns its batches.
func startSession(t *testing.T, h *Hub) <-chan batch {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out := make(chan batch, 100)
	go func() {
		_ = h.RunSession(ctx, func(m []*proto.ProxyMapping, complete bool) error {
			out <- batch{mappings: m, complete: complete}
			return nil
		})
	}()
	return out
}

func next(t *testing.T, ch <-chan batch) batch {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for batch")
		return batch{}
	}
}

func ids(ms []*proto.ProxyMapping) map[string]proto.ProxyMappingUpdateType {
	out := make(map[string]proto.ProxyMappingUpdateType, len(ms))
	for _, m := range ms {
		out[m.GetId()] = m.GetType()
	}
	return out
}

// syncAll acts as management name with snapshot ms: it syncs now and again
// on every resync request, like a real upstream reconnecting for a session.
func syncAll(t *testing.T, h *Hub, name string, ms ...*proto.ProxyMapping) {
	t.Helper()
	fakeUpstreamsMu.Lock()
	set, ok := fakeUpstreams[h]
	if !ok {
		set = &upstreamSet{snaps: make(map[string][]*proto.ProxyMapping)}
		fakeUpstreams[h] = set
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go set.watch(ctx, h)
	}
	fakeUpstreamsMu.Unlock()

	set.mu.Lock()
	set.snaps[name] = ms
	set.mu.Unlock()
	_, gen, _ := h.ProxyInit()
	resync(h, name, gen, ms)
}

var (
	fakeUpstreamsMu sync.Mutex
	fakeUpstreams   = map[*Hub]*upstreamSet{}
)

type upstreamSet struct {
	mu    sync.Mutex
	snaps map[string][]*proto.ProxyMapping
}

func (s *upstreamSet) watch(ctx context.Context, h *Hub) {
	for {
		_, _, changed := h.ProxyInit()
		select {
		case <-changed:
		case <-ctx.Done():
			return
		}
		_, gen, _ := h.ProxyInit()
		s.mu.Lock()
		snaps := make(map[string][]*proto.ProxyMapping, len(s.snaps))
		for k, v := range s.snaps {
			snaps[k] = v
		}
		s.mu.Unlock()
		for name, ms := range snaps {
			resync(h, name, gen, ms)
		}
	}
}

func resync(h *Hub, name string, gen uint64, ms []*proto.ProxyMapping) {
	h.UpstreamConnected(name, gen)
	h.ApplyBatch(name, ms, true)
}

func TestSnapshotMergesAllManagements(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))
	syncAll(t, h, "b", mapping("s2", "acc-b", "two.example.com"), mapping("s3", "acc-b", "three.example.com"))

	b := next(t, startSession(t, h))
	assert.True(t, b.complete)
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"s1": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s3": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
	}, ids(b.mappings))
}

func TestSnapshotWaitsForAllManagements(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))
	ch := startSession(t, h)

	select {
	case <-ch:
		t.Fatal("snapshot sent before management b synced")
	case <-time.After(100 * time.Millisecond):
	}

	syncAll(t, h, "b", mapping("s2", "acc-b", "two.example.com"))
	b := next(t, ch)
	assert.True(t, b.complete)
	assert.Len(t, b.mappings, 2)
}

func TestSnapshotTimeoutSendsPartialState(t *testing.T) {
	h := newTestHub(t, "a", "b")
	h.syncTimeout = 50 * time.Millisecond
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))

	ch := startSession(t, h)
	b := next(t, ch)
	assert.True(t, b.complete)
	assert.Len(t, b.mappings, 1)

	// A late management arrives as incremental update.
	syncAll(t, h, "b", mapping("s2", "acc-b", "two.example.com"))
	b = next(t, ch)
	assert.False(t, b.complete)
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED}, ids(b.mappings))
}

func TestEmptySnapshotIsComplete(t *testing.T) {
	h := newTestHub(t, "a")
	syncAll(t, h, "a")
	b := next(t, startSession(t, h))
	assert.True(t, b.complete)
	assert.Empty(t, b.mappings)
}

func TestUpstreamResyncRemovesOnlyItsStaleServices(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"), mapping("s2", "acc-a", "two.example.com"))
	syncAll(t, h, "b", mapping("s3", "acc-b", "three.example.com"))
	ch := startSession(t, h)
	next(t, ch)

	// a reconnects: s1 still there, s2 gone, s4 new; b must stay untouched.
	// s1 is forwarded as CREATED with its fresh one-time token, like
	// management itself does on reconnect.
	h.UpstreamDisconnected("a", nil)
	_, gen, _ := h.ProxyInit()
	h.UpstreamConnected("a", gen)
	h.ApplyBatch("a", []*proto.ProxyMapping{mapping("s1", "acc-a", "one.example.com")}, false)
	h.ApplyBatch("a", []*proto.ProxyMapping{mapping("s4", "acc-a", "four.example.com")}, true)

	var got []*proto.ProxyMapping
	for len(got) < 3 {
		got = append(got, next(t, ch).mappings...)
	}
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"s1": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s4": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED,
	}, ids(got), "resync forwards current services and removes only a's stale ones")

	// Removed domains still route to their last management (late calls).
	name, ok := h.MgmtForDomain("two.example.com")
	assert.True(t, ok)
	assert.Equal(t, "a", name)
	name, ok = h.MgmtForDomain("three.example.com")
	assert.True(t, ok)
	assert.Equal(t, "b", name)
}

func TestModifiedAndRemovedUpdates(t *testing.T) {
	h := newTestHub(t, "a")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))
	ch := startSession(t, h)
	next(t, ch)

	changed := mapping("s1", "acc-a", "one.example.com")
	changed.PassHostHeader = true
	changed.Type = proto.ProxyMappingUpdateType_UPDATE_TYPE_MODIFIED
	h.ApplyBatch("a", []*proto.ProxyMapping{changed}, false)
	b := next(t, ch)
	require.Len(t, b.mappings, 1)
	assert.Equal(t, proto.ProxyMappingUpdateType_UPDATE_TYPE_MODIFIED, b.mappings[0].GetType())
	assert.True(t, b.mappings[0].GetPassHostHeader())

	h.ApplyBatch("a", []*proto.ProxyMapping{removed("s1")}, false)
	b = next(t, ch)
	require.Len(t, b.mappings, 1)
	assert.Equal(t, proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED, b.mappings[0].GetType())
	assert.Equal(t, "acc-a", b.mappings[0].GetAccountId(), "removal carries the stored mapping")

	// MODIFIED for a service the proxy does not know yet must create it.
	unknown := mapping("s9", "acc-a", "nine.example.com")
	unknown.Type = proto.ProxyMappingUpdateType_UPDATE_TYPE_MODIFIED
	h.ApplyBatch("a", []*proto.ProxyMapping{unknown}, false)
	b = next(t, ch)
	require.Len(t, b.mappings, 1)
	assert.Equal(t, proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED, b.mappings[0].GetType(), "unknown MODIFIED becomes CREATED")

	// Removing an unknown service is not forwarded.
	h.ApplyBatch("a", []*proto.ProxyMapping{removed("nope")}, false)
	select {
	case b := <-ch:
		t.Fatalf("unexpected batch %v", b)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestConflictsAreRejected(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))
	syncAll(t, h, "b",
		mapping("s2", "acc-b", "ONE.example.com"),   // domain owned by a
		mapping("s3", "acc-a", "three.example.com"), // account owned by a
		mapping("s1", "acc-b", "four.example.com"),  // service ID owned by a
		mapping("s5", "acc-b", "five.example.com"),
	)

	b := next(t, startSession(t, h))
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"s1": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s5": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
	}, ids(b.mappings))
	assert.Equal(t, "acc-a", b.mappings[0].GetAccountId())

	for _, st := range h.Status() {
		if st.Name == "b" {
			assert.Equal(t, 3, st.Rejected)
		}
	}
}

func TestRouting(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))
	syncAll(t, h, "b", mapping("s2", "acc-b", "two.example.com"))

	for _, tc := range []struct{ key, want string }{
		{"acc-a", "a"}, {"acc-b", "b"},
	} {
		got, ok := h.MgmtForAccount(tc.key)
		assert.True(t, ok)
		assert.Equal(t, tc.want, got)
	}
	for _, tc := range []struct{ domain, want string }{
		{"one.example.com", "a"}, {"Two.Example.com:443", "b"}, {"two.example.com.", "b"},
	} {
		got, ok := h.MgmtForDomain(tc.domain)
		assert.True(t, ok, tc.domain)
		assert.Equal(t, tc.want, got, tc.domain)
	}
	_, ok := h.MgmtForAccount("unknown")
	assert.False(t, ok)
}

func TestSingleManagementRoutesUnknownKeys(t *testing.T) {
	h := newTestHub(t, "only")
	got, ok := h.MgmtForAccount("whatever")
	assert.True(t, ok)
	assert.Equal(t, "only", got)
}

func TestNewSessionSupersedesOld(t *testing.T) {
	h := newTestHub(t, "a")
	syncAll(t, h, "a", mapping("s1", "acc-a", "one.example.com"))

	done := make(chan error, 1)
	go func() {
		done <- h.RunSession(context.Background(), func([]*proto.ProxyMapping, bool) error { return nil })
	}()
	time.Sleep(50 * time.Millisecond)

	ch := startSession(t, h)
	assert.True(t, next(t, ch).complete)
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("old session not canceled")
	}
}

func TestLargeSnapshotIsChunked(t *testing.T) {
	h := newTestHub(t, "a")
	var ms []*proto.ProxyMapping
	for i := range 250 {
		ms = append(ms, mapping("s"+string(rune('A'+i/26))+string(rune('a'+i%26)), "acc-a", ""))
	}
	syncAll(t, h, "a", ms...)

	ch := startSession(t, h)
	total := 0
	for _, wantComplete := range []bool{false, false, true} {
		b := next(t, ch)
		assert.Equal(t, wantComplete, b.complete)
		total += len(b.mappings)
	}
	assert.Equal(t, 250, total)
}

func TestParseConfig(t *testing.T) {
	t.Setenv("TOKEN_A", "secret-a")
	cfg, err := ParseConfig([]byte(`
listen: 127.0.0.1:6000
initial_sync_timeout: 10s
managements:
  - name: a
    address: https://a.example.com:443
    token: ${TOKEN_A}
  - name: b
    address: http://b.internal:80
    token: plain
    peer_url: https://b.example.com:443
`))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.InitialSyncTimeout)
	assert.False(t, cfg.AllowDirectUpstream, "direct upstream is off unless enabled")
	assert.Equal(t, "secret-a", cfg.Managements[0].Token)
	assert.Equal(t, "https://a.example.com:443", cfg.Managements[0].PeerURL)
	assert.Equal(t, "https://b.example.com:443", cfg.Managements[1].PeerURL)

	for name, raw := range map[string]string{
		"no managements": `listen: ":1"`,
		"duplicate":      "managements: [{name: a, address: 'https://a', token: t}, {name: a, address: 'https://b', token: t}]",
		"no token":       "managements: [{name: a, address: 'https://a'}]",
		"bad scheme":     "managements: [{name: a, address: 'grpc://a', token: t}]",
	} {
		_, err := ParseConfig([]byte(raw))
		assert.Error(t, err, name)
	}
}

func directUpstream(m *proto.ProxyMapping) *proto.ProxyMapping {
	m.Path = []*proto.PathMapping{{Path: "/", Target: "http://10.0.0.5:80", Options: &proto.PathTargetOptions{DirectUpstream: true}}}
	return m
}

func privateMapping(m *proto.ProxyMapping) *proto.ProxyMapping {
	m.Private = true
	return m
}

func TestDirectUpstreamRejectedByDefault(t *testing.T) {
	h := newTestHub(t, "a")
	syncAll(t, h, "a",
		mapping("s1", "acc-a", "one.example.com"),
		privateMapping(mapping("s2", "acc-a", "two.example.com")),
		directUpstream(mapping("s3", "acc-a", "three.example.com")),
	)
	ch := startSession(t, h)
	b := next(t, ch)
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"s1": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
	}, ids(b.mappings), "NetBird-only services pass, direct upstream targets do not")
	assert.Equal(t, 1, h.Status()[0].Rejected, "rejected mappings are reported")

	// An accepted service later switched to direct upstream is withdrawn,
	// not left running with its previous configuration.
	changed := directUpstream(privateMapping(mapping("s2", "acc-a", "two.example.com")))
	changed.Type = proto.ProxyMappingUpdateType_UPDATE_TYPE_MODIFIED
	h.ApplyBatch("a", []*proto.ProxyMapping{changed}, false)
	b = next(t, ch)
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED,
	}, ids(b.mappings), "switching to direct upstream removes the service")
}

func TestDirectUpstreamAllowed(t *testing.T) {
	h := newTestHub(t, "a")
	h.allowDirectUpstream = true
	syncAll(t, h, "a", directUpstream(mapping("s3", "acc-a", "three.example.com")))
	b := next(t, startSession(t, h))
	assert.Len(t, b.mappings, 1, "allow_direct_upstream passes direct upstream targets through")
}

func TestPrivateCapabilityPassedThrough(t *testing.T) {
	on := true
	h := newTestHub(t, "a")
	h.SetProxyInit(&proto.SyncMappingsInit{ProxyId: "p", Capabilities: &proto.ProxyCapabilities{Private: &on}})
	got, _, _ := h.ProxyInit()
	assert.True(t, got.GetCapabilities().GetPrivate(), "managements see the proxy's private mode")
}
