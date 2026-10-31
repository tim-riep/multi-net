package mux

import (
	"context"
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

func syncAll(h *Hub, name string, ms ...*proto.ProxyMapping) {
	h.UpstreamConnected(name)
	h.ApplyBatch(name, ms, true)
}

func TestSnapshotMergesAllManagements(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))
	syncAll(h, "b", mapping("s2", "acc-b", "two.example.com"), mapping("s3", "acc-b", "three.example.com"))

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
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))
	ch := startSession(t, h)

	select {
	case <-ch:
		t.Fatal("snapshot sent before management b synced")
	case <-time.After(100 * time.Millisecond):
	}

	syncAll(h, "b", mapping("s2", "acc-b", "two.example.com"))
	b := next(t, ch)
	assert.True(t, b.complete)
	assert.Len(t, b.mappings, 2)
}

func TestSnapshotTimeoutSendsPartialState(t *testing.T) {
	h := newTestHub(t, "a", "b")
	h.syncTimeout = 50 * time.Millisecond
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))

	ch := startSession(t, h)
	b := next(t, ch)
	assert.True(t, b.complete)
	assert.Len(t, b.mappings, 1)

	// A late management arrives as incremental update.
	syncAll(h, "b", mapping("s2", "acc-b", "two.example.com"))
	b = next(t, ch)
	assert.False(t, b.complete)
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED}, ids(b.mappings))
}

func TestEmptySnapshotIsComplete(t *testing.T) {
	h := newTestHub(t, "a")
	syncAll(h, "a")
	b := next(t, startSession(t, h))
	assert.True(t, b.complete)
	assert.Empty(t, b.mappings)
}

func TestUpstreamResyncRemovesOnlyItsStaleServices(t *testing.T) {
	h := newTestHub(t, "a", "b")
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"), mapping("s2", "acc-a", "two.example.com"))
	syncAll(h, "b", mapping("s3", "acc-b", "three.example.com"))
	ch := startSession(t, h)
	next(t, ch)

	// a reconnects: s1 unchanged, s2 gone, s4 new; b must stay untouched.
	h.UpstreamDisconnected("a", nil)
	h.UpstreamConnected("a")
	h.ApplyBatch("a", []*proto.ProxyMapping{mapping("s1", "acc-a", "one.example.com")}, false)
	h.ApplyBatch("a", []*proto.ProxyMapping{mapping("s4", "acc-a", "four.example.com")}, true)

	var got []*proto.ProxyMapping
	for len(got) < 2 {
		got = append(got, next(t, ch).mappings...)
	}
	assert.Equal(t, map[string]proto.ProxyMappingUpdateType{
		"s4": proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED,
		"s2": proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED,
	}, ids(got))

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
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))
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
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))
	syncAll(h, "b",
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
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))
	syncAll(h, "b", mapping("s2", "acc-b", "two.example.com"))

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
	syncAll(h, "a", mapping("s1", "acc-a", "one.example.com"))

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
	syncAll(h, "a", ms...)

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
