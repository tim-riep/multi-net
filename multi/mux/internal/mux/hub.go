package mux

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	gproto "google.golang.org/protobuf/proto"

	"github.com/netbirdio/netbird/shared/management/proto"
)

// snapshotBatchSize caps the number of mappings per batch sent to the proxy.
const snapshotBatchSize = 100

// Hub holds the merged mapping state of all managements, the routing indexes
// derived from it, and the single downstream proxy session.
type Hub struct {
	log         *log.Entry
	syncTimeout time.Duration

	mu       sync.Mutex
	order    []string
	mgmts    map[string]*mgmtState
	services map[string]string // service ID -> management
	accounts map[string]*owner // account ID -> management
	domains  map[string]*owner // normalized domain -> management
	// lastOwner keeps the management of removed accounts and domains, so
	// late calls (access logs, status updates) are still routed.
	lastOwner map[string]string
	init      *proto.SyncMappingsInit
	// gen counts resync requests. Every proxy session bumps it and upstreams
	// reconnect on change: management mints the one-time auth tokens per
	// snapshot, so a session must never be served cached tokens.
	gen        uint64
	genChange  chan struct{} // closed and replaced whenever gen changes
	syncDone   chan struct{} // closed and replaced whenever a management completes a sync
	session    *session
	sessionSeq uint64
}

type owner struct {
	mgmt string
	refs int
}

type mgmtState struct {
	name        string
	peerURL     string
	services    map[string]*proto.ProxyMapping
	connected   bool
	synced      bool
	inSnapshot  bool
	snapshotIDs map[string]struct{}
	streamGen   uint64 // gen the current stream was opened for
	syncedGen   uint64 // gen of the last completed snapshot
	lastErr     string
	lastSync    time.Time
	rejected    map[string]struct{} // service IDs currently rejected for conflicts
}

type session struct {
	id     uint64
	cancel context.CancelFunc
	live   bool
	queue  []*proto.ProxyMapping
	notify chan struct{}
}

// NewHub creates a hub for the given managements.
func NewHub(logger *log.Entry, mgmts []Management, syncTimeout time.Duration) *Hub {
	h := &Hub{
		log:         logger,
		syncTimeout: syncTimeout,
		mgmts:       make(map[string]*mgmtState, len(mgmts)),
		services:    make(map[string]string),
		accounts:    make(map[string]*owner),
		domains:     make(map[string]*owner),
		lastOwner:   make(map[string]string),
		genChange:   make(chan struct{}),
		syncDone:    make(chan struct{}),
	}
	for _, m := range mgmts {
		h.order = append(h.order, m.Name)
		h.mgmts[m.Name] = &mgmtState{
			name:     m.Name,
			peerURL:  m.PeerURL,
			services: make(map[string]*proto.ProxyMapping),
			rejected: make(map[string]struct{}),
		}
	}
	return h
}

// SetProxyInit records the proxy's identity and capabilities. Upstreams use
// it when they reconnect for the session that follows.
func (h *Hub) SetProxyInit(in *proto.SyncMappingsInit) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.init = gproto.Clone(in).(*proto.SyncMappingsInit)
}

// ProxyInit returns the current proxy init, the resync generation and a
// channel closed when the generation changes. The init is nil until the
// proxy first connects.
func (h *Hub) ProxyInit() (*proto.SyncMappingsInit, uint64, <-chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.init == nil {
		return nil, h.gen, h.genChange
	}
	return gproto.Clone(h.init).(*proto.SyncMappingsInit), h.gen, h.genChange
}

func (h *Hub) bumpGenLocked() uint64 {
	h.gen++
	close(h.genChange)
	h.genChange = make(chan struct{})
	return h.gen
}

// UpstreamConnected starts a new snapshot phase for a management whose
// stream was opened for resync generation gen.
func (h *Hub) UpstreamConnected(name string, gen uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.mgmts[name]
	st.streamGen = gen
	st.connected = true
	clear(st.rejected)
	st.inSnapshot = true
	st.snapshotIDs = make(map[string]struct{})
	st.lastErr = ""
}

// UpstreamDisconnected marks a management offline. Its mappings are kept so
// the proxy keeps serving them until the management resyncs.
func (h *Hub) UpstreamDisconnected(name string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.mgmts[name]
	st.connected = false
	st.inSnapshot = false
	st.snapshotIDs = nil
	if err != nil {
		st.lastErr = err.Error()
	}
}

// ApplyBatch merges a batch received from a management and forwards the
// resulting changes to the proxy.
func (h *Hub) ApplyBatch(name string, mappings []*proto.ProxyMapping, initialSyncComplete bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	st := h.mgmts[name]
	var out []*proto.ProxyMapping
	for _, m := range mappings {
		if st.inSnapshot && m.GetType() != proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED {
			st.snapshotIDs[m.GetId()] = struct{}{}
		}
		out = append(out, h.applyLocked(st, m)...)
	}

	if initialSyncComplete && st.inSnapshot {
		for _, id := range sortedKeys(st.services) {
			if _, ok := st.snapshotIDs[id]; !ok {
				out = append(out, h.removeLocked(st, id))
			}
		}
		st.inSnapshot = false
		st.snapshotIDs = nil
		st.synced = true
		st.syncedGen = st.streamGen
		st.lastSync = time.Now()
		h.log.WithField("management", name).Infof("initial sync complete, %d services", len(st.services))
		close(h.syncDone)
		h.syncDone = make(chan struct{})
	}

	h.publishLocked(out)
}

func (h *Hub) applyLocked(st *mgmtState, m *proto.ProxyMapping) []*proto.ProxyMapping {
	id := m.GetId()
	logger := h.log.WithFields(log.Fields{"management": st.name, "service_id": id, "domain": m.GetDomain()})
	if id == "" {
		logger.Warn("ignoring mapping without service ID")
		return nil
	}

	if m.GetType() == proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED {
		delete(st.rejected, id)
		if _, ok := st.services[id]; !ok {
			return nil
		}
		return []*proto.ProxyMapping{h.removeLocked(st, id)}
	}

	if other, ok := h.services[id]; ok && other != st.name {
		logger.Errorf("service ID already provided by management %q, rejecting", other)
		st.rejected[id] = struct{}{}
		return nil
	}
	if o, ok := h.accounts[m.GetAccountId()]; ok && o.mgmt != st.name {
		logger.Errorf("account %s already provided by management %q, rejecting", m.GetAccountId(), o.mgmt)
		st.rejected[id] = struct{}{}
		return nil
	}
	if d := normalizeDomain(m.GetDomain()); d != "" {
		if o, ok := h.domains[d]; ok && o.mgmt != st.name {
			logger.Errorf("domain already served for management %q, rejecting", o.mgmt)
			st.rejected[id] = struct{}{}
			return nil
		}
	}

	delete(st.rejected, id)
	stored := gproto.Clone(m).(*proto.ProxyMapping)
	stored.Type = proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED

	// Forward the type management sent: a resync snapshot arrives as CREATED,
	// which is what makes the proxy (re)create a peer that failed before.
	// Every mapping carries a fresh one-time token, so none is skipped.
	fwd := gproto.Clone(m).(*proto.ProxyMapping)
	if old, ok := st.services[id]; ok {
		h.unindexLocked(old)
	} else if fwd.GetType() == proto.ProxyMappingUpdateType_UPDATE_TYPE_MODIFIED {
		fwd.Type = proto.ProxyMappingUpdateType_UPDATE_TYPE_CREATED
	}
	st.services[id] = stored
	h.indexLocked(st.name, stored)
	return []*proto.ProxyMapping{fwd}
}

func (h *Hub) removeLocked(st *mgmtState, id string) *proto.ProxyMapping {
	old := st.services[id]
	delete(st.services, id)
	h.unindexLocked(old)
	fwd := gproto.Clone(old).(*proto.ProxyMapping)
	fwd.Type = proto.ProxyMappingUpdateType_UPDATE_TYPE_REMOVED
	return fwd
}

func (h *Hub) indexLocked(mgmt string, m *proto.ProxyMapping) {
	h.services[m.GetId()] = mgmt
	addRef(h.accounts, m.GetAccountId(), mgmt)
	addRef(h.domains, normalizeDomain(m.GetDomain()), mgmt)
}

func (h *Hub) unindexLocked(m *proto.ProxyMapping) {
	delete(h.services, m.GetId())
	h.dropRefLocked(h.accounts, "account:"+m.GetAccountId(), m.GetAccountId())
	h.dropRefLocked(h.domains, "domain:"+normalizeDomain(m.GetDomain()), normalizeDomain(m.GetDomain()))
}

func addRef(idx map[string]*owner, key, mgmt string) {
	if key == "" {
		return
	}
	if o, ok := idx[key]; ok {
		o.refs++
		return
	}
	idx[key] = &owner{mgmt: mgmt, refs: 1}
}

func (h *Hub) dropRefLocked(idx map[string]*owner, lastKey, key string) {
	if o, ok := idx[key]; ok {
		if o.refs--; o.refs <= 0 {
			delete(idx, key)
			h.lastOwner[lastKey] = o.mgmt
		}
	}
}

func (h *Hub) publishLocked(out []*proto.ProxyMapping) {
	s := h.session
	if len(out) == 0 || s == nil || !s.live {
		return
	}
	s.queue = append(s.queue, out...)
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// RunSession streams the merged state to the proxy: a full snapshot once
// every management completed a fresh sync for this session (or the initial
// sync timeout passed), then incremental changes. A newer session cancels
// this one.
func (h *Hub) RunSession(ctx context.Context, send func(batch []*proto.ProxyMapping, complete bool) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h.mu.Lock()
	if h.session != nil {
		h.session.cancel()
	}
	h.sessionSeq++
	s := &session{id: h.sessionSeq, cancel: cancel, notify: make(chan struct{}, 1)}
	h.session = s
	gen := h.bumpGenLocked()
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		if h.session == s {
			h.session = nil
		}
		h.mu.Unlock()
	}()

	if err := h.waitSynced(ctx, gen); err != nil {
		return err
	}

	h.mu.Lock()
	snapshot := h.snapshotLocked()
	s.live = true
	h.mu.Unlock()

	if err := sendChunked(snapshot, true, send); err != nil {
		return err
	}
	h.log.WithField("session", s.id).Infof("sent snapshot with %d services to proxy", len(snapshot))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.notify:
		}
		h.mu.Lock()
		pending := s.queue
		s.queue = nil
		h.mu.Unlock()
		if err := sendChunked(pending, false, send); err != nil {
			return err
		}
	}
}

// waitSynced blocks until every management completed a sync opened for gen,
// so the snapshot carries one-time tokens management has not handed out yet.
func (h *Hub) waitSynced(ctx context.Context, gen uint64) error {
	timer := time.NewTimer(h.syncTimeout)
	defer timer.Stop()
	for {
		h.mu.Lock()
		pending := h.unsyncedLocked(gen)
		done := h.syncDone
		h.mu.Unlock()
		if len(pending) == 0 {
			return nil
		}
		select {
		case <-done:
		case <-timer.C:
			for _, name := range pending {
				h.log.WithField("management", name).Warn("no fresh sync yet, proxy snapshot uses its cached mappings")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *Hub) unsyncedLocked(gen uint64) []string {
	var pending []string
	for _, name := range h.order {
		if h.mgmts[name].syncedGen < gen {
			pending = append(pending, name)
		}
	}
	return pending
}

func (h *Hub) snapshotLocked() []*proto.ProxyMapping {
	var out []*proto.ProxyMapping
	for _, name := range h.order {
		st := h.mgmts[name]
		for _, id := range sortedKeys(st.services) {
			out = append(out, gproto.Clone(st.services[id]).(*proto.ProxyMapping))
		}
	}
	return out
}

func sendChunked(mappings []*proto.ProxyMapping, complete bool, send func([]*proto.ProxyMapping, bool) error) error {
	if len(mappings) == 0 {
		if complete {
			return send(nil, true)
		}
		return nil
	}
	for start := 0; start < len(mappings); start += snapshotBatchSize {
		end := min(start+snapshotBatchSize, len(mappings))
		if err := send(mappings[start:end], complete && end == len(mappings)); err != nil {
			return err
		}
	}
	return nil
}

// MgmtForAccount returns the management owning an account.
func (h *Hub) MgmtForAccount(accountID string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if o, ok := h.accounts[accountID]; ok {
		return o.mgmt, true
	}
	if name, ok := h.lastOwner["account:"+accountID]; ok {
		return name, true
	}
	return h.soleLocked()
}

// MgmtForDomain returns the management owning a domain (port and case ignored).
func (h *Hub) MgmtForDomain(domain string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := normalizeDomain(domain)
	if o, ok := h.domains[d]; ok {
		return o.mgmt, true
	}
	if name, ok := h.lastOwner["domain:"+d]; ok {
		return name, true
	}
	return h.soleLocked()
}

// soleLocked routes unknown keys to the only management, if there is one.
func (h *Hub) soleLocked() (string, bool) {
	if len(h.order) == 1 {
		return h.order[0], true
	}
	return "", false
}

// MgmtStatus is a management's state as reported on the status page.
type MgmtStatus struct {
	Name      string    `json:"name"`
	Connected bool      `json:"connected"`
	Synced    bool      `json:"synced"`
	Services  int       `json:"services"`
	Rejected  int       `json:"rejected_mappings"`
	LastSync  time.Time `json:"last_sync,omitzero"`
	LastError string    `json:"last_error,omitempty"`
}

// Status reports the state of all managements.
func (h *Hub) Status() []MgmtStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]MgmtStatus, 0, len(h.order))
	for _, name := range h.order {
		st := h.mgmts[name]
		out = append(out, MgmtStatus{
			Name:      name,
			Connected: st.connected,
			Synced:    st.synced,
			Services:  len(st.services),
			Rejected:  len(st.rejected),
			LastSync:  st.lastSync,
			LastError: st.lastErr,
		})
	}
	return out
}

func normalizeDomain(d string) string {
	d = strings.ToLower(strings.TrimSpace(d))
	if host, _, err := net.SplitHostPort(d); err == nil {
		d = host
	}
	return strings.TrimSuffix(d, ".")
}

func sortedKeys(m map[string]*proto.ProxyMapping) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
