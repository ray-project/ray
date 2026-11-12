// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeKVClient is a minimal in-memory gcs.Client used to observe KV writes.
// It embeds the gcs.Client interface so it satisfies the full interface while
// only overriding the operations exercised by the tests.
type fakeKVClient struct {
	gcs.Client
	kv map[string]map[string][]byte
}

func (f *fakeKVClient) Put(_ context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	if f.kv == nil {
		f.kv = map[string]map[string][]byte{}
	}
	if f.kv[ns] == nil {
		f.kv[ns] = map[string][]byte{}
	}
	f.kv[ns][key] = value
	return true, nil
}

func (f *fakeKVClient) Get(_ context.Context, ns, key string) ([]byte, error) {
	if v, ok := f.kv[ns][key]; ok {
		return v, nil
	}
	return nil, gcs.ErrKeyNotFound
}

type stubModule struct{ started bool }

func (s *stubModule) Start(ctx context.Context) error {
	s.started = true
	return nil
}

func (s *stubModule) RegisterHTTP(mux *http.ServeMux) error { return nil }
func (s *stubModule) RegisterGRPC(srv *grpc.Server) error   { return nil }

func TestRegisterAgentToGCS(t *testing.T) {
	fake := &fakeKVClient{}
	nodeID, err := ids.NodeIDFromHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c")
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	cfg := Config{
		NodeIP: "1.2.3.4",
		NodeID: nodeID,
		GCS:    fake,
	}
	if err := registerAgentToGCS(context.Background(), cfg, 8888, 8080); err != nil {
		t.Fatalf("registerAgentToGCS: %v", err)
	}
	// The two keys carry different shapes, mirroring Python agent.py:
	// NODE_ID_PREFIX:<id> -> [ip, http, grpc], IP_PREFIX:<ip> -> [id, http, grpc].
	// Head-side consumers resolve the missing field through the other key —
	// e.g. the dashboard head's log module reads vals[0] of the ip-keyed value
	// as a node id, so that slot must never carry the ip.
	if got, want := string(fake.kv[KVNamespaceDashboard][DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX+cfg.NodeID.Hex()]), `["1.2.3.4", 8080, 8888]`; got != want {
		t.Errorf("node-id key value = %q, want %q", got, want)
	}
	if got, want := string(fake.kv[KVNamespaceDashboard][DASHBOARD_AGENT_ADDR_IP_PREFIX+"1.2.3.4"]), fmt.Sprintf("[%q, 8080, 8888]", cfg.NodeID.Hex()); got != want {
		t.Errorf("ip key value = %q, want %q", got, want)
	}
}

func TestRunStartsAllModules(t *testing.T) {
	m1, m2 := &stubModule{}, &stubModule{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, Config{NodeID: ids.NewNodeID(), GCS: &fakeKVClient{}}, []Module{m1, m2}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !m1.started || !m2.started {
		t.Errorf("Run did not start both modules: m1=%v m2=%v", m1.started, m2.started)
	}
}

// TestNewHTTPServerBindsBeforeServe verifies the listen sockets are bound by the
// constructor (so they accept connections before the agent publishes its
// address in GCS) and that both the node-ip listener (head-side consumers dial
// the pod IP resolved through ports_by_node) and the loopback listener (the
// in-container readiness probe) accept connections before Serve is called.
func TestNewHTTPServerBindsBeforeServe(t *testing.T) {
	// The node IP must be an address assigned to this host, and non-loopback so
	// the loopback listener is created. 127.0.0.2 is a loopback-device address
	// that IsLocalhost does not treat as localhost, so it works even on hosts
	// without a routable non-loopback IPv4.
	nodeIP := nonLoopbackIPv4(t)
	if nodeIP == "" {
		nodeIP = "127.0.0.2"
	}
	httpSrv, err := newHTTPServer(Config{NodeIP: nodeIP, ListenPort: 0}, []Module{&stubModule{}})
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}
	defer func() {
		_ = httpSrv.shutdown(context.Background())
		_ = httpSrv.listener.Close()
	}()
	port := listenerPort(httpSrv.listener)
	if port <= 0 {
		t.Fatalf("listener port = %d, want a bound ephemeral port", port)
	}
	if got := httpSrv.listener.Addr().(*net.TCPAddr).IP.String(); got != nodeIP {
		t.Errorf("node-ip listener bound %s, want %s", got, nodeIP)
	}
	dialBeforeServe := func(addr string) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Errorf("dial %s before serve: %v", addr, err)
			return
		}
		conn.Close()
	}
	dialBeforeServe(fmt.Sprintf("%s:%d", nodeIP, port))
	if httpSrv.loopbackListener == nil {
		t.Fatalf("no loopback listener bound for the non-local node IP %s", nodeIP)
	}
	dialBeforeServe(httpSrv.loopbackListener.Addr().String())
}

// nonLoopbackIPv4 returns this host's first non-loopback IPv4 address, or ""
// when the host has none.
func nonLoopbackIPv4(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("InterfaceAddrs: %v", err)
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipNet.IP.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	return ""
}

// failingRegisterModule is a Module whose HTTP registration always fails,
// forcing the HTTP server construction to abort before the bind. It exercises
// the non-EADDRINUSE degrade branch of Run deterministically (a taken port
// falls back to a dynamic port instead of degrading).
type failingRegisterModule struct{}

func (failingRegisterModule) Start(context.Context) error       { return nil }
func (failingRegisterModule) RegisterHTTP(*http.ServeMux) error { return errors.New("boom") }
func (failingRegisterModule) RegisterGRPC(*grpc.Server) error   { return nil }

// TestRunDegradesWhenHTTPServerStartFails verifies the Python parity contract
// for an HTTP startup failure other than a taken port: the agent stays alive
// (Run returns nil on cancellation), the address is not registered in GCS, and
// module startup still proceeds.
func TestRunDegradesWhenHTTPServerStartFails(t *testing.T) {
	fake := &fakeKVClient{}
	mod := &stubModule{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			NodeIP: "127.0.0.1",
			GCS:    fake,
		}, []Module{failingRegisterModule{}, mod})
	}()

	// Give Run time to fail the HTTP startup and reach the serving state.
	time.Sleep(200 * time.Millisecond)
	if keys := fake.kv[KVNamespaceDashboard]; len(keys) != 0 {
		t.Errorf("agent address registered despite HTTP startup failure: %v", keys)
	}
	if !mod.started {
		t.Errorf("module startup skipped despite the HTTP degrade")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil (degraded, not failed)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestRunFallsBackToDynamicPortWhenListenPortTaken verifies that a taken fixed
// listen port falls back to a dynamic port instead of degrading to a disabled
// HTTP server (persisted -1): the Python and Go agents share the fixed default
// listen port when co-hosted, and Run registers the actually bound port in GCS
// InternalKV, which is the only channel carrying the dynamic port — the raylet
// waits for the persisted port files only when the configured port is 0.
func TestRunFallsBackToDynamicPortWhenListenPortTaken(t *testing.T) {
	occupied, err := net.Listen("tcp", ":52365")
	if err != nil {
		t.Skipf("cannot hold the fixed port on this machine: %v", err)
	}
	defer occupied.Close()

	cfg := testConfig(t)
	cfg.ListenPort = 52365

	// Run serves until cancelled; drive it from a goroutine and wait for the
	// port files before asserting (mirrors TestRunPersistsDashboardAgentPorts).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, nil)
	}()

	portFile := filepath.Join(cfg.SessionDir, dashboardAgentListenPortName+"_"+cfg.NodeID.Hex())
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(portFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent did not persist the dashboard agent listen port file")
		}
		time.Sleep(10 * time.Millisecond)
	}

	got := readPersistedPort(t, cfg.SessionDir, cfg.NodeID, dashboardAgentListenPortName)
	if got <= 0 || got == 52365 {
		t.Fatalf("listen port persisted as %d, want a dynamic port > 0 different from the taken fixed port", got)
	}
	// The rebound HTTP server actually serves on the new port.
	conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", got), time.Second)
	if dialErr != nil {
		t.Fatalf("fallback HTTP port %d not serving: %v", got, dialErr)
	}
	conn.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run with the fixed port taken must succeed via fallback: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// readPersistedPort reads a port-discovery file written by the agent. The file
// name is "{port_name}_{node_id_hex}" and the content is the decimal port,
// mirroring ray::PersistPort.
func readPersistedPort(t *testing.T, dir string, nodeID ids.NodeID, name string) int {
	t.Helper()
	path := filepath.Join(dir, name+"_"+nodeID.Hex())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	port, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatalf("parse %s content %q: %v", name, data, err)
	}
	return port
}

// TestRunPersistsDashboardAgentPorts verifies the agent publishes the port
// files raylet's WaitForDashboardAgentPorts waits on when a port is 0
// (dynamic allocation). raylet blocks for these files and CHECK-fails after
// 15s if they never appear, so failing to write them kills startup.
func TestRunPersistsDashboardAgentPorts(t *testing.T) {
	dir := t.TempDir()
	nodeID := ids.NewNodeID()
	fake := &fakeKVClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, Config{
		NodeIP:     "127.0.0.1",
		NodeID:     nodeID,
		SessionDir: dir,
		GCS:        fake,
	}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	grpcPort := readPersistedPort(t, dir, nodeID, "metrics_agent_port")
	if grpcPort <= 0 {
		t.Errorf("metrics_agent_port = %d, want a bound ephemeral port", grpcPort)
	}
	if got := readPersistedPort(t, dir, nodeID, "metrics_export_port"); got != -1 {
		t.Errorf("metrics_export_port = %d, want -1 (no export port is configured in this test)", got)
	}
	listenPort := readPersistedPort(t, dir, nodeID, "dashboard_agent_listen_port")
	if listenPort <= 0 {
		t.Errorf("dashboard_agent_listen_port = %d, want a bound ephemeral port", listenPort)
	}

	// The GCS address registration must carry the same actual bound ports, not
	// the configured 0 that dynamic allocation starts from; head-side lookups
	// through ports_by_node would otherwise resolve to port 0.
	want := fmt.Sprintf("[%q, %d, %d]", "127.0.0.1", listenPort, grpcPort)
	key := DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX + nodeID.Hex()
	if got := string(fake.kv[KVNamespaceDashboard][key]); got != want {
		t.Errorf("registered agent address = %q, want %q", got, want)
	}
}

// TestRunMinimalPublishesSentinelsAndSkipsRegistration pins the two opposite
// halves of the Python agent's minimal mode. The port files raylet waits on
// must still appear, each holding the not-in-use sentinel, because a node
// whose configured port is 0 blocks on them; the GCS agent-address keys must
// not appear at all, because no server exists behind them and a reader that
// finds the key would dial the sentinel port.
func TestRunMinimalPublishesSentinelsAndSkipsRegistration(t *testing.T) {
	dir := t.TempDir()
	nodeID := ids.NewNodeID()
	fake := &fakeKVClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, Config{
		NodeIP:     "127.0.0.1",
		NodeID:     nodeID,
		SessionDir: dir,
		GCS:        fake,
		Minimal:    true,
	}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, name := range []string{"metrics_agent_port", "metrics_export_port", "dashboard_agent_listen_port"} {
		if got := readPersistedPort(t, dir, nodeID, name); got != -1 {
			t.Errorf("%s = %d, want -1", name, got)
		}
	}

	if got := fake.kv[KVNamespaceDashboard]; len(got) != 0 {
		t.Errorf("minimal mode published agent addresses %v, want none", got)
	}
}

// TestPersistAgentPorts verifies the exact file names and decimal contents for
// the two server ports. The metrics export port is written separately, once the
// module that serves it has bound.
func TestPersistAgentPorts(t *testing.T) {
	dir := t.TempDir()
	nodeID, err := ids.NodeIDFromHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c")
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	if err := persistAgentPorts(Config{SessionDir: dir, NodeID: nodeID}, 12345, 54321); err != nil {
		t.Fatalf("persistAgentPorts: %v", err)
	}
	for _, c := range []struct {
		name string
		want string
	}{
		{"metrics_agent_port", "12345"},
		{"dashboard_agent_listen_port", "54321"},
	} {
		path := filepath.Join(dir, c.name+"_"+nodeID.Hex())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", c.name, err)
		}
		if got := string(data); got != c.want {
			t.Errorf("%s content = %q, want %q", c.name, got, c.want)
		}
	}
}

// fakeMetricsModule is a module that serves a Prometheus endpoint, so the agent
// can be shown publishing the port the module bound rather than a configured
// one.
type fakeMetricsModule struct {
	startErr error
	bound    int
}

func (f *fakeMetricsModule) Start(context.Context) error       { return f.startErr }
func (f *fakeMetricsModule) RegisterHTTP(*http.ServeMux) error { return nil }
func (f *fakeMetricsModule) RegisterGRPC(*grpc.Server) error   { return nil }
func (f *fakeMetricsModule) BoundMetricsExportPort() int       { return f.bound }

// blockingModule is a module whose Start blocks until released, so a test can
// observe what Run published before the last module finished starting.
type blockingModule struct {
	release chan struct{}
	started chan struct{}
}

func (b *blockingModule) Start(context.Context) error {
	close(b.started)
	<-b.release
	return nil
}

func (b *blockingModule) RegisterHTTP(*http.ServeMux) error { return nil }
func (b *blockingModule) RegisterGRPC(*grpc.Server) error   { return nil }

// TestRunPublishesTheExportPortBeforeTheLastModuleStarts verifies the export
// port file appears as soon as the module that serves it has started. The
// raylet waits 15s for that file before it registers the node, so tying the
// write to the slowest unrelated module would put a node's startup on whatever
// that module happens to need.
func TestRunPublishesTheExportPortBeforeTheLastModuleStarts(t *testing.T) {
	dir := t.TempDir()
	nodeID := ids.NewNodeID()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	blocking := &blockingModule{release: make(chan struct{}), started: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			NodeIP:     "127.0.0.1",
			NodeID:     nodeID,
			SessionDir: dir,
			GCS:        &fakeKVClient{},
		}, []Module{&fakeMetricsModule{bound: 4321}, blocking})
	}()

	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking module never started")
	}

	path := filepath.Join(dir, "metrics_export_port_"+nodeID.Hex())
	deadline := time.Now().Add(5 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil {
			if got := strings.TrimSpace(string(data)); got != "4321" {
				t.Fatalf("metrics_export_port = %q, want 4321", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("metrics_export_port was not published while the last module was still starting")
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(blocking.release)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the module was released")
	}
}

// TestRunPublishesTheBoundMetricsExportPort verifies Run reads the export port
// from the module that serves it. The configured port says nothing about
// whether the bind succeeded, so publishing it blindly would hand head-side
// consumers an endpoint nothing answers on.
func TestRunPublishesTheBoundMetricsExportPort(t *testing.T) {
	dir := t.TempDir()
	nodeID := ids.NewNodeID()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mod := &fakeMetricsModule{bound: 4321}
	if err := Run(ctx, Config{
		NodeIP:            "127.0.0.1",
		NodeID:            nodeID,
		SessionDir:        dir,
		GCS:               &fakeKVClient{},
		MetricsExportPort: 44217,
	}, []Module{mod}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := readPersistedPort(t, dir, nodeID, "metrics_export_port"); got != 4321 {
		t.Errorf("metrics_export_port = %d, want the module's bound port 4321", got)
	}
}

// TestPersistAgentPortsPublishesBoundExportPort verifies the export port the
// exporter bound is the one published, so head-side consumers scrape a live
// endpoint instead of the -1 sentinel, and that a disabled collection or a
// failed bind still publishes the sentinel.
func TestPersistAgentPortsPublishesBoundExportPort(t *testing.T) {
	dir := t.TempDir()
	nodeID := ids.NewNodeID()
	cfg := Config{SessionDir: dir, NodeID: nodeID}
	if err := persistMetricsExportPort(cfg, 44217); err != nil {
		t.Fatalf("persistMetricsExportPort: %v", err)
	}
	if got := readPersistedPort(t, dir, nodeID, "metrics_export_port"); got != 44217 {
		t.Errorf("metrics_export_port = %d, want 44217", got)
	}

	disabled := Config{SessionDir: dir, NodeID: nodeID, MetricsCollectionDisabled: true}
	if err := persistMetricsExportPort(disabled, 44217); err != nil {
		t.Fatalf("persistMetricsExportPort: %v", err)
	}
	if got := readPersistedPort(t, dir, nodeID, "metrics_export_port"); got != -1 {
		t.Errorf("metrics_export_port = %d, want -1 when metrics collection is disabled", got)
	}

	// A failed bind is reported as -1 by the module, and must not be turned
	// into a port the agent cannot serve.
	if err := persistMetricsExportPort(cfg, -1); err != nil {
		t.Fatalf("persistMetricsExportPort: %v", err)
	}
	if got := readPersistedPort(t, dir, nodeID, "metrics_export_port"); got != -1 {
		t.Errorf("metrics_export_port = %d, want -1 when the exporter did not bind", got)
	}
}

// TestNewGRPCServerBindsAllInterfaces verifies that on a non-localhost node the
// gRPC listener is reachable from both localhost (the raylet's
// MetricsAgentClient dials localhost) and this host's non-loopback address
// (head-side consumers dial the pod IP resolved through ports_by_node). The
// localhost node case, which binds the loopback address only, is covered by
// TestAgentGRPCBindsLoopbackForLocalhostNode.
func TestNewGRPCServerBindsAllInterfaces(t *testing.T) {
	// Any non-localhost node IP selects the all-interfaces branch. The literal
	// does not have to be reachable: the listener binds the wildcard address,
	// which the dials below exercise through localhost and this host's actual
	// non-loopback address.
	srv, err := newGRPCServer(Config{NodeIP: "192.0.2.10", GRPCPort: 0}, nil)
	if err != nil {
		t.Fatalf("newGRPCServer: %v", err)
	}
	defer srv.listener.Close()
	port := listenerPort(srv.listener)
	if port <= 0 {
		t.Fatalf("listener port = %d, want a bound ephemeral port", port)
	}
	dial := func(addr string) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Errorf("dial %s: %v", addr, err)
			return
		}
		conn.Close()
	}
	dial(fmt.Sprintf("localhost:%d", port))
	if ip := nonLoopbackIPv4(t); ip != "" {
		dial(fmt.Sprintf("%s:%d", ip, port))
	}
}

// recoveryTestModule registers a minimal gRPC service with one panicking and
// one healthy unary method, exercising the agent's panic recovery end to end.
type recoveryTestModule struct{}

func (m *recoveryTestModule) Start(context.Context) error       { return nil }
func (m *recoveryTestModule) RegisterHTTP(*http.ServeMux) error { return nil }
func (m *recoveryTestModule) RegisterGRPC(s *grpc.Server) error {
	s.RegisterService(&recoveryTestServiceDesc, &recoveryTestService{})
	return nil
}

type recoveryTestService struct{}

var recoveryTestServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.RecoveryService",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Panic",
			Handler: func(_ interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(proto.ListLogsRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				handler := func(context.Context, interface{}) (interface{}, error) {
					panic("boom")
				}
				if interceptor == nil {
					return handler(ctx, in)
				}
				info := &grpc.UnaryServerInfo{FullMethod: "/test.RecoveryService/Panic"}
				return interceptor(ctx, in, info, handler)
			},
		},
		{
			MethodName: "Healthy",
			Handler: func(_ interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(proto.ListLogsRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				handler := func(context.Context, interface{}) (interface{}, error) {
					return &proto.ListLogsReply{}, nil
				}
				if interceptor == nil {
					return handler(ctx, in)
				}
				info := &grpc.UnaryServerInfo{FullMethod: "/test.RecoveryService/Healthy"}
				return interceptor(ctx, in, info, handler)
			},
		},
	},
}

// TestUnaryRecoveryInterceptorKeepsServerAlive verifies that a panicking unary
// handler surfaces as codes.Internal to the client and does not kill the
// agent: a subsequent request is still served.
func TestUnaryRecoveryInterceptorKeepsServerAlive(t *testing.T) {
	srv, err := newGRPCServer(Config{NodeIP: "127.0.0.1", GRPCPort: 0}, []Module{&recoveryTestModule{}})
	if err != nil {
		t.Fatalf("newGRPCServer: %v", err)
	}
	go srv.serve()
	defer srv.stop()

	conn, err := grpc.DialContext(context.Background(),
		fmt.Sprintf("127.0.0.1:%d", listenerPort(srv.listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Invoke(ctx, "/test.RecoveryService/Panic", &proto.ListLogsRequest{}, &proto.ListLogsReply{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Internal {
		t.Fatalf("panic call error = %v, want codes.Internal", err)
	}
	if err := conn.Invoke(ctx, "/test.RecoveryService/Healthy", &proto.ListLogsRequest{}, &proto.ListLogsReply{}); err != nil {
		t.Fatalf("healthy call after a recovered panic = %v, want nil", err)
	}
}

// failingKVClient is a gcs.Client whose Put always fails, forcing the GCS
// registration step of Run to return an error. It embeds the gcs.Client
// interface so it satisfies the full interface while only overriding Put.
type failingKVClient struct {
	gcs.Client
}

func (f *failingKVClient) Put(context.Context, string, string, []byte, bool) (bool, error) {
	return false, errors.New("gcs put failed")
}

// failingModule is a Module whose Start always fails, forcing Run to abort
// after the servers are already bound and their ports persisted.
type failingModule struct{}

func (failingModule) Start(context.Context) error       { return errors.New("boom") }
func (failingModule) RegisterHTTP(*http.ServeMux) error { return nil }
func (failingModule) RegisterGRPC(*grpc.Server) error   { return nil }

// testConfig builds a Run config with a throwaway session directory and
// dynamically allocated ports, mirroring TestRunPersistsDashboardAgentPorts.
func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		NodeIP:     "127.0.0.1",
		NodeID:     ids.NewNodeID(),
		SessionDir: t.TempDir(),
		GCS:        &fakeKVClient{},
	}
}

// readPersistedPorts reads the port-discovery files persistAgentPorts wrote
// under SessionDir and returns the ports corresponding to a bound listener.
// The metrics export file is skipped: it is republished after the modules
// start, so on a module failure it holds the not-in-use sentinel.
func readPersistedPorts(t *testing.T, cfg Config) []int {
	t.Helper()
	return []int{
		readPersistedPort(t, cfg.SessionDir, cfg.NodeID, metricsAgentPortName),
		readPersistedPort(t, cfg.SessionDir, cfg.NodeID, dashboardAgentListenPortName),
	}
}

// TestRunReleasesServersWhenModuleStartFails verifies that a module failing to
// start releases both the HTTP and gRPC listeners instead of returning with
// them still bound.
func TestRunReleasesServersWhenModuleStartFails(t *testing.T) {
	cfg := testConfig(t)
	err := Run(context.Background(), cfg, []Module{failingModule{}})
	if err == nil || !strings.Contains(err.Error(), "start module") {
		t.Fatalf("Run err = %v, want a start module failure", err)
	}
	for _, port := range readPersistedPorts(t, cfg) {
		conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if dialErr == nil {
			conn.Close()
			t.Fatalf("port %d still accepting connections after Run failed", port)
		}
	}
}

// TestRunReleasesHTTPServerWhenGRPCServerFails verifies that failing to create
// the gRPC server releases the already-bound HTTP listener instead of leaking
// it. The gRPC failure happens before persistAgentPorts runs, so no port files
// exist; the HTTP port is pre-picked instead and probed after Run returns.
func TestRunReleasesHTTPServerWhenGRPCServerFails(t *testing.T) {
	// Occupy a gRPC port so newGRPCServer fails to bind it.
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy gRPC port: %v", err)
	}
	defer grpcLis.Close()
	grpcPort := grpcLis.Addr().(*net.TCPAddr).Port

	// Pick a currently-free port for the HTTP listener.
	httpProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe HTTP port: %v", err)
	}
	httpPort := httpProbe.Addr().(*net.TCPAddr).Port
	httpProbe.Close()

	cfg := testConfig(t)
	cfg.GRPCPort = grpcPort
	cfg.ListenPort = httpPort
	err = Run(context.Background(), cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "create gRPC server") {
		t.Fatalf("Run err = %v, want a create gRPC server failure", err)
	}
	conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", httpPort), time.Second)
	if dialErr == nil {
		conn.Close()
		t.Fatalf("HTTP port %d still accepting connections after Run failed", httpPort)
	}
}

// routeModule is a Module that registers HTTP routes supplied by a test.
type routeModule struct {
	register func(*http.ServeMux)
}

func (m *routeModule) Start(context.Context) error { return nil }

func (m *routeModule) RegisterHTTP(mux *http.ServeMux) error {
	if m.register != nil {
		m.register(mux)
	}
	return nil
}

func (m *routeModule) RegisterGRPC(*grpc.Server) error { return nil }

// healthRouteModule registers the two public agent health endpoints plus a
// private endpoint, so middleware and router semantics can be observed.
func healthRouteModule() *routeModule {
	return &routeModule{register: func(mux *http.ServeMux) {
		ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
		mux.HandleFunc("GET /api/healthz", ok)
		mux.HandleFunc("GET /api/local_raylet_healthz", ok)
		mux.HandleFunc("GET /api/jobs", ok)
	}}
}

// newTestRouter builds the aiohttp-compatible router directly, bypassing the
// auth and browser middleware so the router semantics can be observed in
// isolation.
func newTestRouter(t *testing.T, mods ...Module) *aiohttpRouter {
	t.Helper()
	mux := http.NewServeMux()
	for _, m := range mods {
		if err := m.RegisterHTTP(mux); err != nil {
			t.Fatalf("register HTTP routes: %v", err)
		}
	}
	return &aiohttpRouter{mux: mux}
}

// newAgentHandler builds the production handler chain (max-bytes, browser
// block, token auth, CORS) for the given modules.
func newAgentHandler(t *testing.T, mods ...Module) http.Handler {
	t.Helper()
	srv, err := newHTTPServer(Config{NodeIP: "127.0.0.1"}, mods)
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.shutdown(context.Background())
		_ = srv.listener.Close()
	})
	return srv.server.Handler
}

// doRequest serves one request through h and returns the recorded response.
func doRequest(t *testing.T, h http.Handler, method, target string, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestAgentHTTPRequiresTokenOutsidePublicPaths verifies the token-auth
// middleware the agent shares with the head: the health endpoints stay public,
// every other path requires a bearer token when RAY_AUTH_MODE=token, and an
// unknown path is authenticated before it is routed (auth answers 401, not the
// router's 404), mirroring the Python agent's middleware order.
func TestAgentHTTPRequiresTokenOutsidePublicPaths(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "test-token")
	h := newAgentHandler(t, healthRouteModule())

	for _, path := range []string{"/api/healthz", "/api/local_raylet_healthz"} {
		rec := doRequest(t, h, http.MethodGet, "http://agent"+path, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s without a token = %d, want 200 (public path)", path, rec.Code)
		}
	}

	cases := []struct {
		name     string
		path     string
		auth     string
		wantCode int
		wantBody string
	}{
		{"missing token", "/api/jobs", "", http.StatusUnauthorized, "Unauthorized: Missing authentication token"},
		{"invalid token", "/api/jobs", "Bearer wrong", http.StatusForbidden, "Forbidden: Invalid authentication token"},
		{"valid token", "/api/jobs", "Bearer test-token", http.StatusOK, ""},
		{"unknown path is authenticated first", "/api/unknown", "", http.StatusUnauthorized, "Unauthorized: Missing authentication token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := map[string]string{}
			if tc.auth != "" {
				header["Authorization"] = tc.auth
			}
			rec := doRequest(t, h, http.MethodGet, "http://agent"+tc.path, header)
			if rec.Code != tc.wantCode {
				t.Fatalf("GET %s = %d, want %d", tc.path, rec.Code, tc.wantCode)
			}
			if got := rec.Body.String(); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
		})
	}
}

// TestAgentHTTPPreflightMatchesAiohttpCORS verifies the router answers CORS
// preflights only for registered routes, with the aiohttp_cors headers, and
// that the production chain answers a browser-origin preflight before the
// router: with token auth disabled the browser block rejects it with 403, and
// in token mode the token middleware, which is outermost, rejects it with 401.
func TestAgentHTTPPreflightMatchesAiohttpCORS(t *testing.T) {
	mod := healthRouteModule()
	preflightHeaders := map[string]string{
		"Origin":                        "http://dashboard",
		"Access-Control-Request-Method": http.MethodGet,
	}

	t.Run("registered route", func(t *testing.T) {
		rec := doRequest(t, newTestRouter(t, mod), http.MethodOptions, "http://agent/api/jobs", preflightHeaders)
		if rec.Code != http.StatusOK {
			t.Fatalf("preflight = %d, want 200", rec.Code)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://dashboard" {
			t.Errorf("Access-Control-Allow-Origin = %q, want the request origin", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Methods"); got != http.MethodGet {
			t.Errorf("Access-Control-Allow-Methods = %q, want GET", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Errorf("Access-Control-Allow-Credentials = %q, want true", got)
		}
	})

	t.Run("unregistered route", func(t *testing.T) {
		rec := doRequest(t, newTestRouter(t, mod), http.MethodOptions, "http://agent/api/unknown", preflightHeaders)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("preflight on an unregistered route = %d, want 404", rec.Code)
		}
		if got := rec.Body.String(); got != "404: Not Found" {
			t.Errorf("body = %q, want the aiohttp 404 text", got)
		}
	})

	t.Run("browser origin is blocked by the chain with auth disabled", func(t *testing.T) {
		t.Setenv("RAY_AUTH_MODE", "")
		rec := doRequest(t, newAgentHandler(t, mod), http.MethodGet, "http://agent/api/jobs", map[string]string{
			"Origin": "http://dashboard",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("browser request = %d, want 403", rec.Code)
		}
		if got := rec.Body.String(); got != "Browser requests not allowed." {
			t.Errorf("body = %q, want the browser-block text", got)
		}
	})

	t.Run("preflight is answered by token auth first in token mode", func(t *testing.T) {
		t.Setenv("RAY_AUTH_MODE", "token")
		t.Setenv("RAY_AUTH_TOKEN", "test-token")
		rec := doRequest(t, newAgentHandler(t, mod), http.MethodOptions, "http://agent/api/jobs", preflightHeaders)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("preflight in token mode = %d, want 401", rec.Code)
		}
		if got := rec.Body.String(); got != "Unauthorized: Missing authentication token" {
			t.Errorf("body = %q, want the missing-token text", got)
		}
	})
}

// TestAgentHTTPMethodNotAllowedMatchesAiohttp verifies an unregistered method
// on a registered path produces aiohttp's 405 body and an Allow header listing
// the methods the mux actually serves (plus OPTIONS).
func TestAgentHTTPMethodNotAllowedMatchesAiohttp(t *testing.T) {
	rec := doRequest(t, newTestRouter(t, healthRouteModule()), http.MethodPut, "http://agent/api/jobs", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /api/jobs = %d, want 405", rec.Code)
	}
	if got, want := rec.Header().Get("Allow"), "GET,HEAD,OPTIONS"; got != want {
		t.Errorf("Allow = %q, want %q", got, want)
	}
	if got := rec.Body.String(); got != "405: Method Not Allowed" {
		t.Errorf("body = %q, want the aiohttp 405 text", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain; charset=utf-8", got)
	}
}

// TestAgentHTTPHealthzMethodNotAllowedMatchesAiohttp verifies the method-scoped
// health routes still surface aiohttp's 405 envelope through the router: a POST
// matches no method, so the router answers instead of routing to the handler.
func TestAgentHTTPHealthzMethodNotAllowedMatchesAiohttp(t *testing.T) {
	rec := doRequest(t, newTestRouter(t, healthRouteModule()), http.MethodPost, "http://agent/api/healthz", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/healthz = %d, want 405", rec.Code)
	}
	if got, want := rec.Header().Get("Allow"), "GET,HEAD,OPTIONS"; got != want {
		t.Errorf("Allow = %q, want %q", got, want)
	}
	if got := rec.Body.String(); got != "405: Method Not Allowed" {
		t.Errorf("body = %q, want the aiohttp 405 text", got)
	}
}

// TestAgentHTTPTrailingSlashNotRedirected verifies the router does not leak
// ServeMux's 301 redirect when a request matches a trailing-slash pattern only.
// aiohttp answers the request itself (or 404s), it never redirects.
func TestAgentHTTPTrailingSlashNotRedirected(t *testing.T) {
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mod := &routeModule{register: func(mux *http.ServeMux) { mux.HandleFunc("GET /dir/", ok) }}
	rt := newTestRouter(t, mod)

	for _, target := range []string{"http://agent/dir/", "http://agent/dir/x"} {
		if rec := doRequest(t, rt, http.MethodGet, target, nil); rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", target, rec.Code)
		}
	}

	rec := doRequest(t, rt, http.MethodGet, "http://agent/dir", nil)
	if rec.Code == http.StatusMovedPermanently {
		t.Fatalf("GET /dir = 301, want no redirect")
	}
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /dir = %d, want 404", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "" {
		t.Errorf("Location = %q, want no redirect", got)
	}
}

// TestAgentHTTPPathEscapingMatchesAiohttp verifies paths with empty segments or
// dot segments are not silently normalized the way net/http's ServeMux would
// (a 301 redirect) and instead fall through to aiohttp's 404, matching
// aiohttp's router, which matches the request path literally.
func TestAgentHTTPPathEscapingMatchesAiohttp(t *testing.T) {
	rt := newTestRouter(t, healthRouteModule())

	if rec := doRequest(t, rt, http.MethodGet, "http://agent/api/jobs", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET /api/jobs = %d, want 200", rec.Code)
	}

	for _, target := range []string{
		"http://agent//api/jobs",
		"http://agent/api/./jobs",
		"http://agent/api/../api/jobs",
	} {
		rec := doRequest(t, rt, http.MethodGet, target, nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404 (aiohttp matches the path literally)", target, rec.Code)
		}
		if got := rec.Header().Get("Location"); got != "" {
			t.Errorf("GET %s redirected to %q, want no redirect", target, got)
		}
		if got := rec.Body.String(); got != "404: Not Found" {
			t.Errorf("GET %s body = %q, want the aiohttp 404 text", target, got)
		}
	}

	// The method probe escapes the same way: a method mismatch on a path that
	// does not exist is a 404, not a 405 with an Allow header.
	if rec := doRequest(t, rt, http.MethodPut, "http://agent//api/jobs", nil); rec.Code != http.StatusNotFound {
		t.Errorf("PUT //api/jobs = %d, want 404", rec.Code)
	}
}

// TestHTTPServerLoopbackListener verifies the agent binds a second listener on
// the localhost address when the node IP is not a loopback address (the
// in-container readiness probe dials localhost), that both listeners serve the
// same handler, and that shutdown releases both. The listen port is injected as
// 0, so the loopback listener is expected to share the node-ip listener's
// actually bound port rather than the configured 0. A localhost or empty node
// IP binds only the single listener.
func TestHTTPServerLoopbackListener(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "")
	srv, err := newHTTPServer(Config{NodeIP: "127.0.0.2", ListenPort: 0}, []Module{healthRouteModule()})
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}
	// Release both sockets if the test aborts before its explicit shutdown.
	t.Cleanup(func() {
		_ = srv.shutdown(context.Background())
		_ = srv.listener.Close()
		if srv.loopbackListener != nil {
			_ = srv.loopbackListener.Close()
		}
	})
	if srv.loopbackListener == nil {
		_ = srv.listener.Close()
		t.Fatal("no loopback listener bound for a non-localhost node IP")
	}
	port := listenerPort(srv.listener)
	loopbackAddr := srv.loopbackListener.Addr().(*net.TCPAddr)
	if got, want := loopbackAddr.IP.String(), common.GetLocalhostIP(); got != want {
		t.Errorf("loopback listener bound %s, want %s", got, want)
	}
	if loopbackAddr.Port != port {
		t.Errorf("loopback listener port = %d, want the node-ip listener port %d", loopbackAddr.Port, port)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.serve() }()

	client := &http.Client{Timeout: 5 * time.Second}
	for _, addr := range []string{
		fmt.Sprintf("%s:%d", "127.0.0.2", port),
		loopbackAddr.String(),
	} {
		resp, err := client.Get("http://" + addr + "/api/healthz")
		if err != nil {
			t.Fatalf("GET %s: %v", addr, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", addr, resp.StatusCode)
		}
	}

	if err := srv.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after shutdown")
	}
	for _, addr := range []string{srv.listener.Addr().String(), loopbackAddr.String()} {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			t.Errorf("%s still accepting connections after shutdown", addr)
		}
	}

	// A loopback node IP needs no second listener: the node-ip listener already
	// answers the readiness probe.
	local, err := newHTTPServer(Config{NodeIP: "127.0.0.1", ListenPort: 0}, nil)
	if err != nil {
		t.Fatalf("newHTTPServer for a loopback node IP: %v", err)
	}
	defer local.listener.Close()
	if local.loopbackListener != nil {
		t.Errorf("loopback listener bound for the loopback node IP %s", local.listener.Addr())
	}

	// An empty node IP falls back to a wildcard bind that already covers the
	// loopback address, so no second listener is created.
	wildcard, err := newHTTPServer(Config{ListenPort: 0}, nil)
	if err != nil {
		t.Fatalf("newHTTPServer for an empty node IP: %v", err)
	}
	defer wildcard.listener.Close()
	if ip := wildcard.listener.Addr().(*net.TCPAddr).IP; !ip.IsUnspecified() {
		t.Errorf("empty node IP bound %s, want the wildcard address", ip)
	}
	if wildcard.loopbackListener != nil {
		t.Errorf("loopback listener bound for an empty node IP: %s", wildcard.loopbackListener.Addr())
	}
}

// TestRunReleasesServersWhenGCSRegistrationFails verifies that failing to
// register the agent address in GCS releases both the HTTP and gRPC listeners
// instead of returning with them still bound.
func TestRunReleasesServersWhenGCSRegistrationFails(t *testing.T) {
	cfg := testConfig(t)
	cfg.GCS = &failingKVClient{}
	err := Run(context.Background(), cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "register agent to GCS") {
		t.Fatalf("Run err = %v, want a register agent to GCS failure", err)
	}
	for _, port := range readPersistedPorts(t, cfg) {
		conn, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if dialErr == nil {
			conn.Close()
			t.Fatalf("port %d still accepting connections after Run failed", port)
		}
	}
}
