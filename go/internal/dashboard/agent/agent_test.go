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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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

// TestNewHTTPServerBindsBeforeServe verifies the listen socket is bound by the
// constructor (so it accepts connections before the agent publishes its address
// in GCS) and that it is reachable both from localhost (the in-container
// readiness probe) and from this host's non-loopback address (head-side
// consumers dialing the pod IP resolved through ports_by_node).
func TestNewHTTPServerBindsBeforeServe(t *testing.T) {
	httpSrv, err := newHTTPServer(Config{NodeIP: "127.0.0.1", ListenPort: 0}, []Module{&stubModule{}})
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}
	port := listenerPort(httpSrv.listener)
	if port <= 0 {
		t.Fatalf("listener port = %d, want a bound ephemeral port", port)
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
	dialBeforeServe(fmt.Sprintf("localhost:%d", port))
	if ip := nonLoopbackIPv4(t); ip != "" {
		dialBeforeServe(fmt.Sprintf("%s:%d", ip, port))
	}
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

// TestRunDegradesWhenHTTPPortTaken verifies the Python parity contract for an
// HTTP bind failure: the agent stays alive (Run returns nil on cancellation),
// the address is not registered in GCS, and module startup still proceeds.
func TestRunDegradesWhenHTTPPortTaken(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer lis.Close()
	port := lis.Addr().(*net.TCPAddr).Port

	fake := &fakeKVClient{}
	mod := &stubModule{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			NodeIP:     "127.0.0.1",
			ListenPort: port,
			GCS:        fake,
		}, []Module{mod})
	}()

	// Give Run time to fail the HTTP bind and reach the serving state.
	time.Sleep(200 * time.Millisecond)
	if keys := fake.kv[KVNamespaceDashboard]; len(keys) != 0 {
		t.Errorf("agent address registered despite HTTP bind failure: %v", keys)
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
		t.Errorf("metrics_export_port = %d, want -1 (the Go agent does not export metrics)", got)
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

// TestPersistAgentPorts verifies the exact file names and decimal contents for
// injected bound ports. The Go agent does not export metrics, so
// metrics_export_port must be -1.
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
		{"metrics_export_port", "-1"},
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

// TestNewGRPCServerBindsAllInterfaces verifies the gRPC listener is reachable
// from both localhost (the raylet's MetricsAgentClient dials localhost) and
// this host's non-loopback address (head-side consumers dial the pod IP
// resolved through ports_by_node).
func TestNewGRPCServerBindsAllInterfaces(t *testing.T) {
	srv, err := newGRPCServer(Config{NodeIP: "127.0.0.1", GRPCPort: 0}, nil)
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
// The metrics export file is written as -1 (the Go agent does not export
// metrics), so it is skipped.
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
