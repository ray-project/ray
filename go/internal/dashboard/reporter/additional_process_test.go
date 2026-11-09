package reporter

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
)

// TestArgvIsScriptWithFlag verifies the argv matcher mirrors the Python
// _argv_is_script_with_flag contract, including the ordering rule that the flag
// must not precede the script token.
func TestArgvIsScriptWithFlag(t *testing.T) {
	cases := []struct {
		name   string
		argv   []string
		script string
		flag   string
		want   bool
	}{
		{
			name:   "script followed by flag",
			argv:   []string{"python", "-u", "/opt/ray/log_monitor.py", "--session-dir=/tmp/x"},
			script: "log_monitor.py",
			flag:   "--session-dir=",
			want:   true,
		},
		{
			name:   "flag follows the script in the same argv",
			argv:   []string{"python", "/p/runtime_env/agent/main.py", "--runtime-env-agent-port=1"},
			script: "runtime_env/agent/main.py",
			flag:   "--runtime-env-agent-port=",
			want:   true,
		},
		{
			name:   "flag precedes the script token",
			argv:   []string{"python", "--session-dir=/tmp/x", "/opt/ray/log_monitor.py"},
			script: "log_monitor.py",
			flag:   "--session-dir=",
			want:   false,
		},
		{
			name:   "script without the flag",
			argv:   []string{"python", "/opt/ray/log_monitor.py"},
			script: "log_monitor.py",
			flag:   "--session-dir=",
			want:   false,
		},
		{
			name:   "empty argv",
			argv:   []string{},
			script: "log_monitor.py",
			flag:   "--session-dir=",
			want:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := argvIsScriptWithFlag(tc.argv, tc.script, tc.flag); got != tc.want {
				t.Errorf("argvIsScriptWithFlag(%q, %q, %q) = %v, want %v",
					tc.argv, tc.script, tc.flag, got, tc.want)
			}
		})
	}
}

// TestArgvIsScriptWithFlagIgnoresEmbeddedSubCommand is the core regression for
// The raylet embeds its children's command lines as quoted values inside
// single argv elements, so a joined-cmdline substring match would classify the
// raylet itself as log_monitor / runtime_env_agent and fold the raylet's pid and
// memory into those components. Whole-token matching must reject such an argv.
func TestArgvIsScriptWithFlagIgnoresEmbeddedSubCommand(t *testing.T) {
	rayletArgv := []string{
		"raylet",
		"--node_ip_address=127.0.0.1",
		"--python_worker_command=python -u /opt/ray/log_monitor.py --session-dir=/tmp/ray/session",
		"--runtime_env_agent_command=python /opt/ray/runtime_env/agent/main.py --runtime-env-agent-port=1234",
	}

	for _, component := range additionalProcessComponents {
		if matchesAnyArgv(additionalProcessMatchers[component], rayletArgv) {
			t.Errorf("raylet argv matched component %q; an embedded sub-command must not classify the raylet",
				component)
		}
	}

	// Sanity check that the same content does match as a real standalone command
	// line, so the assertions above are not vacuous.
	standalone := []string{"python", "-u", "/opt/ray/log_monitor.py", "--session-dir=/tmp/ray/session"}
	if !matchesAnyArgv(additionalProcessMatchers["log_monitor"], standalone) {
		t.Fatal("standalone log_monitor argv did not match; the matcher table is broken")
	}

	// Negative control: the joined-cmdline form really would have matched, which
	// is exactly the bug this token-based matcher avoids. This keeps the
	// assertions above honest.
	joined := strings.Join(rayletArgv, " ")
	for _, script := range []string{"log_monitor.py", "runtime_env/agent/main.py"} {
		if !strings.Contains(joined, script) {
			t.Fatalf("joined raylet cmdline %q does not contain %q; the negative control is malformed",
				joined, script)
		}
	}
}

// TestAdditionalProcessMatcherTable verifies each component's matchers accept
// the real command lines of both log_monitor flavours and of the runtime env
// agent, while an unrelated process matches nothing.
func TestAdditionalProcessMatcherTable(t *testing.T) {
	cases := []struct {
		name      string
		argv      []string
		component string
		want      bool
	}{
		{
			name:      "python log_monitor",
			argv:      []string{"python", "-u", "/tmp/ray/log_monitor.py", "--session-dir=/tmp/ray/session"},
			component: "log_monitor",
			want:      true,
		},
		{
			name:      "cpp log_monitor",
			argv:      []string{"/opt/ray/lib/ray/log_monitor/log_monitor", "--session-dir=/tmp/ray/session"},
			component: "log_monitor",
			want:      true,
		},
		{
			name:      "runtime_env_agent",
			argv:      []string{"python", "/opt/ray/runtime_env/agent/main.py", "--runtime-env-agent-port=5555"},
			component: "runtime_env_agent",
			want:      true,
		},
		{
			name:      "log_monitor argv does not match the agent",
			argv:      []string{"python", "-u", "/tmp/ray/log_monitor.py", "--session-dir=/tmp/ray/session"},
			component: "runtime_env_agent",
			want:      false,
		},
		{
			name:      "unrelated process does not match log_monitor",
			argv:      []string{"nginx", "-g", "daemon off;"},
			component: "log_monitor",
			want:      false,
		},
		{
			name:      "unrelated process does not match the agent",
			argv:      []string{"nginx", "-g", "daemon off;"},
			component: "runtime_env_agent",
			want:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchesAnyArgv(additionalProcessMatchers[tc.component], tc.argv)
			if got != tc.want {
				t.Errorf("component %q match(%q) = %v, want %v", tc.component, tc.argv, got, tc.want)
			}
		})
	}
}

// TestProcPIDsAreAscendingAndContainSelf verifies the /proc enumeration returns
// numeric pids in ascending numeric order, which keeps the per-component process
// list stable across cycles instead of following the lexical /proc order.
func TestProcPIDsAreAscendingAndContainSelf(t *testing.T) {
	pids := procPIDs()
	if len(pids) == 0 {
		t.Fatal("procPIDs returned no pids; the test cannot validate ordering")
	}
	for i := 1; i < len(pids); i++ {
		if pids[i-1] >= pids[i] {
			t.Fatalf("procPIDs not strictly ascending at %d: %d >= %d", i, pids[i-1], pids[i])
		}
	}
	self := os.Getpid()
	if !containsPID(pids, self) {
		t.Errorf("procPIDs does not include the current pid %d", self)
	}
}

// startScriptChild launches `/bin/sh <tempdir>/<script> <args...>` so the child's
// /proc cmdline carries the script path and the component flag as separate argv
// tokens. It waits until the child's argv is observable and returns a cleanup
// function that reaps it.
func startScriptChild(t *testing.T, script string, args ...string) (int, func()) {
	t.Helper()

	path := filepath.Join(t.TempDir(), script)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create script dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("sleep 30\n"), 0o644); err != nil {
		t.Fatalf("create script: %v", err)
	}

	cmd := exec.Command("/bin/sh", append([]string{path}, args...)...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if argv, err := readProcArgv(cmd.Process.Pid); err == nil && containsToken(argv, path) {
			return cmd.Process.Pid, cleanup
		}
		time.Sleep(5 * time.Millisecond)
	}
	cleanup()
	t.Fatalf("child %d never exposed its argv", cmd.Process.Pid)
	return 0, nil
}

// readProcArgv returns the tokenized cmdline of a pid.
func readProcArgv(pid int) ([]string, error) {
	cmd, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return nil, err
	}
	return splitCmdline(cmd), nil
}

// containsToken reports whether the argv contains the exact token.
func containsToken(argv []string, token string) bool {
	for _, a := range argv {
		if a == token {
			return true
		}
	}
	return false
}

// containsPID reports whether the pid list contains the pid.
func containsPID(pids []int, pid int) bool {
	for _, p := range pids {
		if p == pid {
			return true
		}
	}
	return false
}

// additionalStatForPID returns the entry of a component's stats list whose pid
// matches, failing when the process was not discovered.
func additionalStatForPID(t *testing.T, stats []map[string]interface{}, pid int) map[string]interface{} {
	t.Helper()
	for _, entry := range stats {
		if asInt(entry["pid"]) == pid {
			return entry
		}
	}
	t.Fatalf("pid %d not found among %d discovered entries: %v", pid, len(stats), stats)
	return nil
}

// agentConfigForAdditional builds the production collector config for the
// auxiliary-process tests. No raylet is configured, so the worker path stays
// empty and the assertions depend only on the /proc scan.
func agentConfigForAdditional(t *testing.T) agent.Config {
	t.Helper()
	return agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1"}
}

// componentPIDs renders the (Component, pid) tag pairs of the component CPU
// records for failure messages.
func componentPIDs(records []gaugeRecord) []string {
	var out []string
	for _, r := range records {
		if r.Tags["Component"] != "" {
			out = append(out, fmt.Sprintf("%s/pid=%s", r.Tags["Component"], r.Tags["pid"]))
		}
	}
	return out
}

// componentNumFDs returns the num_fds value of a component, or -1 when absent.
func componentNumFDs(records []gaugeRecord, component, pid string) float64 {
	for _, r := range records {
		if r.Gauge.Name == ComponentGaugeNumFDs && r.Tags["Component"] == component && r.Tags["pid"] == pid {
			return r.Value
		}
	}
	return -1
}

// TestAdditionalDiscoversRuntimeEnvAgent verifies the production /proc scan
// discovers a live runtime_env_agent process and reports the process stats the
// payload and the gauges need. Before the fix this map was always empty.
func TestAdditionalDiscoversRuntimeEnvAgent(t *testing.T) {
	pid, cleanup := startScriptChild(t, "runtime_env/agent/main.py", "--runtime-env-agent-port=1234")
	defer cleanup()

	s := &systemSource{}
	got, err := s.additional()
	if err != nil {
		t.Fatalf("additional: %v", err)
	}

	raw, ok := got["runtime_env_agent"]
	if !ok {
		t.Fatalf("no runtime_env_agent entry in %v", got)
	}
	stats, ok := raw.([]map[string]interface{})
	if !ok {
		t.Fatalf("runtime_env_agent stats type = %T, want []map[string]interface{}", raw)
	}

	entry := additionalStatForPID(t, stats, pid)
	for _, key := range []string{"pid", "create_time", "cpu_percent", "memory_info", "num_fds", "cmdline"} {
		if _, ok := entry[key]; !ok {
			t.Errorf("additional entry missing %q (entry: %v)", key, entry)
		}
	}
	argv, ok := entry["cmdline"].([]string)
	if !ok {
		t.Fatalf("cmdline type = %T, want []string", entry["cmdline"])
	}
	if !containsToken(argv, "--runtime-env-agent-port=1234") {
		t.Errorf("cmdline = %q, want the agent port flag token", argv)
	}
	if mem, ok := entry["memory_info"].(map[string]interface{}); !ok || asFloat(mem["rss"]) <= 0 {
		t.Errorf("memory_info = %v, want an rss > 0", entry["memory_info"])
	}
	if _, present := got["log_monitor"]; present {
		t.Errorf("log_monitor unexpectedly present: %v", got["log_monitor"])
	}
}

// TestAdditionalDiscoversLogMonitor verifies the Python log_monitor command line
// is discovered and that every reported entry really matches a matcher, so an
// unrelated process can never be folded into the component.
func TestAdditionalDiscoversLogMonitor(t *testing.T) {
	pid, cleanup := startScriptChild(t, "log_monitor.py", "--session-dir=/tmp/ray/session")
	defer cleanup()

	s := &systemSource{}
	got, err := s.additional()
	if err != nil {
		t.Fatalf("additional: %v", err)
	}

	raw, ok := got["log_monitor"]
	if !ok {
		t.Fatalf("no log_monitor entry in %v", got)
	}
	stats, ok := raw.([]map[string]interface{})
	if !ok {
		t.Fatalf("log_monitor stats type = %T, want []map[string]interface{}", raw)
	}
	additionalStatForPID(t, stats, pid)

	for _, entry := range stats {
		argv, _ := entry["cmdline"].([]string)
		if !matchesAnyArgv(additionalProcessMatchers["log_monitor"], argv) {
			t.Errorf("entry pid=%v classified as log_monitor without a matching argv: %q", entry["pid"], argv)
		}
	}
}

// TestAdditionalOmitsComponentsWithoutProcesses verifies a component with no
// live process is absent from the result rather than present with an empty list,
// mirroring the Python defaultdict -> dict conversion.
func TestAdditionalOmitsComponentsWithoutProcesses(t *testing.T) {
	s := &systemSource{}
	got, err := s.additional()
	if err != nil {
		t.Fatalf("additional: %v", err)
	}
	if got == nil {
		t.Fatal("additional returned nil, want a non-nil map")
	}
	for component, v := range got {
		stats, ok := v.([]map[string]interface{})
		if !ok {
			t.Errorf("component %q has type %T, want []map[string]interface{}", component, v)
			continue
		}
		if len(stats) == 0 {
			t.Errorf("component %q present with an empty list; absent components must be omitted", component)
		}
	}
}

// TestCollectFeedsAdditionalToRecords is the end-to-end proof that the
// additional process metrics reach the record layer: the
// collected auxiliary processes reach the record generator, which emits the
// component gauges tagged with the component name and its pid. Before the fix no
// such series existed for log_monitor / runtime_env_agent.
func TestCollectFeedsAdditionalToRecords(t *testing.T) {
	pid, cleanup := startScriptChild(t, "runtime_env/agent/main.py", "--runtime-env-agent-port=4321")
	defer cleanup()

	stats := NewStatsCollector(agentConfigForAdditional(t)).Collect()

	additional, ok := stats["additional"].(map[string]interface{})
	if !ok {
		t.Fatalf("stats[additional] type = %T, want map[string]interface{}", stats["additional"])
	}
	if _, ok := additional["runtime_env_agent"]; !ok {
		t.Fatalf("collected additional has no runtime_env_agent entry: %v", additional)
	}

	records := recordsFor(t, stats, nil, false)
	cpuRecords := findRecords(records, ComponentGaugeCPUPercentage)

	found := false
	for _, r := range cpuRecords {
		if r.Tags["Component"] == "runtime_env_agent" && r.Tags["pid"] == strconv.Itoa(pid) {
			found = true
		}
	}
	if !found {
		t.Errorf("no %s record for Component=runtime_env_agent pid=%d (component records: %v)",
			ComponentGaugeCPUPercentage, pid, componentPIDs(cpuRecords))
	}

	for _, gauge := range []string{ComponentGaugeRSSMB, ComponentGaugeNumFDs, ComponentGaugeSharedBytes} {
		if len(findRecords(records, gauge)) == 0 {
			t.Errorf("gauge %s missing for the discovered runtime_env_agent", gauge)
		}
	}

	if got := componentNumFDs(records, "runtime_env_agent", strconv.Itoa(pid)); got <= 0 {
		t.Errorf("runtime_env_agent num_fds = %v, want > 0", got)
	}
}

// TestAdditionalComponentRecordsResetAfterProcessExit verifies the data source
// and the record generator agree on staleness: once the auxiliary process is
// gone, the following cycle zeroes its component gauges instead of reporting the
// previous values forever.
func TestAdditionalComponentRecordsResetAfterProcessExit(t *testing.T) {
	pid, cleanup := startScriptChild(t, "runtime_env/agent/main.py", "--runtime-env-agent-port=9999")

	cfg := agentConfigForAdditional(t)
	first := NewStatsCollector(cfg).Collect()

	state := &recordsState{}
	live := state.toRecords(first, nil, false, globalTestTags())
	tag := strconv.Itoa(pid)
	if got := componentNumFDs(live, "runtime_env_agent", tag); got <= 0 {
		t.Fatalf("first cycle runtime_env_agent num_fds = %v, want > 0", got)
	}

	cleanup()

	second := NewStatsCollector(cfg).Collect()
	reset := state.toRecords(second, nil, false, globalTestTags())

	if got := componentNumFDs(reset, "runtime_env_agent", ""); got != 0 {
		t.Errorf("stale runtime_env_agent num_fds = %v, want a zeroed reset record", got)
	}
}
