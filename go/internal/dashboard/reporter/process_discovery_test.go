package reporter

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
)

// ---------------------------------------------------------------------------
// Test doubles for the ancestor walk
// ---------------------------------------------------------------------------

// stubProcessTree pins the /proc view the ancestor walk sees: a pid to parent
// map and a pid to comm map. Missing entries report "no such process", so a
// walk that steps off the pinned tree terminates.
func stubProcessTree(t *testing.T, parents map[int]int, comms map[int]string) {
	t.Helper()

	origParent, origComm := parentPID, commName
	parentPID = func(pid int) int { return parents[pid] }
	commName = func(pid int) (string, error) {
		name, ok := comms[pid]
		if !ok {
			return "", os.ErrNotExist
		}
		return name, nil
	}
	t.Cleanup(func() { parentPID, commName = origParent, origComm })
}

// stubZombies pins the zombie check: the listed pids are zombies and every
// other pid is alive.
func stubZombies(t *testing.T, zombies ...int) {
	t.Helper()

	set := make(map[int]bool, len(zombies))
	for _, pid := range zombies {
		set[pid] = true
	}
	orig := isZombie
	isZombie = func(pid int) bool { return set[pid] }
	t.Cleanup(func() { isZombie = orig })
}

// ---------------------------------------------------------------------------
// raylet discovery: ancestor walk
// ---------------------------------------------------------------------------

// TestRayletWalksParentChainToRaylet verifies the walk stops at the first
// ancestor whose comm contains "raylet", mirroring the Python
// _get_raylet_proc loop. The dashboard agent sits under the raylet, sometimes
// through an intermediate runner, so the raylet is not necessarily the direct
// parent.
func TestRayletWalksParentChainToRaylet(t *testing.T) {
	self := os.Getpid()
	const (
		runner = 4001
		raylet = 4002
		init   = 1
	)
	stubProcessTree(t, map[int]int{
		self:   runner,
		runner: raylet,
		raylet: init,
	}, map[int]string{
		self:   "dashboard_agent",
		runner: "python3",
		raylet: "raylet",
		init:   "systemd",
	})
	stubZombies(t)

	if got := findRayletByParentChain(); got != raylet {
		t.Errorf("findRayletByParentChain() = %d, want %d (the raylet ancestor)", got, raylet)
	}
}

// TestRayletDoesNotMatchItself pins the walk's starting point. The Python
// implementation starts from curr_proc.parent(), so a process that happens to be
// named "raylet" must never report itself. Starting the walk at the current pid
// would make every process its own raylet and is the mutation this test guards.
func TestRayletDoesNotMatchItself(t *testing.T) {
	self := os.Getpid()
	const parent = 4101
	stubProcessTree(t, map[int]int{
		self:   parent,
		parent: 1,
	}, map[int]string{
		self:   "raylet", // named like a raylet but is the agent itself
		parent: "systemd",
	})
	stubZombies(t)

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0; the walk must start at the parent, never match the agent itself", got)
	}
}

// TestRayletNilWhenNoRayletAncestor verifies a chain without a raylet yields no
// pid, mirroring the Python _get_raylet returning None when the walk runs out.
func TestRayletNilWhenNoRayletAncestor(t *testing.T) {
	self := os.Getpid()
	stubProcessTree(t, map[int]int{
		self: 4201,
		4201: 1,
	}, map[int]string{
		self: "dashboard_agent",
		4201: "systemd",
	})
	stubZombies(t)

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0 when no ancestor is a raylet", got)
	}
}

// TestRayletRejectsPidOne verifies an init process named raylet is not reported
// as the node's raylet, mirroring the Python `if self._raylet_proc.pid == 1:
// return None` guard.
func TestRayletRejectsPidOne(t *testing.T) {
	stubProcessTree(t, map[int]int{
		os.Getpid(): 1,
	}, map[int]string{
		os.Getpid(): "dashboard_agent",
		1:           "raylet", // container init happens to be named raylet
	})
	stubZombies(t)

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0; pid 1 must never be reported as the raylet", got)
	}
}

// TestRayletRejectsZombie verifies a zombie ancestor named raylet is rejected,
// mirroring the Python STATUS_ZOMBIE check. A zombie's /proc entry is on its way
// out, so reporting it would publish process attributes that are already gone.
func TestRayletRejectsZombie(t *testing.T) {
	self := os.Getpid()
	const zombieRaylet = 4301
	stubProcessTree(t, map[int]int{
		self:         zombieRaylet,
		zombieRaylet: 1,
	}, map[int]string{
		self:         "dashboard_agent",
		zombieRaylet: "raylet",
	})
	stubZombies(t, zombieRaylet)

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0; a zombie raylet must be rejected", got)
	}
}

// TestRayletNilWhenParentUnreadable verifies a broken chain terminates the walk
// instead of reporting a bogus pid.
func TestRayletNilWhenParentUnreadable(t *testing.T) {
	orig := parentPID
	parentPID = func(int) int { return 0 }
	t.Cleanup(func() { parentPID = orig })

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0 when the parent chain is unreadable", got)
	}
}

// TestRayletNilWhenCommUnreadable verifies an ancestor that exits mid-walk ends
// the search rather than producing a partial result.
func TestRayletNilWhenCommUnreadable(t *testing.T) {
	self := os.Getpid()
	const gone = 4401
	stubProcessTree(t, map[int]int{
		self: gone,
		gone: 1,
	}, map[int]string{
		self: "dashboard_agent",
		// gone has no comm entry: it exited between the ppid and comm reads.
	})
	stubZombies(t)

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0 when the ancestor's comm is unreadable", got)
	}
}

// TestRayletAccessorReturnsNilWithoutRaylet verifies the accessor keeps the
// Python contract of reporting nil (omitted) rather than a fabricated entry.
func TestRayletAccessorReturnsNilWithoutRaylet(t *testing.T) {
	stubProcessTree(t, map[int]int{os.Getpid(): 1}, map[int]string{
		os.Getpid(): "dashboard_agent",
		1:           "systemd",
	})

	stats, err := (&systemSource{}).raylet()
	if err != nil {
		t.Fatalf("raylet: %v", err)
	}
	if stats != nil {
		t.Errorf("raylet = %v, want nil when the walk finds no raylet", stats)
	}
}

// TestRayletIgnoresUnrelatedProcessWithRayletInArgv is the direct regression for
// the wrong-process selection. Every worker and the dashboard agent itself
// carry --raylet-name=<socket> in their argv (services.py), so the previous
// cmdline-substring scan could report any of them as the raylet. The ancestor
// walk must ignore such a process because its comm is unrelated.
func TestRayletIgnoresUnrelatedProcessWithRayletInArgv(t *testing.T) {
	// A real process whose argv mentions raylet, chosen because the old
	// cmdline-substring scan would select it and the ancestor walk must not. A
	// shell loop is used rather than a script so the shell does not exec the
	// last command in its place, which would drop the argument from the argv
	// before the test reads it.
	cmd := exec.Command("/bin/sh", "-c", "while :; do sleep 0.2; done",
		"--raylet-name=/tmp/ray/session/sockets/raylet")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	child := cmd.Process.Pid
	// /proc/<pid>/cmdline is empty until the child's exec completes, which races
	// with the parent returning from Start, so the argv is polled rather than
	// read once.
	argvPath := filepath.Join("/proc", strconv.Itoa(child), "cmdline")
	var argv string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		raw, err := readProcFile(argvPath)
		if err != nil {
			t.Fatalf("read %s: %v", argvPath, err)
		}
		argv = strings.ReplaceAll(raw, "\x00", " ")
		if strings.Contains(argv, "raylet") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(argv, "raylet") {
		t.Fatalf("test setup invalid: the child's argv %q does not mention raylet", argv)
	}

	comm, err := readCommName(child)
	if err != nil {
		t.Fatalf("readCommName(%d): %v", child, err)
	}
	if strings.Contains(comm, "raylet") {
		t.Fatalf("test setup invalid: the child's comm %q already contains raylet", comm)
	}

	// Pin the walk so the child is the first ancestor the search visits.
	stubProcessTree(t, map[int]int{
		os.Getpid(): child,
		child:       1,
	}, map[int]string{
		os.Getpid(): "dashboard_agent",
		child:       comm,
		1:           "systemd",
	})
	stubZombies(t)

	if got := findRayletByParentChain(); got != 0 {
		t.Errorf("findRayletByParentChain() = %d, want 0; a process merely mentioning raylet in its argv must not be selected", got)
	}
}

// TestRayletRediscoveredEachCycle verifies the raylet is resolved afresh on every
// call, so a raylet restart is observed without any cache invalidation. This
// replaces the coverage the removed process cache used to need.
func TestRayletRediscoveredEachCycle(t *testing.T) {
	self := os.Getpid()
	const (
		first  = 4501
		second = 4502
	)
	parents := map[int]int{self: first, first: 1, second: 1}
	comms := map[int]string{self: "dashboard_agent", first: "raylet", second: "raylet", 1: "systemd"}
	stubProcessTree(t, parents, comms)
	stubZombies(t)

	s := &systemSource{}

	got, err := s.raylet()
	if err != nil {
		t.Fatalf("raylet: %v", err)
	}
	if pid := asInt(got["pid"]); pid != first {
		t.Fatalf("first raylet() pid = %d, want %d", pid, first)
	}

	// The raylet restarts under a new pid; the next lookup must observe it.
	parents[self] = second
	stubProcessTree(t, parents, comms)

	got, err = s.raylet()
	if err != nil {
		t.Fatalf("raylet after restart: %v", err)
	}
	if pid := asInt(got["pid"]); pid != second {
		t.Errorf("raylet() pid after restart = %d, want %d; the pid must be rediscovered, not cached", pid, second)
	}
}

// ---------------------------------------------------------------------------
// gcs discovery: GCS internal KV
// ---------------------------------------------------------------------------

// stubGCSFetcher installs a gcsPIDFetcher returning the given pid and ok flag.
func stubGCSFetcher(s *systemSource, pid int, ok bool) {
	s.gcsPIDFetcher = func() (int, bool) { return pid, ok }
}

// TestGCSReadsPidFromInternalKV verifies the gcs pid comes from the GCS internal
// KV store, mirroring the Python _get_gcs, and that the pid is turned into a
// process-info entry.
func TestGCSReadsPidFromInternalKV(t *testing.T) {
	s := &systemSource{}
	stubGCSFetcher(s, os.Getpid(), true)

	stats, err := s.gcs()
	if err != nil {
		t.Fatalf("gcs: %v", err)
	}
	if stats == nil {
		t.Fatal("gcs = nil, want a process-info map")
	}
	if got := asInt(stats["pid"]); got != os.Getpid() {
		t.Errorf("gcs pid = %d, want the pid published in the KV store %d", got, os.Getpid())
	}
}

// TestGCSEmptyWhenKVHasNoPid verifies a missing KV value yields an empty dict,
// not nil, matching the Python _get_gcs `return {}`.
func TestGCSEmptyWhenKVHasNoPid(t *testing.T) {
	s := &systemSource{}
	stubGCSFetcher(s, 0, false)

	stats, err := s.gcs()
	if err != nil {
		t.Fatalf("gcs: %v", err)
	}
	if stats == nil {
		t.Fatal("gcs = nil, want an empty map")
	}
	if len(stats) != 0 {
		t.Errorf("gcs = %v, want an empty map when no pid is published", stats)
	}
}

// TestGCSEmptyWhenFetcherUnconfigured verifies a non-head node (no GCS client)
// reports an empty dict rather than dialing anything.
func TestGCSEmptyWhenFetcherUnconfigured(t *testing.T) {
	stats, err := (&systemSource{}).gcs()
	if err != nil {
		t.Fatalf("gcs: %v", err)
	}
	if stats == nil {
		t.Fatal("gcs = nil, want an empty map")
	}
	if len(stats) != 0 {
		t.Errorf("gcs = %v, want an empty map when no GCS client is configured", stats)
	}
}

// TestGCSDegradesWhenPidGone documents a deliberate divergence from Python: when
// the published pid no longer exists, Python's psutil.Process raises
// NoSuchProcess and _run_loop skips the entire publish for that cycle, whereas
// procAttrs degrades to a pid-only shell so the rest of the cycle still
// publishes.
func TestGCSDegradesWhenPidGone(t *testing.T) {
	gone := unusedPID(t)
	s := &systemSource{}
	stubGCSFetcher(s, gone, true)

	stats, err := s.gcs()
	if err != nil {
		t.Fatalf("gcs: %v", err)
	}
	if got := asInt(stats["pid"]); got != gone {
		t.Errorf("gcs pid = %d, want %d even though the process is gone", got, gone)
	}
}

// TestGCSRereadEachCycle verifies the pid is read from the KV store on every
// call, so a restarted gcs_server is observed without cache invalidation.
func TestGCSRereadEachCycle(t *testing.T) {
	const (
		first  = 4601
		second = 4602
	)
	pid := first
	s := &systemSource{gcsPIDFetcher: func() (int, bool) { return pid, true }}

	got, err := s.gcs()
	if err != nil {
		t.Fatalf("gcs: %v", err)
	}
	if asInt(got["pid"]) != first {
		t.Fatalf("first gcs() pid = %v, want %d", got["pid"], first)
	}

	pid = second
	got, err = s.gcs()
	if err != nil {
		t.Fatalf("gcs after restart: %v", err)
	}
	if asInt(got["pid"]) != second {
		t.Errorf("gcs() pid after restart = %v, want %d; the pid must be re-read, not cached", got["pid"], second)
	}
}

// TestGCSPIDFetcherReadsGcsPIDKey verifies the production fetcher reads the
// gcs_pid key and parses the value, mirroring the Python read of GCS_PID_KEY.
func TestGCSPIDFetcherReadsGcsPIDKey(t *testing.T) {
	client := &fakeGCSClient{value: []byte("4321")}

	pid, ok := newGCSPIDFetcher(client)()
	if !ok {
		t.Fatal("gcsPIDFetcher reported ok=false, want true for a published gcs_pid")
	}
	if pid != 4321 {
		t.Errorf("gcsPIDFetcher pid = %d, want 4321", pid)
	}
	if len(client.keys) != 1 || client.keys[0] != gcsPIDKey {
		t.Errorf("gcsPIDFetcher read keys %v, want exactly [%q]", client.keys, gcsPIDKey)
	}
}

// TestGCSPIDFetcherMissingKey verifies an absent gcs_pid yields ok=false.
func TestGCSPIDFetcherMissingKey(t *testing.T) {
	pid, ok := newGCSPIDFetcher(&fakeGCSClient{})()
	if ok {
		t.Errorf("gcsPIDFetcher ok = true (pid %d), want false when gcs_pid is absent", pid)
	}
}

// TestGCSPIDFetcherUnparsableKey verifies a non-numeric value degrades to
// ok=false instead of reporting pid 0 or a garbage pid.
func TestGCSPIDFetcherUnparsableKey(t *testing.T) {
	client := &fakeGCSClient{value: []byte("not-a-pid")}

	if pid, ok := newGCSPIDFetcher(client)(); ok {
		t.Errorf("gcsPIDFetcher ok = true (pid %d), want false for an unparsable gcs_pid", pid)
	}
}

// TestGCSPIDFetcherReadError verifies a failed KV read degrades to ok=false so
// the collection cycle still publishes without the gcs entry.
func TestGCSPIDFetcherReadError(t *testing.T) {
	client := &fakeGCSClient{err: errors.New("kv unavailable")}

	if pid, ok := newGCSPIDFetcher(client)(); ok {
		t.Errorf("gcsPIDFetcher ok = true (pid %d), want false when the KV read fails", pid)
	}
}

// TestNewSystemSourceWiresGCSFetcherOnHeadOnly verifies the fetcher is installed
// exactly on the head node with a GCS client, mirroring the Python
// `if self._is_head_node` guard around the GCS_PID_KEY read.
func TestNewSystemSourceWiresGCSFetcherOnHeadOnly(t *testing.T) {
	client := &fakeGCSClient{value: []byte("1")}

	for _, tc := range []struct {
		name  string
		cfg   agent.Config
		wired bool
	}{
		{"head with client", agent.Config{IsHead: true, GCS: client}, true},
		{"head without client", agent.Config{IsHead: true}, false},
		{"worker with client", agent.Config{IsHead: false, GCS: client}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newSystemSource(tc.cfg)
			if got := src.gcsPIDFetcher != nil; got != tc.wired {
				t.Errorf("gcsPIDFetcher wired = %v, want %v", got, tc.wired)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Seam correctness against the live /proc
// ---------------------------------------------------------------------------

// TestReadParentPIDMatchesProcStat verifies the ppid field index used by the
// production reader against the kernel's own value. The stat line's comm field
// may contain spaces and parentheses, which is why the fields are taken after
// the last ')'.
func TestReadParentPIDMatchesProcStat(t *testing.T) {
	self := os.Getpid()
	want := os.Getppid()

	if got := readParentPID(self); got != want {
		t.Errorf("readParentPID(%d) = %d, want the kernel-reported ppid %d", self, got, want)
	}
}

// TestReadCommNameMatchesProcComm verifies the production comm reader returns the
// process name the kernel exposes.
func TestReadCommNameMatchesProcComm(t *testing.T) {
	self := os.Getpid()

	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(self), "comm"))
	if err != nil {
		t.Fatalf("read /proc/%d/comm: %v", self, err)
	}
	want := strings.TrimSpace(string(raw))

	got, err := readCommName(self)
	if err != nil {
		t.Fatalf("readCommName(%d): %v", self, err)
	}
	if got != want {
		t.Errorf("readCommName(%d) = %q, want %q", self, got, want)
	}
}

// TestReadParentPIDUnparsablePid verifies a pid with no /proc entry reports 0 so
// the ancestor walk terminates instead of looping on a bogus parent.
func TestReadParentPIDUnparsablePid(t *testing.T) {
	if got := readParentPID(unusedPID(t)); got != 0 {
		t.Errorf("readParentPID(gone) = %d, want 0 for a pid with no /proc entry", got)
	}
}
