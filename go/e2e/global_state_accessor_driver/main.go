// Copyright 2026 The Ray Authors.
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

// global_state_accessor_driver is a Ray Go driver application that verifies the
// GlobalStateAccessor production implementation and the actor-list state
// filter end to end on a real cluster. It exercises the surfaces where the Go
// API previously trailed Java and C++:
//
//	GlobalStateAccessor implementation: the interface previously had no
//	    production implementation: gcs.GetGlobalStateAccessor() returned
//	    ErrNotImplemented. This driver asserts the singleton resolves to a
//	    working implementation and exercises representative queries (jobs,
//	    nodes, actors, workers, system config, next job id) against the live
//	    GCS.
//	Actor-list state filter: actor list did not support filtering by state:
//	    ListActors only filtered by job id. This driver asserts
//	    GetAllActorInfo(jobID, state) pushes the state filter down to the GCS:
//	    only actors in the requested state are returned (ALIVE vs DEAD),
//	    matching Java GcsClient.getAllActorInfo(JobId, ActorState).
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//	bazel build //go/e2e/global_state_accessor_driver
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/e2e/global_state_accessor_driver_/global_state_accessor_driver job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./global_state_accessor_driver
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/e2e/userfuncs"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/proto"
)

// requireUserfuncsPlugin fails fast when the plugin artifact is missing from
// the working directory: without the check, worker startup fails late inside
// raygo's plugin discovery with an opaque error.
func requireUserfuncsPlugin(path string) {
	if _, err := os.Stat(path); err != nil {
		fmt.Fprintf(os.Stderr, "userfuncs plugin not found at %s: build it with `bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin` and ship it in the job's working dir (%v)\n", path, err)
		os.Exit(1)
	}
}

// initRay initializes the Ray runtime with a JobConfig that carries the
// userfuncs.so code search path, so the Counter actors created below can be
// resolved by the Go worker. The GCS address is read from the RAY_ADDRESS
// environment variable (set by `ray job submit`).
func initRay() error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}
	codeSearchPath := filepath.Join(cwd, "userfuncs.so")
	requireUserfuncsPlugin(codeSearchPath)

	jobConfigBuilder := options.NewJobConfigBuilder().
		WithCodeSearchPath(codeSearchPath)

	if err := api.Instance().InitializeWithJobConfig(jobConfigBuilder); err != nil {
		return fmt.Errorf("ray runtime init failed: %w", err)
	}
	log.Printf("driver code search path: %s", codeSearchPath)
	return nil
}

// ensureStateAccessorRegistered forces the native runtime to create the GCS
// client (lazily initialized on first query) so that ConnectClient registers
// the GlobalStateAccessor singleton (SetGlobalStateAccessor). Without this the
// singleton is still nil and gcs.GetGlobalStateAccessor() would report
// ErrNotImplemented. A RuntimeContext cluster query is the cheapest way to
// trigger the lazy GCS client initialization.
func ensureStateAccessorRegistered() error {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	// Querying cluster state initializes the GCS client and therefore the
	// GlobalStateAccessor singleton. The result itself is validated later.
	_ = ctx.GetAllActorInfo()
	return nil
}

// actorIDFromProto decodes an ActorTableData's actor id bytes into ids.ActorID.
func actorIDFromProto(data *proto.ActorTableData) (ids.ActorID, error) {
	if data == nil || len(data.ActorId) == 0 {
		return ids.NilActorID(), fmt.Errorf("actor table data has empty actor id")
	}
	actorID, err := ids.ActorIDFromBinary(data.ActorId)
	if err != nil {
		return ids.NilActorID(), fmt.Errorf("parse actor id %x: %w", data.ActorId, err)
	}
	return actorID, nil
}

// listedInState reports whether the given actor id appears in a listing of
// ActorTableData whose state equals want.
func listedInState(actorID ids.ActorID, want proto.ActorTableData_ActorState, infos []*proto.ActorTableData) bool {
	for _, a := range infos {
		id, err := actorIDFromProto(a)
		if err != nil {
			continue
		}
		if id.Equal(actorID) && a.State == want {
			return true
		}
	}
	return false
}

// actorStateName renders an ActorTableData_ActorState enum as its proto name
// ("PENDING_CREATION", "DEAD", ...) for human-readable diagnostics.
func actorStateName(s proto.ActorTableData_ActorState) string {
	if name, ok := proto.ActorTableData_ActorState_name[int32(s)]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", int32(s))
}

// actorStateDiag tracks the observed state timeline of a single actor, sampled
// from the unfiltered GetAllActorInfo(jobID, nil) listing. testActorStateFilter
// uses it to distinguish a live actor that stays PENDING_CREATION (its creation
// task is still queued while the autoscaler scales up a node) from one that
// transitioned to DEAD (killed / unschedulable) when the poll deadline expires.
type actorStateDiag struct {
	id          ids.ActorID
	inTable     bool
	lastState   proto.ActorTableData_ActorState
	nodeID      string
	pid         uint32
	deathCause  string
	firstSeen   time.Time
	firstSeenAt map[proto.ActorTableData_ActorState]time.Time
}

func newActorStateDiag(id ids.ActorID) *actorStateDiag {
	return &actorStateDiag{
		id:          id,
		firstSeenAt: make(map[proto.ActorTableData_ActorState]time.Time),
	}
}

// observe samples the actor's current state from an unfiltered listing.
func (d *actorStateDiag) observe(now time.Time, infos []*proto.ActorTableData) {
	if d.firstSeen.IsZero() {
		d.firstSeen = now
	}
	for _, a := range infos {
		id, err := actorIDFromProto(a)
		if err != nil || !id.Equal(d.id) {
			continue
		}
		d.inTable = true
		d.lastState = a.State
		if _, ok := d.firstSeenAt[a.State]; !ok {
			d.firstSeenAt[a.State] = now
		}
		if len(a.NodeId) > 0 {
			if nid, err := ids.NodeIDFromBinary(a.NodeId); err == nil {
				d.nodeID = nid.Hex()
			}
		}
		d.pid = a.Pid
		if a.DeathCause != nil {
			d.deathCause = a.DeathCause.String()
		}
		return
	}
	d.inTable = false
}

// timeline renders the observed state transitions relative to the probe start.
func (d *actorStateDiag) timeline(start time.Time) string {
	if d.firstSeen.IsZero() {
		return "never appeared in the unfiltered actor listing"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "first seen +%.1fs", d.firstSeen.Sub(start).Seconds())
	order := []proto.ActorTableData_ActorState{
		proto.ActorTableData_DEPENDENCIES_UNREADY,
		proto.ActorTableData_PENDING_CREATION,
		proto.ActorTableData_ALIVE,
		proto.ActorTableData_RESTARTING,
		proto.ActorTableData_DEAD,
	}
	for _, s := range order {
		if t, ok := d.firstSeenAt[s]; ok {
			fmt.Fprintf(&sb, "; %s at +%.1fs", actorStateName(s), t.Sub(start).Seconds())
		}
	}
	return sb.String()
}

// summary renders the actor's latest observation as a one-line diagnostic.
func (d *actorStateDiag) summary() string {
	if !d.inTable {
		return fmt.Sprintf("actor %s NOT in the unfiltered (job-filtered) actor listing", d.id.Hex())
	}
	return fmt.Sprintf("actor %s state=%s node=%s pid=%d death_cause=%q",
		d.id.Hex(), actorStateName(d.lastState), d.nodeID, d.pid, d.deathCause)
}

// actorStatePollTimeout returns the actor-state probe deadline. The 300s
// default covers autoscaling clusters where the autoscaler must provision a
// new node (VM boot + raylet registration + resource report + worker start)
// before the actor can be placed; on an idle single-node cluster the probe
// finishes in well under a second. It can be raised further via
// GSA_ACTOR_STATE_TIMEOUT (seconds) if provisioning is slower.
func actorStatePollTimeout() (time.Duration, error) {
	raw := os.Getenv("GSA_ACTOR_STATE_TIMEOUT")
	if raw == "" {
		return 300 * time.Second, nil
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		return 0, fmt.Errorf("invalid GSA_ACTOR_STATE_TIMEOUT %q: want a positive number of seconds", raw)
	}
	return time.Duration(secs) * time.Second, nil
}

// testGlobalStateAccessor verifies the GlobalStateAccessor
// singleton resolves to a production implementation (not ErrNotImplemented)
// and representative synchronous queries against GCS return live cluster data.
func testGlobalStateAccessor() error {
	if err := ensureStateAccessorRegistered(); err != nil {
		return err
	}

	accessor, err := gcs.GetGlobalStateAccessor()
	if err != nil {
		return fmt.Errorf("GetGlobalStateAccessor() returned %v, want a production implementation", err)
	}
	if accessor == nil {
		return fmt.Errorf("GetGlobalStateAccessor() returned nil accessor")
	}
	fmt.Println("  GetGlobalStateAccessor() resolves to a production implementation ✓")

	// Connect must succeed on a live cluster.
	connected, err := accessor.Connect()
	if err != nil || !connected {
		return fmt.Errorf("accessor.Connect() = %v (err %v), want true/nil", connected, err)
	}
	fmt.Println("  accessor.Connect() -> true ✓")

	// Job info: the current driver's job must be visible.
	jobs, err := accessor.GetAllJobInfo(false, false)
	if err != nil {
		return fmt.Errorf("GetAllJobInfo() failed: %w", err)
	}
	if len(jobs) < 1 {
		return fmt.Errorf("GetAllJobInfo() = %d jobs, want >= 1", len(jobs))
	}
	fmt.Printf("  GetAllJobInfo() = %d job(s) ✓\n", len(jobs))

	// Next job id: must be a valid non-nil job id.
	nextJobID, err := accessor.GetNextJobID()
	if err != nil {
		return fmt.Errorf("GetNextJobID() failed: %w", err)
	}
	if nextJobID.IsNil() {
		return fmt.Errorf("GetNextJobID() = nil job id")
	}
	fmt.Printf("  GetNextJobID() = %s ✓\n", nextJobID.Hex())

	// Node info: at least the head node must be present.
	nodes, err := accessor.GetAllNodeInfo()
	if err != nil {
		return fmt.Errorf("GetAllNodeInfo() failed: %w", err)
	}
	if len(nodes) < 1 {
		return fmt.Errorf("GetAllNodeInfo() = %d nodes, want >= 1", len(nodes))
	}
	alive := 0
	for _, n := range nodes {
		if n != nil && n.State == proto.GcsNodeInfo_ALIVE {
			alive++
		}
	}
	fmt.Printf("  GetAllNodeInfo() = %d node(s), %d alive ✓\n", len(nodes), alive)

	// Worker info: workers (driver at least) are registered.
	workers, err := accessor.GetAllWorkerInfo()
	if err != nil {
		return fmt.Errorf("GetAllWorkerInfo() failed: %w", err)
	}
	if len(workers) < 1 {
		return fmt.Errorf("GetAllWorkerInfo() = %d workers, want >= 1", len(workers))
	}
	fmt.Printf("  GetAllWorkerInfo() = %d worker(s) ✓\n", len(workers))

	fmt.Println("  GlobalStateAccessor production queries verified ✓")
	return nil
}

// testGlobalStateAccessorExtended verifies the seven newly implemented
// GlobalStateAccessor methods against the live cluster: system config, resource
// usage, task events, draining nodes, and
// the worker debugger methods (using a real worker id from GetAllWorkerInfo).
func testGlobalStateAccessorExtended() error {
	accessor, err := gcs.GetGlobalStateAccessor()
	if err != nil {
		return fmt.Errorf("GetGlobalStateAccessor() failed: %w", err)
	}

	cfg, err := accessor.GetSystemConfig()
	if err != nil {
		return fmt.Errorf("GetSystemConfig() failed: %w", err)
	}
	if len(cfg) == 0 {
		return fmt.Errorf("GetSystemConfig() returned empty config")
	}
	fmt.Printf("  GetSystemConfig() = %d bytes ✓\n", len(cfg))

	usage, err := accessor.GetAllResourceUsage()
	if err != nil {
		return fmt.Errorf("GetAllResourceUsage() failed: %w", err)
	}
	if usage == nil {
		return fmt.Errorf("GetAllResourceUsage() returned nil batch")
	}
	fmt.Printf("  GetAllResourceUsage() = %d batch entries ✓\n", len(usage.Batch))

	events, err := accessor.GetAllTaskEvents()
	if err != nil {
		return fmt.Errorf("GetAllTaskEvents() failed: %w", err)
	}
	fmt.Printf("  GetAllTaskEvents() = %d event(s) ✓\n", len(events))

	draining, err := accessor.GetDrainingNodes()
	if err != nil {
		return fmt.Errorf("GetDrainingNodes() failed: %w", err)
	}
	if draining == nil {
		return fmt.Errorf("GetDrainingNodes() returned nil map")
	}
	fmt.Printf("  GetDrainingNodes() = %d node(s) ✓\n", len(draining))

	workers, err := accessor.GetAllWorkerInfo()
	if err != nil {
		return fmt.Errorf("GetAllWorkerInfo() failed: %w", err)
	}
	var workerID ids.WorkerID
	for _, w := range workers {
		if w == nil || w.WorkerAddress == nil || len(w.WorkerAddress.WorkerId) == 0 {
			continue
		}
		if id, err := ids.WorkerIDFromBinary(w.WorkerAddress.WorkerId); err == nil {
			workerID = id
			break
		}
	}
	if workerID.IsNil() {
		return fmt.Errorf("no worker id available from GetAllWorkerInfo()")
	}

	port, err := accessor.GetWorkerDebuggerPort(workerID)
	if err != nil {
		return fmt.Errorf("GetWorkerDebuggerPort() failed: %w", err)
	}
	fmt.Printf("  GetWorkerDebuggerPort(%s) = %d ✓\n", workerID.Hex(), port)

	ok, err := accessor.UpdateWorkerDebuggerPort(workerID, port)
	if err != nil {
		return fmt.Errorf("UpdateWorkerDebuggerPort() failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("UpdateWorkerDebuggerPort() returned false")
	}

	ok, err = accessor.UpdateWorkerNumPausedThreads(workerID, 0)
	if err != nil {
		return fmt.Errorf("UpdateWorkerNumPausedThreads() failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("UpdateWorkerNumPausedThreads() returned false")
	}
	fmt.Println("  worker debugger port / paused-thread methods ✓")

	fmt.Println("  GlobalStateAccessor extended methods verified ✓")
	return nil
}

// testActorStateFilter verifies GetAllActorInfo(jobID, state)
// pushes the actor-state filter down to the GCS, returning only actors in the
// requested state. It creates two Counter actors, kills one (which transitions
// it to DEAD), then asserts the ALIVE query returns only the live actor and
// the DEAD query returns only the dead one. The same filtering is cross-checked
// against RuntimeContext.GetAllActorInfoByFilter for Java parity.
func testActorStateFilter() error {
	pollTimeout, err := actorStatePollTimeout()
	if err != nil {
		return err
	}
	aliveHandle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(10)
	if err != nil {
		return fmt.Errorf("create alive actor failed: %w", err)
	}
	defer func() { _ = api.KillActor(aliveHandle, true, true) }()
	aliveID := aliveHandle.ID()

	deadHandle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(20)
	if err != nil {
		return fmt.Errorf("create doomed actor failed: %w", err)
	}
	defer func() { _ = api.KillActor(deadHandle, true, true) }()
	deadID := deadHandle.ID()

	// Kill the second actor with noRestart=true so it transitions to DEAD and
	// never comes back.
	if err := api.KillActor(deadHandle, true, true); err != nil {
		return fmt.Errorf("kill doomed actor failed: %w", err)
	}

	// Get the current job id for filtering.
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	jobID := ctx.JobID()
	var jobIDPtr *ids.JobID
	if !jobID.IsNil() {
		j := jobID
		jobIDPtr = &j
	}

	accessor, err := gcs.GetGlobalStateAccessor()
	if err != nil {
		return fmt.Errorf("GetGlobalStateAccessor() failed: %w", err)
	}

	// Both actors must eventually register (asynchronous). GO workers are
	// started on demand (RAY_ENABLE_GO_SETUP_WORKER=1), so the alive actor's
	// construction task may take ~200ms to even reach a worker while the killed
	// actor transitions to DEAD immediately. Poll until the DEAD query observes
	// the killed actor AND the ALIVE query observes the live actor, so both
	// filter assertions below are stable.
	//
	// On an autoscaling cluster the alive actor may stay PENDING_CREATION for a
	// long time while the autoscaler provisions a node (no idle capacity), so on
	// timeout the diagnostic reports the actor's actual state timeline to
	// distinguish "still queued for scheduling" (PENDING_CREATION) from "killed
	// or unschedulable" (DEAD). The deadline is configurable via
	// GSA_ACTOR_STATE_TIMEOUT (seconds, default 300).
	aliveState := gcs.ActorStateNameAlive
	deadState := gcs.ActorStateNameDead
	fmt.Printf("  actor-state probe deadline: %s (override with GSA_ACTOR_STATE_TIMEOUT seconds)\n", pollTimeout)
	start := time.Now()
	deadline := start.Add(pollTimeout)
	deadObserved := false
	aliveObserved := false
	aliveDiag := newActorStateDiag(aliveID)
	deadDiag := newActorStateDiag(deadID)
	lastProbeLog := time.Time{}
	lastDiag := time.Time{}
	for {
		now := time.Now()

		// Unfiltered listing: the diagnostic baseline for both actors. The
		// state filter itself is separately verified by the DEAD/ALIVE queries.
		// This diagnostic query is throttled to the same 5s cadence as the
		// progress log (it is only consumed by the timeout diagnostics), so a
		// long poll (up to GSA_ACTOR_STATE_TIMEOUT) does not issue a full-table
		// GCS scan on every 200ms iteration.
		if now.Sub(lastDiag) >= 5*time.Second {
			allActors, err := accessor.GetAllActorInfo(jobIDPtr, nil)
			if err != nil {
				return fmt.Errorf("GetAllActorInfo(job, all states) failed: %w", err)
			}
			aliveDiag.observe(now, allActors)
			deadDiag.observe(now, allActors)
			lastDiag = now
		}

		if !deadObserved {
			deadActors, err := accessor.GetAllActorInfo(jobIDPtr, &deadState)
			if err != nil {
				return fmt.Errorf("GetAllActorInfo(job, DEAD) failed: %w", err)
			}
			if listedInState(deadID, proto.ActorTableData_DEAD, deadActors) {
				deadObserved = true
			}
		}
		if !aliveObserved {
			aliveActors, err := accessor.GetAllActorInfo(jobIDPtr, &aliveState)
			if err != nil {
				return fmt.Errorf("GetAllActorInfo(job, ALIVE) failed: %w", err)
			}
			if listedInState(aliveID, proto.ActorTableData_ALIVE, aliveActors) {
				aliveObserved = true
			}
		}
		if deadObserved && aliveObserved {
			break
		}
		if now.After(deadline) {
			return fmt.Errorf("actor-state probe timed out after %s (deadObserved=%v, aliveObserved=%v):\n"+
				"  alive actor diag:  %s\n"+
				"  alive actor trace: %s\n"+
				"  dead actor diag:   %s\n"+
				"  dead actor trace:  %s\n"+
				"  hint: if the alive actor is stuck in %s its creation task was never "+
				"scheduled to a node (node is empty). On an autoscaling cluster this almost "+
				"always means the autoscaler is still provisioning a node (VM boot + raylet "+
				"registration + resource report + worker start can exceed the probe deadline); "+
				"raise GSA_ACTOR_STATE_TIMEOUT (seconds) and retry. If it went DEAD the actor "+
				"was killed or unschedulable and will never reach ALIVE.",
				pollTimeout, deadObserved, aliveObserved,
				aliveDiag.summary(), aliveDiag.timeline(start),
				deadDiag.summary(), deadDiag.timeline(start),
				actorStateName(aliveDiag.lastState))
		}
		// Print a progress line every 5s so a slow autoscaler scale-up is
		// visible in the job log.
		if now.Sub(lastProbeLog) >= 5*time.Second {
			lastProbeLog = now
			log.Printf("  [probe] t=+%.1fs alive={%s} dead={%s}",
				now.Sub(start).Seconds(), aliveDiag.summary(), deadDiag.summary())
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("  killed actor %s reached DEAD in GCS ✓\n", deadID.Hex())
	fmt.Printf("  live actor %s reached ALIVE in GCS ✓\n", aliveID.Hex())
	fmt.Printf("  [diag] live actor trace: %s\n", aliveDiag.timeline(start))
	fmt.Printf("  [diag] dead actor trace: %s\n", deadDiag.timeline(start))

	// ALIVE filter: must include the live actor and exclude the dead one.
	aliveActors, err := accessor.GetAllActorInfo(jobIDPtr, &aliveState)
	if err != nil {
		return fmt.Errorf("GetAllActorInfo(job, ALIVE) failed: %w", err)
	}
	if !listedInState(aliveID, proto.ActorTableData_ALIVE, aliveActors) {
		return fmt.Errorf("GetAllActorInfo(job, ALIVE) missing live actor %s", aliveID.Hex())
	}
	if listedInState(deadID, proto.ActorTableData_ALIVE, aliveActors) {
		return fmt.Errorf("GetAllActorInfo(job, ALIVE) included killed actor %s", deadID.Hex())
	}
	fmt.Printf("  GetAllActorInfo(job, ALIVE) = %d actor(s): live present, dead excluded ✓\n", len(aliveActors))

	// DEAD filter: must include the dead actor and exclude the live one.
	deadActors, err := accessor.GetAllActorInfo(jobIDPtr, &deadState)
	if err != nil {
		return fmt.Errorf("GetAllActorInfo(job, DEAD) failed: %w", err)
	}
	if !listedInState(deadID, proto.ActorTableData_DEAD, deadActors) {
		return fmt.Errorf("GetAllActorInfo(job, DEAD) missing killed actor %s", deadID.Hex())
	}
	if listedInState(aliveID, proto.ActorTableData_DEAD, deadActors) {
		return fmt.Errorf("GetAllActorInfo(job, DEAD) included live actor %s", aliveID.Hex())
	}
	fmt.Printf("  GetAllActorInfo(job, DEAD) = %d actor(s): dead present, live excluded ✓\n", len(deadActors))

	// Cross-check with the RuntimeContext API (Java-parity filtered overload):
	// the same actor-state filtering must hold through GetAllActorInfoByFilter.
	rtAlive := ctx.GetAllActorInfoByFilter(jobID, api.ActorStateAlive)
	rtAliveFound := false
	for _, a := range rtAlive {
		if a.ActorID == aliveID {
			rtAliveFound = true
		}
		if a.ActorID == deadID {
			return fmt.Errorf("RuntimeContext GetAllActorInfoByFilter(Alive) included killed actor %s", deadID.Hex())
		}
	}
	if !rtAliveFound {
		return fmt.Errorf("RuntimeContext GetAllActorInfoByFilter(Alive) missing live actor %s", aliveID.Hex())
	}

	rtDead := ctx.GetAllActorInfoByFilter(jobID, api.ActorStateDead)
	rtDeadFound := false
	for _, a := range rtDead {
		if a.ActorID == deadID {
			rtDeadFound = true
		}
		if a.ActorID == aliveID {
			return fmt.Errorf("RuntimeContext GetAllActorInfoByFilter(Dead) included live actor %s", aliveID.Hex())
		}
	}
	if !rtDeadFound {
		return fmt.Errorf("RuntimeContext GetAllActorInfoByFilter(Dead) missing killed actor %s", deadID.Hex())
	}
	fmt.Println("  RuntimeContext.GetAllActorInfoByFilter cross-check matches (Alive/Dead) ✓")

	// Release the surviving actor so its worker can be recycled.
	typed := api.NewActorHandleImpl[struct{}](aliveID)
	if _, err := typed.Task((*userfuncs.Counter).Exit).Remote(); err != nil {
		return fmt.Errorf("submit Exit for cleanup failed: %w", err)
	}
	fmt.Println("  Actor state filter verified ✓")
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestGlobalStateAccessor ===")
	if err := testGlobalStateAccessor(); err != nil {
		log.Fatalf("TestGlobalStateAccessor FAILED: %v", err)
	}
	fmt.Println("=== TestGlobalStateAccessor PASSED ===")

	fmt.Println("=== TestGlobalStateAccessorExtended ===")
	if err := testGlobalStateAccessorExtended(); err != nil {
		log.Fatalf("TestGlobalStateAccessorExtended FAILED: %v", err)
	}
	fmt.Println("=== TestGlobalStateAccessorExtended PASSED ===")

	fmt.Println("=== TestActorStateFilter ===")
	if err := testActorStateFilter(); err != nil {
		log.Fatalf("TestActorStateFilter FAILED: %v", err)
	}
	fmt.Println("=== TestActorStateFilter PASSED ===")

	fmt.Println("All GlobalStateAccessor / state-filter checks passed.")
}
