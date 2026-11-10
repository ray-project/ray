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

package job

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRayException implements object.RayException so the supervisor error
// classification can be exercised without a real core worker.
type fakeRayException struct {
	msg  string
	code int
}

func (f *fakeRayException) Error() string   { return f.msg }
func (f *fakeRayException) ToBytes() []byte { return nil }
func (f *fakeRayException) ErrorCode() int  { return f.code }

func TestClassifySupervisorError(t *testing.T) {
	// RPC exception codes
	msg, et := classifySupervisorError(&fakeRayException{msg: "x", code: rpcErrorTypeRuntimeEnvSetupFailed})
	assert.Equal(t, JobErrorTypeRuntimeEnvSetupFailure, et)
	assert.Contains(t, *msg, "runtime_env setup failed")

	msg, et = classifySupervisorError(&fakeRayException{msg: "x", code: rpcErrorTypeActorUnschedulable})
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnschedulable, et)

	msg, et = classifySupervisorError(&fakeRayException{msg: "x", code: object.RpcErrorTypeActorDied})
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	msg, et = classifySupervisorError(&fakeRayException{msg: "x", code: object.ErrorCodeActorDied})
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	// typed errors
	msg, et = classifySupervisorError(newActorError("runtime_env_setup", errors.New("boom")))
	assert.Equal(t, JobErrorTypeRuntimeEnvSetupFailure, et)

	msg, et = classifySupervisorError(newActorError("unschedulable", errors.New("boom")))
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnschedulable, et)

	msg, et = classifySupervisorError(newActorError("died", errors.New("boom")))
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	// string matching fallbacks
	msg, et = classifySupervisorError(errors.New("ActorUnschedulableError occurred"))
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnschedulable, et)
	assert.Contains(t, *msg, "could not be scheduled")

	msg, et = classifySupervisorError(errors.New("RuntimeEnvSetupError"))
	assert.Equal(t, JobErrorTypeRuntimeEnvSetupFailure, et)

	msg, et = classifySupervisorError(errors.New("actor died"))
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	// unknown -> ActorUnknownFailure
	msg, et = classifySupervisorError(errors.New("something else"))
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnknownFailure, et)
	assert.Contains(t, *msg, "Unexpected error occurred")
}

func TestContainsAny(t *testing.T) {
	assert.True(t, containsAny("hello world", "world"))
	assert.True(t, containsAny("abc", "a", "z"))
	assert.False(t, containsAny("abc", "x", "y"))
	assert.False(t, containsAny("", "a"))
}

func TestIsActorNotFoundError(t *testing.T) {
	assert.True(t, isActorNotFoundError(errors.New("actor not found")))
	assert.False(t, isActorNotFoundError(errors.New("other error")))
	assert.False(t, isActorNotFoundError(nil))
}

func TestSupervisorMetadata(t *testing.T) {
	m := map[string]string{"k": "v"}
	assert.Equal(t, m, supervisorMetadata(m))
	assert.Equal(t, map[string]string{}, supervisorMetadata(nil))
}

func TestJoinLines(t *testing.T) {
	assert.Equal(t, "ab\ncd\n", joinLines([]string{"ab\n", "cd\n"}))
	assert.Equal(t, "", joinLines(nil))
}

// TestGetSupervisorRuntimeEnvSerializationError verifies a runtime env that
// cannot be serialized is reported to the caller instead of yielding an empty
// env, which would run the job without its pip packages, env vars or working
// dir.
func TestGetSupervisorRuntimeEnvSerializationError(t *testing.T) {
	jm := &JobManager{}
	_, err := jm.getSupervisorRuntimeEnv(map[string]interface{}{
		"env_vars": map[string]interface{}{"bad": make(chan int)},
	}, "sub1", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to serialize user runtime env")
}

// TestSubmitJobRuntimeEnvSerializationFailure routes the same failure through
// submitJob: the job is recorded and immediately marked FAILED with the
// supervisor-start error type, instead of running with a silently emptied
// runtime env.
func TestSubmitJobRuntimeEnvSerializationFailure(t *testing.T) {
	ctx := t.Context()
	jm, sink := newTestJobManager(t)
	jm.recoverDone = make(chan struct{})
	close(jm.recoverDone)

	submissionID, err := jm.submitJob(ctx, &JobSubmitRequest{
		Entrypoint:   "echo",
		SubmissionID: ptrString("sub1"),
		RuntimeEnv:   map[string]interface{}{"bad": make(chan int)},
	})
	require.NoError(t, err)
	assert.Equal(t, "sub1", submissionID)

	info, err := jm.jobInfoClient.GetInfo(ctx, "sub1")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, JobStatusFailed, info.Status)
	require.NotNil(t, info.ErrorType)
	assert.Equal(t, string(JobErrorTypeJobSupervisorActorStartFailure), *info.ErrorType)
	require.NotNil(t, info.Message)
	assert.Contains(t, *info.Message, "failed to serialize runtime env")
	assert.Nil(t, info.RuntimeEnvJSON, "an unserializable env must not be recorded as empty")
	assert.Len(t, sink.started, 1)
}

func TestJobStartTimeoutSeconds(t *testing.T) {
	t.Setenv(rayJobStartTimeoutSecondsEnvVar, "")
	assert.Equal(t, float64(defaultJobStartTimeoutSeconds), jobStartTimeoutSeconds())

	t.Setenv(rayJobStartTimeoutSecondsEnvVar, "30")
	assert.Equal(t, 30.0, jobStartTimeoutSeconds())

	t.Setenv(rayJobStartTimeoutSecondsEnvVar, "not-a-number")
	assert.Equal(t, float64(defaultJobStartTimeoutSeconds), jobStartTimeoutSeconds())
}

func TestSupervisorCodeSearchPath(t *testing.T) {
	paths := supervisorCodeSearchPath()
	require.NotEmpty(t, paths)
	assert.NotEmpty(t, paths[0])
}

// TestRayInternalDashboardNamespace pins the composed namespace against the
// double-underscore regression: the shared prefix already ends in "_".
func TestRayInternalDashboardNamespace(t *testing.T) {
	assert.Equal(t, "_ray_internal_dashboard", rayInternalDashboardNamespace)
}

func TestApplySchedulingStrategyResourcesSpecified(t *testing.T) {
	// An entrypoint with explicit resources keeps the default scheduling so
	// the actor lands where its resources are available.
	ctx := t.Context()
	client := newFakeGCSClient()
	_, err := client.Put(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte(testHeadNodeIDHex), false)
	require.NoError(t, err)
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}

	assert.Nil(t, jm.applySchedulingStrategy("sub1", true))
}

func TestApplySchedulingStrategyNoHeadNode(t *testing.T) {
	// Without a head node id in the KV the strategy must fall back to default
	// scheduling (nil affinity) and submission proceeds.
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(newFakeGCSClient()), ctx: t.Context()}
	assert.Nil(t, jm.applySchedulingStrategy("sub1", false))
}

func TestApplySchedulingStrategyPinsToHeadNode(t *testing.T) {
	ctx := t.Context()
	client := newFakeGCSClient()
	_, err := client.Put(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte(testHeadNodeIDHex), false)
	require.NoError(t, err)
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}

	opts := jm.applySchedulingStrategy("sub1", false)
	require.NotNil(t, opts)
	assert.Equal(t, testHeadNodeIDHex, opts.NodeID)
	assert.False(t, opts.Soft, "head-node pinning must be strict")
}

func TestApplySchedulingStrategyAllowDriverOnWorkers(t *testing.T) {
	t.Setenv(rayJobAllowDriverOnWorkerNodesEnvVar, "1")
	ctx := t.Context()
	client := newFakeGCSClient()
	_, err := client.Put(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte(testHeadNodeIDHex), false)
	require.NoError(t, err)
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}

	assert.Nil(t, jm.applySchedulingStrategy("sub1", false))
}

func TestAppendDriverLog(t *testing.T) {
	jm := &JobManager{ctx: t.Context(), logClient: NewJobLogStorageClient(t.TempDir())}
	jm.appendDriverLog("raysubmit_1", "Runtime env is setting up.")
	jm.appendDriverLog("raysubmit_1", "Job failed.")
	assert.Equal(t, "Runtime env is setting up.\nJob failed.\n", jm.getJobLogs("raysubmit_1"))
}

// TestPutStatusAfterTerminal proves a terminal status is not overwritten.
func TestPutStatusAfterTerminal(t *testing.T) {
	ctx := t.Context()
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(newFakeGCSClient()), ctx: ctx}
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)

	previous, err := jm.putStatus(ctx, "job1", JobStatusSucceeded, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, previous)
	assert.Equal(t, JobStatusPending, *previous)
	previous, err = jm.putStatus(ctx, "job1", JobStatusFailed, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "terminal")
	require.NotNil(t, previous)
	assert.Equal(t, JobStatusSucceeded, *previous)

	status, err := jm.getJobStatus(ctx, "job1")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, JobStatusSucceeded, *status)
}

// overlapTrackingGCSClient wraps the in-memory fake and detects overlapping
// internal-KV reads, which is what a non-serialized read-modify-write looks
// like from the storage client's point of view.
type overlapTrackingGCSClient struct {
	*fakeGCSClient

	mu       sync.Mutex
	inFlight int
	overlaps int
	delay    time.Duration
}

func (c *overlapTrackingGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > 1 {
		c.overlaps++
	}
	c.mu.Unlock()
	time.Sleep(c.delay)
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()
	return c.fakeGCSClient.Get(ctx, ns, key)
}

func (c *overlapTrackingGCSClient) overlapCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overlaps
}

// TestPutStatusConcurrentTransitionsDoNotResurrect pins the JobManager
// serialization of status writes: two concurrent transitions must not both
// read the pending status and then write, which would let a FAILED write
// overwrite a terminal SUCCEEDED status (the storage client has no CAS).
func TestPutStatusConcurrentTransitionsDoNotResurrect(t *testing.T) {
	ctx := t.Context()
	client := &overlapTrackingGCSClient{fakeGCSClient: newFakeGCSClient(), delay: 25 * time.Millisecond}
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, err := jm.putStatus(ctx, "job1", JobStatusSucceeded, nil, nil, nil)
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		_, err := jm.putStatus(ctx, "job1", JobStatusFailed, nil, nil, nil)
		errs <- err
	}()
	close(start)
	wg.Wait()
	close(errs)

	accepted := 0
	for err := range errs {
		if err == nil {
			accepted++
		}
	}
	assert.Equal(t, 1, accepted, "exactly one of the racing transitions must win")
	assert.Zero(t, client.overlapCount(), "status read-modify-write must not overlap")

	status, err := jm.getJobStatus(ctx, "job1")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.IsTerminal())
}

// TestPutStatusDoesNotResurrectDeletedJob pins the delete tombstone: a monitor
// that read the status before a concurrent delete must not recreate the record
// (PutStatus creates missing records).
func TestPutStatusDoesNotResurrectDeletedJob(t *testing.T) {
	ctx := t.Context()
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(newFakeGCSClient()), ctx: ctx}
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)
	require.NoError(t, jm.jobInfoClient.DeleteInfo(ctx, "job1"))

	previous, err := jm.putStatus(ctx, "job1", JobStatusFailed, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, previous)

	info, err := jm.jobInfoClient.GetInfo(ctx, "job1")
	require.NoError(t, err)
	assert.Nil(t, info, "a monitor status write must not recreate a deleted job")
}

// completedEvent records one submissionCompleted emission for assertions.
type completedEvent struct {
	jobID  string
	status JobStatus
	errMsg string
}

// recordingEventSink captures the manager's event emissions in memory.
type recordingEventSink struct {
	started   []string
	completed []completedEvent
}

func (r *recordingEventSink) submissionStarted(submissionID string) {
	r.started = append(r.started, submissionID)
}

func (r *recordingEventSink) submissionCompleted(jobID string, status JobStatus, errorMessage string) {
	r.completed = append(r.completed, completedEvent{jobID: jobID, status: status, errMsg: errorMessage})
}

// newTestJobManager builds a manager backed by the in-memory GCS and a
// recording event sink.
func newTestJobManager(t *testing.T) (*JobManager, *recordingEventSink) {
	t.Helper()
	sink := &recordingEventSink{}
	jm := &JobManager{
		ctx:           t.Context(),
		jobInfoClient: NewJobInfoStorageClient(newFakeGCSClient()),
		logClient:     NewJobLogStorageClient(t.TempDir()),
		eventLogger:   sink,
	}
	return jm, sink
}

// TestHandleSupervisorFailureAfterCleanStop covers the stop race: the monitor
// loop still holds a stale PENDING status when the ping fails because the job
// was stopped, so the failure handler must re-read the status and treat the
// actor exit as expected (no FAILED event, no error line in the driver log).
func TestHandleSupervisorFailureAfterCleanStop(t *testing.T) {
	ctx := t.Context()
	jm, sink := newTestJobManager(t)
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)
	// The Python supervisor writes STOPPED while the monitor already read
	// PENDING.
	require.NoError(t, jm.jobInfoClient.PutStatus(ctx, "job1", JobStatusStopped, nil, nil, nil))

	jm.handleSupervisorFailure("job1", errors.New("actor died"))

	status, err := jm.getJobStatus(ctx, "job1")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, JobStatusStopped, *status)
	require.Len(t, sink.completed, 1)
	assert.Equal(t, JobStatusStopped, sink.completed[0].status)
	assert.Empty(t, sink.completed[0].errMsg)
	assert.Empty(t, jm.getJobLogs("job1"), "an expected exit must not write an error line")
	assert.Empty(t, sink.started)
}

// TestHandleSupervisorFailureWritesFailed covers the genuine failure path: the
// classified error is persisted, emitted, and appended to the driver log.
func TestHandleSupervisorFailureWritesFailed(t *testing.T) {
	ctx := t.Context()
	jm, sink := newTestJobManager(t)
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)

	jm.handleSupervisorFailure("job1", errors.New("ActorDiedError: supervisor exited"))

	status, err := jm.getJobStatus(ctx, "job1")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, JobStatusFailed, *status)
	require.Len(t, sink.completed, 1)
	assert.Equal(t, JobStatusFailed, sink.completed[0].status)
	assert.NotEmpty(t, sink.completed[0].errMsg)
	assert.Contains(t, jm.getJobLogs("job1"), "Job supervisor actor died")
}

// stagedStatusGCSClient scripts the status returned for one job's internal-KV
// reads and repeats the last scripted status afterwards. It reproduces the
// interleaving where the monitor's re-read sees PENDING but the status write's
// own read sees the terminal status a concurrent stop just wrote.
type stagedStatusGCSClient struct {
	*fakeGCSClient
	key      string
	statuses []JobStatus

	mu    sync.Mutex
	reads int
}

func (c *stagedStatusGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	if key != c.key || len(c.statuses) == 0 {
		return c.fakeGCSClient.Get(ctx, ns, key)
	}
	c.mu.Lock()
	idx := c.reads
	c.reads++
	c.mu.Unlock()
	if idx >= len(c.statuses) {
		idx = len(c.statuses) - 1
	}
	payload, err := json.Marshal(&JobInfo{Entrypoint: "python -c pass", Status: c.statuses[idx]})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// TestHandleSupervisorFailureAfterTerminalRace covers the residual window: the
// handler's re-read still sees PENDING, but the status write's own read sees
// the STOPPED status a concurrent stop wrote. The exit must be reported as
// expected, with no FAILED event, no error log line, and no driver log line.
func TestHandleSupervisorFailureAfterTerminalRace(t *testing.T) {
	ctx := t.Context()
	sink := &recordingEventSink{}
	client := &stagedStatusGCSClient{
		fakeGCSClient: newFakeGCSClient(),
		key:           jobDataKeyFor("job1"),
		statuses:      []JobStatus{JobStatusPending, JobStatusStopped},
	}
	jm := &JobManager{
		ctx:           ctx,
		jobInfoClient: NewJobInfoStorageClient(client),
		logClient:     NewJobLogStorageClient(t.TempDir()),
		eventLogger:   sink,
	}

	jm.handleSupervisorFailure("job1", errors.New("actor died"))

	require.Len(t, sink.completed, 1)
	assert.Equal(t, JobStatusStopped, sink.completed[0].status)
	assert.Empty(t, sink.completed[0].errMsg)
	assert.Empty(t, jm.getJobLogs("job1"))
	_, err := os.Stat(jm.logClient.getLogFilePath("job1"))
	assert.True(t, os.IsNotExist(err), "an expected exit must not create a driver log")
}

// TestHandleSupervisorFailureAfterDelete covers the deleted-record window: the
// write is dropped, and neither the FAILED event nor the driver log line is
// emitted, so the monitor does not recreate artifacts of a deleted job.
func TestHandleSupervisorFailureAfterDelete(t *testing.T) {
	ctx := t.Context()
	jm, sink := newTestJobManager(t)
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)
	require.NoError(t, jm.jobInfoClient.DeleteInfo(ctx, "job1"))

	jm.handleSupervisorFailure("job1", errors.New("actor died"))

	assert.Empty(t, sink.completed, "a deleted job must not emit a completion event")
	assert.Empty(t, jm.getJobLogs("job1"))
	_, err = os.Stat(jm.logClient.getLogFilePath("job1"))
	assert.True(t, os.IsNotExist(err), "a deleted job must not get a recreated driver log")
}
