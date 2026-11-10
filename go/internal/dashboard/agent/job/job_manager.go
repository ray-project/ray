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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/jobutil"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

const (
	rayJobStartTimeoutSecondsEnvVar            = "RAY_JOB_START_TIMEOUT_SECONDS"
	defaultJobStartTimeoutSeconds              = 60 * 15
	rayJobAllowDriverOnWorkerNodesEnvVar       = "RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES"
	rayStreamRuntimeEnvLogToJobDriverLogEnvVar = "RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG"
	rayWorkerNiceness                          = "RAY_worker_niceness"
	noSetCudaVisibleDevicesEnvVar              = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"
	noSetAscendRTVisibleDevicesEnvVar          = "RAY_EXPERIMENTAL_NOSET_ASCEND_RT_VISIBLE_DEVICES"
	// rayInternalDashboardNamespace is the job config namespace of the agent's
	// driver runtime, composed like the Python job manager's
	// RAY_INTERNAL_NAMESPACE_PREFIX + "dashboard". RayInternalNamespacePrefix
	// already ends in "_", so the suffix carries no leading underscore.
	rayInternalDashboardNamespace = jobutil.RayInternalNamespacePrefix + "dashboard"
)

const jobMonitorLoopPeriodS = time.Second

// jobEventSink is the event emission surface used by the job manager. The
// production implementation is *jobEventLogger, whose emit methods are
// nil-safe; the interface keeps the monitor's event decisions testable.
type jobEventSink interface {
	submissionStarted(submissionID string)
	submissionCompleted(jobID string, status JobStatus, errorMessage string)
}

// JobManager drives the Python JobSupervisor actors of the submission jobs
// owned by this agent. Behavior mirrors the Python dashboard's JobManager.
type JobManager struct {
	ctx context.Context

	gcsClient  gcs.Client
	logsDir    string
	gcsAddress string
	nodeIP     string

	jobInfoClient *JobInfoStorageClient
	logClient     *JobLogStorageClient
	eventLogger   jobEventSink

	monitoredJobs sync.Map
	recoverDone   chan struct{}

	runtimeOnce sync.Once
	runtimeErr  error

	// statusMu serializes the status read-modify-write of every submission
	// owned by this manager. JobInfoStorageClient.PutStatus has no
	// compare-and-swap, so without it the monitor goroutine and a concurrent
	// stop/delete can both read a non-terminal status and the later write
	// resurrects a job that already reached a terminal state.
	statusMu sync.Mutex
}

// NewJobManager creates the job manager and starts recovering the jobs that
// were still running before the agent restarted.
func NewJobManager(ctx context.Context, gcsClient gcs.Client, logsDir, nodeIP string) *JobManager {
	jm := &JobManager{
		ctx:           ctx,
		gcsClient:     gcsClient,
		logsDir:       logsDir,
		gcsAddress:    gcsClient.Address(),
		nodeIP:        nodeIP,
		jobInfoClient: NewJobInfoStorageClient(gcsClient),
		logClient:     NewJobLogStorageClient(logsDir),
		eventLogger:   newJobEventLogger(logsDir),
		recoverDone:   make(chan struct{}),
	}
	go jm.recoverRunningJobs()
	return jm
}

// ensureRuntime lazily initializes the Go runtime as a driver so the agent can
// create Python actor supervisors. The runtime is shared by every submission
// of this agent process.
func (m *JobManager) ensureRuntime() error {
	m.runtimeOnce.Do(func() {
		if api.IsInitialized() {
			return
		}
		codeSearchPath := supervisorCodeSearchPath()
		builder := options.NewJobConfigBuilder().
			WithNamespace(rayInternalDashboardNamespace).
			WithCodeSearchPath(codeSearchPath...)
		if err := builder.MergeFromEnv(); err != nil {
			m.runtimeErr = fmt.Errorf("failed to build JobConfig for job submission: %w", err)
			return
		}
		jobOpts, err := builder.BuildToJobOptions()
		if err != nil {
			m.runtimeErr = fmt.Errorf("failed to build JobConfig for job submission: %w", err)
			return
		}
		log.Log.Info("initialized JobConfig for job submission",
			"codeSearchPath", codeSearchPath)
		opts := &options.InitializeOptions{
			WorkerType: options.WorkerTypeDriver,
			Network: options.NetworkOptions{
				GcsAddress:    m.gcsAddress,
				NodeIPAddress: m.nodeIP,
			},
			Runtime: options.RuntimeOptions{
				LogDir: m.logsDir,
			},
			Job: options.JobOptions{
				JobConfig: jobOpts.JobConfig,
			},
			SkipDriverLogSetup: true,
		}
		if err := api.InitWithOptions(opts); err != nil {
			m.runtimeErr = fmt.Errorf("failed to initialize Ray runtime for job submission: %w", err)
			return
		}
		if err := verifyTaskSubmitter(); err != nil {
			m.runtimeErr = err
		}
	})
	return m.runtimeErr
}

// supervisorCodeSearchPath returns the directory the Python workers should add
// to sys.path so they can import the JobSupervisor class locally instead of
// loading it from the GCS function table (which Go cannot export).
func supervisorCodeSearchPath() []string {
	if exe, err := os.Executable(); err == nil {
		cmdDir := filepath.Dir(exe)
		if filepath.Base(cmdDir) == "cmd" {
			return []string{filepath.Dir(filepath.Dir(cmdDir))}
		}
		return []string{filepath.Dir(exe)}
	}
	if wd, err := os.Getwd(); err == nil {
		return []string{wd}
	}
	return []string{"."}
}

// verifyTaskSubmitter probes the actor lookup path to confirm the initialized
// runtime can reach the core worker. An absent probe actor is the expected
// outcome and counts as success.
func verifyTaskSubmitter() error {
	const probeActorName = "_ray_internal_job_submitter_probe_non_existent"
	_, err := api.GetPythonActorWithNamespace(probeActorName, SupervisorActorRayNamespace, supervisorModuleName, supervisorClassName)
	if err == nil {
		return nil
	}
	if isActorNotFoundError(err) {
		return nil
	}
	return fmt.Errorf("task submitter is not confirmed usable after runtime initialization: %w", err)
}

func isActorNotFoundError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

func (m *JobManager) getJobInfoClient() *JobInfoStorageClient {
	return m.jobInfoClient
}

// appendDriverLog appends one line to the driver log file of a submission.
func (m *JobManager) appendDriverLog(jobID, line string) {
	path := m.logClient.getLogFilePath(jobID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line + "\n")
}

// putStatus is the only path that writes a job status from this process. See
// the statusMu comment for why the write is serialized. It returns the status
// stored before the write (nil when the record is gone) so callers can
// reconcile a terminal transition that raced them. A missing record means the
// job was deleted while the caller was racing it: PutStatus would create a new
// record, so the write is dropped instead of resurrecting a deleted job.
func (m *JobManager) putStatus(ctx context.Context, jobID string, status JobStatus, message *string, driverExitCode *int32, errorType *JobErrorType) (*JobStatus, error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	existing, err := m.jobInfoClient.GetStatus(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		log.Log.V(4).Info("job record is gone; dropping status write", "job_id", jobID, "status", status)
		return nil, nil
	}
	return existing, m.jobInfoClient.PutStatus(ctx, jobID, status, message, driverExitCode, errorType)
}

// handleSupervisorFailure handles a ping failure of the supervisor. The status
// is re-read first: a stop that landed after the monitor loop's last status
// read makes the actor exit expected, not a job failure. Python performs the
// same read inside its exception handler. A status that turns terminal at
// write time (the stop's own status write won the race) is routed to the same
// expected-exit branch, and a deleted record suppresses every emission so the
// monitor does not recreate artifacts of a job the user deleted.
func (m *JobManager) handleSupervisorFailure(jobID string, pingErr error) {
	jobStatus, err := m.jobInfoClient.GetStatus(m.ctx, jobID)
	if err != nil {
		log.Log.Error(err, "failed to re-read job status after a supervisor ping failure")
	}
	if jobStatus != nil && jobStatus.IsTerminal() {
		m.eventLogger.submissionCompleted(jobID, *jobStatus, "")
		return
	}

	msg, et := classifySupervisorError(pingErr)
	previous, perr := m.putStatus(m.ctx, jobID, JobStatusFailed, msg, nil, &et)
	if previous != nil && previous.IsTerminal() {
		// A concurrent writer made the job terminal between the read above and
		// the status write (e.g. the stop path's supervisor wrote STOPPED).
		m.eventLogger.submissionCompleted(jobID, *previous, "")
		return
	}
	if perr != nil {
		log.Log.Error(perr, "failed to update job status")
		return
	}
	if previous == nil {
		// The record was deleted; do not recreate the driver log or emit an
		// event for a deleted job.
		return
	}

	log.Log.Error(pingErr, "job monitoring failed")
	var errorMessage string
	if msg != nil {
		errorMessage = *msg
	}
	// Write the error to the driver log file for easy access.
	if errorMessage != "" {
		m.appendDriverLog(jobID, errorMessage)
	}
	m.eventLogger.submissionCompleted(jobID, JobStatusFailed, errorMessage)
}

// recoverRunningJobs resumes monitoring every job that was not terminal when
// the agent last exited.
func (m *JobManager) recoverRunningJobs() {
	defer close(m.recoverDone)
	allJobs, err := m.jobInfoClient.GetAllJobs(m.ctx)
	if err != nil {
		log.Log.Error(err, "failed to recover running jobs")
		return
	}
	for jobID, info := range allJobs {
		if info != nil && !info.Status.IsTerminal() {
			go m.monitorJob(jobID, nil)
		}
	}
}

// applySchedulingStrategy pins the job driver to the head node unless the
// entrypoint specified resources (so the actor placement honors them) or the
// operator explicitly allowed drivers on worker nodes.
func (m *JobManager) applySchedulingStrategy(submissionID string, resourcesSpecified bool) *submitter.NodeAffinityOptions {
	if resourcesSpecified {
		return nil
	}
	if os.Getenv(rayJobAllowDriverOnWorkerNodesEnvVar) == "1" {
		log.Log.Info(rayJobAllowDriverOnWorkerNodesEnvVar + " was set to 1. Using default scheduling for the job driver instead of the head node.")
		return nil
	}
	headNodeID, err := m.jobInfoClient.GetHeadNodeID(m.ctx)
	if err != nil {
		log.Log.Error(err, "failed to fetch head node id; using default scheduling for the job driver")
		return nil
	}
	if headNodeID == "" {
		log.Log.Info("Head node ID not found in GCS. Using default scheduling strategy for the job driver.")
		return nil
	}
	log.Log.Info("Pinning job driver to the head node", "submission_id", submissionID, "head_node_id", headNodeID)
	return &submitter.NodeAffinityOptions{NodeID: headNodeID, Soft: false}
}

// getSupervisorRuntimeEnv builds the runtime env of a supervisor actor: the
// user runtime env plus the agent's niceness, device-visibility workarounds,
// and (optionally) redirection of the runtime env setup logs to the driver log.
// Any failure is returned to the caller: silently dropping the user runtime env
// would run the job without its pip packages, env vars or working dir.
func (m *JobManager) getSupervisorRuntimeEnv(userRuntimeEnv map[string]interface{}, submissionID string, resourcesSpecified bool) (string, error) {
	userEnvJSON := "{}"
	if userRuntimeEnv != nil {
		b, err := jsonMarshal(userRuntimeEnv)
		if err != nil {
			return "", fmt.Errorf("failed to serialize user runtime env: %w", err)
		}
		userEnvJSON = string(b)
	}
	env, err := api.NewRuntimeEnvFromJSON(userEnvJSON)
	if err != nil {
		return "", fmt.Errorf("failed to build supervisor runtime env: %w", err)
	}
	envVars, _ := userRuntimeEnv["env_vars"].(map[string]interface{})
	if envVars == nil {
		envVars = map[string]interface{}{}
	}
	envVars[rayWorkerNiceness] = "0"
	if !resourcesSpecified {
		envVars[noSetCudaVisibleDevicesEnvVar] = "1"
		envVars[noSetAscendRTVisibleDevicesEnvVar] = "1"
	}
	if err := env.Set("env_vars", envVars); err != nil {
		return "", fmt.Errorf("failed to set supervisor runtime env env_vars: %w", err)
	}

	if os.Getenv(rayStreamRuntimeEnvLogToJobDriverLogEnvVar) == "1" {
		config, _ := userRuntimeEnv["config"].(map[string]interface{})
		if config == nil {
			config = map[string]interface{}{}
		}
		config["log_files"] = []string{m.logClient.getLogFilePath(submissionID)}
		if err := env.Set("config", config); err != nil {
			return "", fmt.Errorf("failed to set supervisor runtime env config: %w", err)
		}
	}

	infoJSON, err := env.SerializeToRuntimeEnvInfo()
	if err != nil {
		return "", fmt.Errorf("failed to serialize supervisor runtime env: %w", err)
	}
	return infoJSON, nil
}

// submitJob records a pending job, creates its supervisor actor, and starts
// monitoring it. It returns the submission id even when the actor fails to
// start, because the failure is reported through the persisted job status.
func (m *JobManager) submitJob(ctx context.Context, req *JobSubmitRequest) (string, error) {
	entrypointNumCPUs := float64(0)
	if req.EntrypointNumCPUs != nil {
		entrypointNumCPUs = *req.EntrypointNumCPUs
	}
	entrypointNumGPUs := float64(0)
	if req.EntrypointNumGPUs != nil {
		entrypointNumGPUs = *req.EntrypointNumGPUs
	}
	entrypointMemory := int64(0)
	if req.EntrypointMemory != nil {
		entrypointMemory = int64(*req.EntrypointMemory)
	}
	submissionID := ""
	if req.SubmissionID != nil {
		submissionID = *req.SubmissionID
	} else if req.JobID != nil {
		submissionID = *req.JobID
	}
	if submissionID == "" {
		submissionID = generateJobID()
	}

	select {
	case <-m.recoverDone:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	log.Log.Info("starting job", "submission_id", submissionID)
	startTime := time.Now().UnixMilli()
	jobInfo := &JobInfo{
		Entrypoint:          req.Entrypoint,
		Status:              JobStatusPending,
		StartTime:           ptrInt64(startTime),
		Metadata:            req.Metadata,
		EntrypointNumCPUs:   ptrFloat64(entrypointNumCPUs),
		EntrypointNumGPUs:   ptrFloat64(entrypointNumGPUs),
		EntrypointMemory:    ptrUint64(uint64(entrypointMemory)),
		EntrypointResources: req.EntrypointResources,
	}
	var runtimeEnvJSONErr error
	if req.RuntimeEnv != nil {
		b, err := jsonMarshal(req.RuntimeEnv)
		if err != nil {
			runtimeEnvJSONErr = err
		} else {
			s := string(b)
			jobInfo.RuntimeEnvJSON = &s
		}
	}
	jobInfo.Message = defaultMessage(jobInfo, JobStatusPending)
	newKeyAdded, err := m.jobInfoClient.PutInfo(ctx, submissionID, jobInfo, false)
	if err != nil {
		return "", err
	}
	if !newKeyAdded {
		return "", &valueError{msg: fmt.Sprintf("job with submission_id %s already exists. Please use a different submission_id.", submissionID)}
	}

	m.appendDriverLog(submissionID, "Runtime env is setting up.")
	resourcesSpecified := (jobInfo.EntrypointNumCPUs != nil && *jobInfo.EntrypointNumCPUs > 0) ||
		(jobInfo.EntrypointNumGPUs != nil && *jobInfo.EntrypointNumGPUs > 0) ||
		(jobInfo.EntrypointMemory != nil && *jobInfo.EntrypointMemory > 0) ||
		len(jobInfo.EntrypointResources) > 0
	nodeAffinity := m.applySchedulingStrategy(submissionID, resourcesSpecified)

	m.eventLogger.submissionStarted(submissionID)

	if runtimeEnvJSONErr != nil {
		tb := fmt.Sprintf("failed to serialize runtime env for job %s: %v", submissionID, runtimeEnvJSONErr)
		log.Log.Error(fmt.Errorf("%s", tb), "failed to serialize job runtime env")
		msg := tb
		et := JobErrorTypeJobSupervisorActorStartFailure
		if _, perr := m.putStatus(ctx, submissionID, JobStatusFailed, &msg, nil, &et); perr != nil {
			log.Log.Error(perr, "failed to update job status to FAILED")
		}
		return submissionID, nil
	}

	if err := m.ensureRuntime(); err != nil {
		tb := fmt.Sprintf("failed to initialize Ray runtime for job %s: %v", submissionID, err)
		log.Log.Error(fmt.Errorf("%s", tb), "failed to initialize Ray runtime")
		msg := tb
		et := JobErrorTypeJobSupervisorActorStartFailure
		if _, perr := m.putStatus(ctx, submissionID, JobStatusFailed, &msg, nil, &et); perr != nil {
			log.Log.Error(perr, "failed to update job status to FAILED")
		}
		return submissionID, nil
	}

	supervisorRuntimeEnvJSON, err := m.getSupervisorRuntimeEnv(req.RuntimeEnv, submissionID, resourcesSpecified)
	if err != nil {
		tb := fmt.Sprintf("failed to build runtime env for job %s: %v", submissionID, err)
		log.Log.Error(fmt.Errorf("%s", tb), "failed to build supervisor runtime env")
		msg := tb
		et := JobErrorTypeJobSupervisorActorStartFailure
		if _, perr := m.putStatus(ctx, submissionID, JobStatusFailed, &msg, nil, &et); perr != nil {
			log.Log.Error(perr, "failed to update job status to FAILED")
		}
		return submissionID, nil
	}
	supervisorOpts := supervisorCreateOptions{
		name:           jobActorName(submissionID),
		numCPUs:        entrypointNumCPUs,
		numGPUs:        entrypointNumGPUs,
		memory:         entrypointMemory,
		resources:      req.EntrypointResources,
		runtimeEnvJSON: supervisorRuntimeEnvJSON,
		nodeAffinity:   nodeAffinity,
	}
	// The python JobSupervisor in this tree takes (job_id, entrypoint,
	// user_metadata, logs_dir): it reads the job info — including the runtime
	// env — back from the KV through the hosting worker's GCS client, so no
	// gcs address or cluster id is passed. The reference implementation's
	// python supervisor carries two extra constructor parameters for that
	// purpose; this tree's does not.
	constructorArgs := []interface{}{
		submissionID,
		req.Entrypoint,
		supervisorMetadata(req.Metadata),
		m.logsDir,
	}
	supervisor, err := createSupervisorActor(supervisorOpts, constructorArgs...)
	if err != nil {
		tb := fmt.Sprintf("failed to start supervisor actor for job %s: %v", submissionID, err)
		log.Log.Error(fmt.Errorf("%s", tb), "failed to start supervisor actor")
		msg := tb
		et := JobErrorTypeJobSupervisorActorStartFailure
		if _, perr := m.putStatus(ctx, submissionID, JobStatusFailed, &msg, nil, &et); perr != nil {
			log.Log.Error(perr, "failed to update job status to FAILED")
		}
		return submissionID, nil
	}
	if err := supervisor.run(resourcesSpecified); err != nil {
		log.Log.Error(err, "failed to start supervisor run")
	}
	go m.monitorJob(submissionID, supervisor)
	return submissionID, nil
}

// supervisorMetadata defaults a nil user metadata map to an empty one, matching
// the Python JobSupervisor constructor's metadata handling.
func supervisorMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return metadata
}

// monitorJob monitors a job until it reaches a terminal state, guarding against
// a second monitor for the same submission.
func (m *JobManager) monitorJob(jobID string, supervisor *supervisorActor) {
	if _, loaded := m.monitoredJobs.LoadOrStore(jobID, struct{}{}); loaded {
		log.Log.V(4).Info("job is already being monitored", "job_id", jobID)
		return
	}
	defer m.monitoredJobs.Delete(jobID)
	if err := m.monitorJobInternal(jobID, supervisor); err != nil {
		log.Log.Error(err, "unhandled exception in job monitoring")
	}
}

// monitorJobInternal implements the monitor loop: a pending job that never
// starts times out, and a supervisor that cannot be reached fails the job with
// the classified error appended to the driver log for easy access.
func (m *JobManager) monitorJobInternal(jobID string, supervisor *supervisorActor) error {
	timeout := jobStartTimeoutSeconds()

	var jobInfo *JobInfo

	for {
		select {
		case <-m.ctx.Done():
			return nil
		case <-time.After(jobMonitorLoopPeriodS):
		}

		jobStatus, err := m.jobInfoClient.GetStatus(m.ctx, jobID)
		if err != nil {
			continue
		}
		if jobStatus != nil && *jobStatus == JobStatusPending {
			if jobInfo == nil {
				jobInfo, err = m.jobInfoClient.GetInfo(m.ctx, jobID)
				if err != nil {
					continue
				}
			}
			var startMs int64
			if jobInfo != nil && jobInfo.StartTime != nil {
				startMs = *jobInfo.StartTime
			}
			if float64(time.Now().UnixMilli()-startMs)/1000.0 > timeout {
				errMsg := fmt.Sprintf("Job supervisor actor failed to start within %v seconds. This timeout can be configured by setting the environment variable %s.", timeout, rayJobStartTimeoutSecondsEnvVar)
				resourcesSpecified := jobInfo != nil &&
					((jobInfo.EntrypointNumCPUs != nil && *jobInfo.EntrypointNumCPUs > 0) ||
						(jobInfo.EntrypointNumGPUs != nil && *jobInfo.EntrypointNumGPUs > 0) ||
						(jobInfo.EntrypointMemory != nil && *jobInfo.EntrypointMemory > 0) ||
						len(jobInfo.EntrypointResources) > 0)
				if resourcesSpecified {
					errMsg += " This may be because the job entrypoint's specified resources (entrypoint_num_cpus, entrypoint_num_gpus, entrypoint_resources, entrypoint_memory) aren't available on the cluster. Try checking the cluster's available resources with `ray status` and specifying fewer resources for the job entrypoint."
				}
				et := JobErrorTypeJobSupervisorActorStartTimeout
				if _, perr := m.putStatus(m.ctx, jobID, JobStatusFailed, &errMsg, nil, &et); perr != nil {
					log.Log.Error(perr, "failed to update job status")
				}
				log.Log.Error(fmt.Errorf("%s", errMsg), "job supervisor start timeout")
				m.appendDriverLog(jobID, errMsg)
				break
			}
		}

		if supervisor == nil {
			if err := m.ensureRuntime(); err == nil {
				supervisor, err = getSupervisorActorForJob(jobID)
				if err != nil {
					supervisor = nil
				}
			}
		}
		if supervisor == nil {
			if jobStatus != nil && *jobStatus == JobStatusPending {
				continue
			}
			msg := "Unexpected error occurred: failed to get job supervisor."
			et := JobErrorTypeJobSupervisorActorStartFailure
			if _, perr := m.putStatus(m.ctx, jobID, JobStatusFailed, &msg, nil, &et); perr != nil {
				log.Log.Error(perr, "failed to update job status")
			}
			break
		}

		if err := supervisor.ping(); err != nil {
			m.handleSupervisorFailure(jobID, err)
			break
		}
	}

	if supervisor == nil {
		if err := m.ensureRuntime(); err == nil {
			var err error
			supervisor, err = getSupervisorActorForJob(jobID)
			if err != nil {
				supervisor = nil
			}
		}
	}
	if supervisor != nil {
		if err := supervisor.kill(); err != nil {
			log.Log.V(4).Info("failed to kill supervisor actor", "job_id", jobID, "err", err)
		}
	}
	return nil
}

// classifySupervisorError maps a monitor failure to the persisted error message
// and error type, aligned with the Python job manager's exception branches.
func classifySupervisorError(err error) (*string, JobErrorType) {
	var exc object.RayException
	if errors.As(err, &exc) {
		switch exc.ErrorCode() {
		case rpcErrorTypeRuntimeEnvSetupFailed:
			msg := fmt.Sprintf("runtime_env setup failed: %v", err)
			return &msg, JobErrorTypeRuntimeEnvSetupFailure
		case rpcErrorTypeActorUnschedulable:
			msg := fmt.Sprintf("Job supervisor actor could not be scheduled: %v", err)
			return &msg, JobErrorTypeJobSupervisorActorUnschedulable
		case object.RpcErrorTypeActorDied, object.ErrorCodeActorDied:
			msg := fmt.Sprintf("Job supervisor actor died: %v", err)
			return &msg, JobErrorTypeJobSupervisorActorDied
		}
	}
	switch err.(type) {
	case *runtimeEnvSetupError:
		msg := fmt.Sprintf("runtime_env setup failed: %v", err)
		return &msg, JobErrorTypeRuntimeEnvSetupFailure
	case *actorUnschedulableError:
		msg := fmt.Sprintf("Job supervisor actor could not be scheduled: %v", err)
		return &msg, JobErrorTypeJobSupervisorActorUnschedulable
	case *actorDiedError:
		msg := fmt.Sprintf("Job supervisor actor died: %v", err)
		return &msg, JobErrorTypeJobSupervisorActorDied
	}
	switch {
	case containsAny(err.Error(), "RuntimeEnvSetupError", "runtime_env setup failed"):
		msg := fmt.Sprintf("runtime_env setup failed: %v", err)
		return &msg, JobErrorTypeRuntimeEnvSetupFailure
	case containsAny(err.Error(), "ActorUnschedulableError", "could not be scheduled"):
		msg := fmt.Sprintf("Job supervisor actor could not be scheduled: %v", err)
		return &msg, JobErrorTypeJobSupervisorActorUnschedulable
	case containsAny(err.Error(), "ActorDiedError", "actor died"):
		msg := fmt.Sprintf("Job supervisor actor died: %v", err)
		return &msg, JobErrorTypeJobSupervisorActorDied
	default:
		msg := fmt.Sprintf("Unexpected error occurred: %v", err)
		return &msg, JobErrorTypeJobSupervisorActorUnknownFailure
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// jobStartTimeoutSeconds returns the pending-start timeout, defaulting to
// defaultJobStartTimeoutSeconds when the environment variable is unset or
// unparsable.
func jobStartTimeoutSeconds() float64 {
	if v := os.Getenv(rayJobStartTimeoutSecondsEnvVar); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return defaultJobStartTimeoutSeconds
}

// stopJob stops the supervisor of a job by submitting the supervisor's stop
// method. The submission returns as soon as the task is enqueued, so the
// mutex covers the enqueue only: the STOPPED status is written later by the
// Python supervisor itself, outside this lock, and the monitor's re-read of
// the status is what keeps a failure classification from overwriting it. The
// Python agent takes no lock at all here.
func (m *JobManager) stopJob(ctx context.Context, jobID string) (bool, error) {
	if err := m.ensureRuntime(); err != nil {
		return false, err
	}
	supervisor, err := getSupervisorActorForJob(jobID)
	if err != nil {
		return false, nil
	}
	if supervisor != nil {
		m.statusMu.Lock()
		err := supervisor.stop()
		m.statusMu.Unlock()
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// deleteJob removes a job whose status is terminal. The mutex keeps a monitor
// status write from recreating the record between the check and the delete.
func (m *JobManager) deleteJob(ctx context.Context, jobID string) (bool, error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	jobStatus, err := m.jobInfoClient.GetStatus(ctx, jobID)
	if err != nil {
		return false, err
	}
	if jobStatus == nil || !jobStatus.IsTerminal() {
		return false, fmt.Errorf("attempted to delete job '%s', but it is in a non-terminal state %v", jobID, jobStatus)
	}
	if err := m.jobInfoClient.DeleteInfo(ctx, jobID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *JobManager) getJobStatus(ctx context.Context, jobID string) (*JobStatus, error) {
	return m.jobInfoClient.GetStatus(ctx, jobID)
}

func (m *JobManager) getJobInfo(ctx context.Context, jobID string) (*JobInfo, error) {
	return m.jobInfoClient.GetInfo(ctx, jobID)
}

func (m *JobManager) listJobs(ctx context.Context) (map[string]*JobInfo, error) {
	return m.jobInfoClient.GetAllJobs(ctx)
}

func (m *JobManager) getJobLogs(jobID string) string {
	return m.logClient.getLogs(jobID)
}

// tailJobLogs streams the driver log of a job. It only returns once the job is
// terminal and the tail iterator stops producing lines.
func (m *JobManager) tailJobLogs(ctx context.Context, jobID string) (<-chan string, error) {
	status, err := m.jobInfoClient.GetStatus(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if status == nil {
		return nil, fmt.Errorf("job '%s' does not exist.", jobID)
	}

	out := make(chan string)
	go func() {
		defer close(out)
		linesCh, errCh := m.logClient.tailLogs(ctx, jobID)
		jobFinished := false
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if err != nil {
					return
				}
			case lines, ok := <-linesCh:
				if !ok {
					return
				}
				if lines == nil {
					if jobFinished {
						return
					}
					status, err := m.jobInfoClient.GetStatus(ctx, jobID)
					if err == nil && status != nil && status.IsTerminal() {
						jobFinished = true
					}
				} else {
					select {
					case out <- joinLines(lines):
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

// joinLines concatenates a chunk of log lines into one payload, preserving the
// original line endings.
func joinLines(lines []string) string {
	var b []byte
	for _, l := range lines {
		b = append(b, l...)
	}
	return string(b)
}
