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
	"fmt"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

const (
	supervisorModuleName = "ray.dashboard.modules.job.job_supervisor"
	supervisorClassName  = "JobSupervisor"
)

// RPC error codes surfaced by the core worker when an actor cannot start,
// aligned with the Python job manager's error classification.
const (
	rpcErrorTypeRuntimeEnvSetupFailed = 7
	rpcErrorTypeActorUnschedulable    = 19
)

// supervisorActor is the Go-side handle of a Python JobSupervisor actor.
type supervisorActor struct {
	handle *api.PythonActorHandle
}

// getSupervisorActorForJob resolves the named supervisor actor of a job.
func getSupervisorActorForJob(jobID string) (*supervisorActor, error) {
	handle, err := api.GetPythonActorWithNamespace(
		jobActorName(jobID),
		SupervisorActorRayNamespace,
		supervisorModuleName,
		supervisorClassName,
	)
	if err != nil {
		return nil, err
	}
	return &supervisorActor{handle: handle}, nil
}

// supervisorCreateOptions carries the actor creation options of a supervisor.
type supervisorCreateOptions struct {
	name           string
	numCPUs        float64
	numGPUs        float64
	memory         int64
	resources      map[string]float64
	runtimeEnvJSON string
	nodeAffinity   *submitter.NodeAffinityOptions
}

// createSupervisorActor creates the detached, async Python JobSupervisor actor
// of a job. The actor must be async so its long-running run method does not
// block ping/stop calls.
func createSupervisorActor(opts supervisorCreateOptions, constructorArgs ...interface{}) (*supervisorActor, error) {
	creator := api.RemotePythonActor(supervisorModuleName, supervisorClassName).
		WithName(opts.name).
		WithNamespace(SupervisorActorRayNamespace).
		WithLifetime(submitter.ActorLifetimeDetached).
		WithAsync(true).
		WithResources(buildActorResources(opts.numCPUs, opts.numGPUs, opts.memory, opts.resources))
	if opts.runtimeEnvJSON != "" {
		creator = creator.WithRuntimeEnv(opts.runtimeEnvJSON)
	}
	if opts.nodeAffinity != nil {
		creator = creator.WithNodeAffinity(opts.nodeAffinity.NodeID, opts.nodeAffinity.Soft)
	}
	handle, err := creator.Create(constructorArgs...)
	if err != nil {
		log.Log.Error(err, "createSupervisorActor failed", "name", opts.name)
		return nil, err
	}
	log.Log.Info("createSupervisorActor succeeded", "name", opts.name, "isAsync", true)
	return &supervisorActor{handle: handle}, nil
}

// buildActorResources builds the resource map the JobSupervisor actor requests:
// the CPU request, plus the entrypoint's GPU, memory and custom resource
// requests when they are positive. A custom entrypoint resource named CPU, GPU
// or memory is merged last and therefore wins over the dedicated field, which
// is the pre-existing behaviour; the reference implementation rejects those
// names instead.
//
// The CPU key is always present, including at zero. These are the job's own
// scheduling demands, and the reference implementation forwards them
// unconditionally: a missing request becomes an explicit zero, which selects the
// specified-resources branch of the actor options and charges no CPU per method
// call. Dropping the zero instead leaves an empty map, and an empty map asks the
// actor bridge for its defaults — a placement requirement of one CPU, which a
// supervisor pinned to a head node with a single CPU cannot always satisfy.
func buildActorResources(numCPUs, numGPUs float64, memory int64, resources map[string]float64) map[string]float64 {
	// A negative request cannot be satisfied; clamp it to the documented
	// default of zero rather than forwarding it to the scheduler.
	if numCPUs < 0 {
		numCPUs = 0
	}
	out := map[string]float64{"CPU": numCPUs}
	if numGPUs > 0 {
		out["GPU"] = numGPUs
	}
	if memory > 0 {
		out["memory"] = float64(memory)
	}
	for k, v := range resources {
		out[k] = v
	}
	return out
}

// run starts the job entrypoint inside the supervisor. It returns as soon as
// the async actor accepted the call.
func (a *supervisorActor) run(resourcesSpecified bool) error {
	_, err := api.ActorTask[struct{}](a.handle, "run", nil, resourcesSpecified).Remote()
	return err
}

// ping checks that the supervisor is alive and reachable. The call retries
// indefinitely because a supervisor may still be starting.
func (a *supervisorActor) ping() error {
	actorID := a.handle.ID().Hex()
	ref, err := api.ActorTask[struct{}](a.handle, "ping").WithMaxRetries(-1).Remote()
	if err != nil {
		log.Log.V(1).Info("supervisor.ping remote failed", "actor_id", actorID, "err", err)
		return err
	}
	_, err = api.Get(ref)
	if err != nil {
		log.Log.V(1).Info("supervisor.ping get failed", "actor_id", actorID, "err", err)
	}
	return err
}

// stop asks the supervisor to stop the job and exit.
func (a *supervisorActor) stop() error {
	_, err := api.ActorTask[struct{}](a.handle, "stop").Remote()
	return err
}

// kill force-kills the supervisor actor without restart, matching the Python
// defensive ray.kill(no_restart=True). force is a no-op when noRestart is set
// (the GCS destroy path always force-kills), so it stays false to state the
// Python intent exactly.
func (a *supervisorActor) kill() error {
	return api.KillActor(a.handle, false, true)
}

// actorError is the message-only base of the supervisor error kinds.
type actorError struct {
	msg string
}

func (e *actorError) Error() string { return e.msg }

type runtimeEnvSetupError struct{ actorError }

type actorUnschedulableError struct{ actorError }

type actorDiedError struct{ actorError }

// newActorError builds one of the typed supervisor errors, used when the
// failure is detected outside the RPC exception path.
func newActorError(kind string, err error) error {
	msg := fmt.Sprintf("%s: %v", kind, err)
	switch kind {
	case "runtime_env_setup":
		return &runtimeEnvSetupError{actorError{msg}}
	case "unschedulable":
		return &actorUnschedulableError{actorError{msg}}
	case "died":
		return &actorDiedError{actorError{msg}}
	default:
		return &actorError{msg}
	}
}
