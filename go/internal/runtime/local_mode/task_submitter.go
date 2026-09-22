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

package local_mode

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// LocalModeTaskSubmitter implements TaskSubmitter for local mode.
// Inspired by Java's LocalModeTaskSubmitter.
//
// Design notes:
// 1. Submits tasks to goroutine pool for execution
// 2. Handles task dependencies (waits for objects to be ready)
// 3. Supports actor creation tasks and actor tasks
// 4. Uses ActorConcurrencyGroupManager for actor task scheduling
type LocalModeTaskSubmitter struct {
	objectStore              *objectstore.LocalModeObjectStore
	workerContext            *LocalModeWorkerContext
	taskExecutor             *LocalModeTaskExecutor
	functionMgr              *function.FunctionManager
	actorConcurrencyGroupMgr *ActorConcurrencyGroupManager

	// lastSyncedVersion is the function registry version at the last
	// FunctionManager sync. Submission re-synchronizes only when the registry
	// version advances (a new function was registered), avoiding a full
	// re-registration on every submission. Atomic because submissions may come
	// from multiple goroutines.
	lastSyncedVersion atomic.Uint64

	// waitingTasks maps object IDs to tasks waiting for them
	waitingTasks      sync.Map // map[ids.ObjectID][]*taskSpec
	taskAndObjectLock sync.Mutex

	// namedActors stores named actors
	namedActors sync.Map // map[string]*namedActorInfo

	// actorMaxConcurrency tracks max concurrency for actors
	actorMaxConcurrency sync.Map // map[ids.ActorID]int
}

// namedActorInfo holds information about a named actor
type namedActorInfo struct {
	actorID ids.ActorID
	handle  interface{} // Could be ActorHandle or similar
}

// taskSpec holds task specification for local mode
type taskSpec struct {
	taskType           base.TaskType
	functionDescriptor function.FunctionDescriptor
	args               []function.FunctionArg
	numReturns         int
	actorID            ids.ActorID
	taskID             ids.TaskID
	jobID              ids.JobID
}

// NewLocalModeTaskSubmitter creates a new LocalModeTaskSubmitter.
func NewLocalModeTaskSubmitter(
	objectStore *objectstore.LocalModeObjectStore,
	workerContext *LocalModeWorkerContext,
	taskExecutor *LocalModeTaskExecutor,
	functionMgr *function.FunctionManager,
) *LocalModeTaskSubmitter {
	return &LocalModeTaskSubmitter{
		objectStore:              objectStore,
		workerContext:            workerContext,
		taskExecutor:             taskExecutor,
		functionMgr:              functionMgr,
		actorConcurrencyGroupMgr: NewActorConcurrencyGroupManager(),
	}
}

// SubmitTask submits a normal task to be executed.
func (s *LocalModeTaskSubmitter) SubmitTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	jobID := s.workerContext.GetCurrentJobID()
	parentTaskID := ids.NilTaskID()
	taskID := ids.TaskIDForNormalTask(jobID, parentTaskID, rand.Uint64())

	spec := &taskSpec{
		taskType:           base.TaskTypeNormal,
		functionDescriptor: functionDescriptor,
		args:               args,
		numReturns:         numReturns,
		taskID:             taskID,
		jobID:              jobID,
	}

	returnIds := s.getReturnIds(taskID, numReturns)
	s.submitTaskSpec(spec)

	return returnIds, nil
}

// CreateActor creates a new actor.
func (s *LocalModeTaskSubmitter) CreateActor(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	options *submitter.ActorCreationOptions,
) (ids.ActorID, error) {
	if options == nil {
		options = &submitter.ActorCreationOptions{}
	}

	jobID := s.workerContext.GetCurrentJobID()
	parentTaskID := ids.NilTaskID()
	actorID := ids.OfActorID(jobID, parentTaskID, rand.Uint64())
	maxConcurrency := options.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	// Create actor creation task spec
	taskID := ids.TaskIDForActorCreationTask(actorID)

	spec := &taskSpec{
		taskType:           base.TaskTypeActorCreation,
		functionDescriptor: functionDescriptor,
		args:               args,
		numReturns:         1,
		actorID:            actorID,
		taskID:             taskID,
		jobID:              jobID,
	}

	// Register actor with concurrency group manager
	s.actorConcurrencyGroupMgr.GetOrCreateGroup(actorID, maxConcurrency)
	s.actorMaxConcurrency.Store(actorID, maxConcurrency)

	// Submit actor creation task
	s.submitTaskSpec(spec)

	// Store named actor if name is provided (using namespace:name format)
	if options.Name != "" {
		key := options.Name
		if options.Namespace != "" {
			key = options.Namespace + ":" + options.Name
		}
		s.namedActors.Store(key, &namedActorInfo{
			actorID: actorID,
		})
	}

	return actorID, nil
}

// SubmitActorTask submits a task to be executed by an actor.
func (s *LocalModeTaskSubmitter) SubmitActorTask(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	jobID := s.workerContext.GetCurrentJobID()
	parentTaskID := ids.NilTaskID()
	taskID := ids.TaskIDForActorTask(jobID, parentTaskID, rand.Uint64(), actorID)

	spec := &taskSpec{
		taskType:           base.TaskTypeActorTask,
		functionDescriptor: functionDescriptor,
		args:               args,
		numReturns:         numReturns,
		actorID:            actorID,
		taskID:             taskID,
		jobID:              jobID,
	}

	returnIds := s.getReturnIds(taskID, numReturns)
	s.submitTaskSpec(spec)

	return returnIds, nil
}

// GetActor retrieves a named actor by its name and namespace.
func (s *LocalModeTaskSubmitter) GetActor(name string, namespace string) (submitter.ActorHandle, error) {
	// Build key with namespace:name format (consistent with Java's naming convention)
	key := name
	if namespace != "" {
		key = namespace + ":" + name
	}

	if info, ok := s.namedActors.Load(key); ok {
		actorInfo := info.(*namedActorInfo)
		// Return a NativeActorHandle as the ActorHandle implementation
		return &object.NativeActorHandle{
			ActorID: actorInfo.actorID,
		}, nil
	}
	return nil, fmt.Errorf("actor not found: name=%s, namespace=%s", name, namespace)
}

// KillActor kills an actor from the driver side.
//
// Local mode has no C++ CoreWorker or GCS, so there is no automatic restart:
// the kill is always final (equivalent to Java's noRestart=true semantics).
// The actor's concurrency groups and task context are torn down, and any
// named-actor registration is removed, so later submissions and GetActor
// lookups report the actor as unavailable. The noRestart parameter is accepted
// for API compatibility but has no effect (Java's local mode, RayDevRuntime,
// does not support kill at all).
func (s *LocalModeTaskSubmitter) KillActor(actorID ids.ActorID, noRestart bool) error {
	s.removeActorState(actorID)
	log.Log.Info("local-mode actor killed",
		"actorID", actorID.Hex(), "noRestart", noRestart)
	return nil
}

// removeActorState tears down all local-mode state for an actor so later
// submissions and GetActor lookups report it as unavailable: the actor's
// concurrency groups, task context, and any named-actor registration.
func (s *LocalModeTaskSubmitter) removeActorState(actorID ids.ActorID) {
	s.actorConcurrencyGroupMgr.RemoveGroup(actorID)
	s.taskExecutor.RemoveActorContext(actorID)
	// Remove any named-actor registration for this actor so GetActor no longer
	// resolves it (matching Java: a killed/exited named actor is not resolvable).
	s.namedActors.Range(func(key, value interface{}) bool {
		if info, ok := value.(*namedActorInfo); ok && info.actorID == actorID {
			s.namedActors.Delete(key)
		}
		return true
	})
}

// submitTaskSpec submits a task specification for execution.
func (s *LocalModeTaskSubmitter) submitTaskSpec(spec *taskSpec) {
	s.syncFunctionsFromRegistry()

	// Check if all dependencies are ready and, if so, mark the task to be
	// executed outside the lock. executeTaskSpec puts return objects into the
	// object store, which triggers the onObjectPut callback (checkWaitingTasks)
	// that re-acquires taskAndObjectLock; holding the lock during execution
	// would deadlock since sync.Mutex is non-reentrant.
	s.taskAndObjectLock.Lock()
	unreadyObjects := s.getUnreadyObjects(spec)
	executeNow := len(unreadyObjects) == 0
	if !executeNow {
		// Some dependencies not ready - add to waiting list
		for _, oid := range unreadyObjects {
			key := oid
			if tasks, ok := s.waitingTasks.Load(key); ok {
				tasks = append(tasks.([]*taskSpec), spec)
				s.waitingTasks.Store(key, tasks)
			} else {
				s.waitingTasks.Store(key, []*taskSpec{spec})
			}
		}
	}
	s.taskAndObjectLock.Unlock()

	if executeNow {
		s.executeTaskSpec(spec)
	}
}

// executeTaskSpec executes a task specification.
func (s *LocalModeTaskSubmitter) executeTaskSpec(spec *taskSpec) {
	// Set up worker context
	workerID := ids.NewUniqueID()
	s.workerContext.SetCurrentWorkerId(workerID)
	s.workerContext.SetCurrentTaskId(spec.taskID)
	s.workerContext.SetCurrentTaskType(spec.taskType)

	if spec.taskType == base.TaskTypeActorTask {
		s.workerContext.SetCurrentActorId(spec.actorID)
	} else {
		s.workerContext.SetCurrentActorId(ids.NilActorID())
	}

	// Execute based on task type
	var returnObjects []function.SerializedObject
	var err error

	switch spec.taskType {
	case base.TaskTypeActorCreation:
		// Execute actor creation
		returnObjects, err = s.taskExecutor.Execute(spec.functionDescriptor, spec.args, spec.numReturns)
		if err != nil {
			// Handle actor creation error
			return
		}

		// Register actor context
		actorContext := NewLocalActorContext(workerID)
		s.taskExecutor.RegisterActorContext(spec.actorID, actorContext)

		// Put dummy object to signal actor creation completion
		creationTaskID := ids.TaskIDForActorCreationTask(spec.actorID)
		dummyOID := ids.ObjectIDFromIndex(creationTaskID, 1)
		s.objectStore.PutRawWithID(&object.NativeRayObject{
			Data: []byte{1},
		}, &dummyOID)

	case base.TaskTypeActorTask:
		// Execute actor task through concurrency group
		returnObjects, err = s.taskExecutor.ExecuteActorTask(spec.actorID, spec.functionDescriptor, spec.args, spec.numReturns)

	case base.TaskTypeNormal:
		// Execute normal task
		returnObjects, err = s.taskExecutor.Execute(spec.functionDescriptor, spec.args, spec.numReturns)
	}

	// Put return objects into object store
	returnIds := s.getReturnIds(spec.taskID, spec.numReturns)

	if err != nil {
		// Surface the failure as error objects for every return ID instead of
		// silently dropping it. Silently returning here meant a task that failed
		// to execute (e.g. an unsupported pass-by-reference argument) never put
		// its return objects, so a driver blocking on ObjectRef.Get() waited
		// forever in waitForObjects. Putting error objects lets the driver's Get
		// surface the failure (ErrorObjectFromNative) instead of deadlocking.
		//
		// An intentional actor exit (api.ExitActor) is not a task failure and is
		// excluded, matching the cluster-mode worker's IsActorExitError check.
		var actorExit *rayerrors.ActorExitError
		if !errors.As(err, &actorExit) {
			log.Log.Error(err, "local-mode task failed", "taskID", spec.taskID.Hex(),
				"taskType", int32(spec.taskType))
			s.putErrorObjects(spec, err)
		}
		s.checkWaitingTasks()
		return
	}

	for i, returnObj := range returnObjects {
		if i < len(returnIds) {
			obj := &object.NativeRayObject{
				Data:     returnObj.Data,
				Metadata: returnObj.Metadata,
			}
			s.objectStore.PutRawWithID(obj, &returnIds[i])
		}
	}

	// Put dummy objects for remaining returns (for actor tasks)
	for i := len(returnObjects); i < spec.numReturns; i++ {
		if i < len(returnIds) {
			dummyObj := &object.NativeRayObject{
				Data: []byte{1},
			}
			s.objectStore.PutRawWithID(dummyObj, &returnIds[i])
		}
	}

	// Check waiting tasks and submit any that are now ready
	s.checkWaitingTasks()
}

// getUnreadyObjects returns the set of object IDs that are not yet ready.
func (s *LocalModeTaskSubmitter) getUnreadyObjects(spec *taskSpec) []ids.ObjectID {
	unreadyObjects := make([]ids.ObjectID, 0)

	// Check task arguments
	for _, arg := range spec.args {
		if arg.ObjectRef != nil && !s.objectStore.IsObjectReady(arg.ObjectRef.ObjectID) {
			unreadyObjects = append(unreadyObjects, arg.ObjectRef.ObjectID)
		}
	}

	// For actor tasks, check if actor is created
	if spec.taskType == base.TaskTypeActorTask {
		creationTaskID := ids.TaskIDForActorCreationTask(spec.actorID)
		dummyOID := ids.ObjectIDFromIndex(creationTaskID, 1)
		if !s.objectStore.IsObjectReady(dummyOID) {
			unreadyObjects = append(unreadyObjects, dummyOID)
		}
	}

	return unreadyObjects
}

// checkWaitingTasks checks waiting tasks and submits any that are now ready.
//
// executeTaskSpec puts return objects into the object store, which triggers
// the onObjectPut callback (checkWaitingTasks) that re-acquires
// taskAndObjectLock; holding the lock during execution would deadlock since
// sync.Mutex is non-reentrant. Tasks are collected under the lock and executed
// after it is released.
func (s *LocalModeTaskSubmitter) checkWaitingTasks() {
	s.taskAndObjectLock.Lock()

	tasksToExecute := make([]*taskSpec, 0)

	s.waitingTasks.Range(func(key, value interface{}) bool {
		oid := key.(ids.ObjectID)
		tasks := value.([]*taskSpec)

		if s.objectStore.IsObjectReady(oid) {
			// Object is ready - check if tasks can be executed
			for _, task := range tasks {
				unreadyObjects := s.getUnreadyObjects(task)
				if len(unreadyObjects) == 0 {
					tasksToExecute = append(tasksToExecute, task)
				}
			}
			// Remove this object from waiting tasks
			s.waitingTasks.Delete(key)
		}
		return true
	})

	s.taskAndObjectLock.Unlock()

	// Execute ready tasks outside the lock.
	for _, task := range tasksToExecute {
		s.executeTaskSpec(task)
	}
}

// getReturnIds generates return object IDs for a task.
func (s *LocalModeTaskSubmitter) getReturnIds(taskID ids.TaskID, numReturns int) []ids.ObjectID {
	returnIds := make([]ids.ObjectID, numReturns)
	for i := 0; i < numReturns; i++ {
		returnIds[i] = ids.ObjectIDFromIndex(taskID, uint32(i+1))
	}
	return returnIds
}

// putErrorObjects puts a task execution exception error object for every return
// ID of the failed task. This mirrors the cluster-mode worker, which returns an
// error object for each expected return value when task execution fails, so a
// driver Get()ing the result surfaces the failure (via ErrorObjectFromNative)
// instead of blocking forever on a return object that never appears.
func (s *LocalModeTaskSubmitter) putErrorObjects(spec *taskSpec, err error) {
	exc := object.NewRayTaskExecutionException(spec.taskID.Hex(), err, "")
	data, serErr := (&object.RayExceptionSerializer{}).ToBytes(exc)
	if serErr != nil {
		log.Log.Error(serErr, "failed to serialize task execution error", "taskID", spec.taskID.Hex())
		// Fall back to a raw error object so the driver still receives a failure.
		data = []byte(fmt.Sprintf("task %s failed: %v", spec.taskID.Hex(), err))
	}
	returnIds := s.getReturnIds(spec.taskID, spec.numReturns)
	for i := range returnIds {
		if err := s.objectStore.PutRawWithID(&object.NativeRayObject{
			Data:     data,
			Metadata: []byte(object.MetadataTypeTaskExecutionException),
		}, &returnIds[i]); err != nil {
			log.Log.Error(err, "failed to put error object for failed task",
				"taskID", spec.taskID.Hex())
		}
	}
}

// onObjectPut is called when an object is put into the object store.
// This is used to trigger waiting tasks.
func (s *LocalModeTaskSubmitter) onObjectPut(oid ids.ObjectID) {
	s.checkWaitingTasks()
}

// Shutdown shuts down the task submitter.
func (s *LocalModeTaskSubmitter) Shutdown() {
	s.actorConcurrencyGroupMgr.Shutdown()
}

// Compile-time check to ensure LocalModeTaskSubmitter implements TaskSubmitter
var _ submitter.TaskSubmitter = (*LocalModeTaskSubmitter)(nil)

// Compile-time check that LocalModeTaskSubmitter satisfies the structural
// actorKiller capability probed by api.KillActor. Without it, a signature drift
// would silently turn the public API back into a kill_actor_not_supported error.
var _ interface {
	KillActor(ids.ActorID, bool) error
} = (*LocalModeTaskSubmitter)(nil)
