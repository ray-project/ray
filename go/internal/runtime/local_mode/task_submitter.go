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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/internal/runtime/localstore"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
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
	objectStore              *localstore.LocalModeObjectStore
	workerContext            *LocalModeWorkerContext
	taskExecutor             *LocalModeTaskExecutor
	functionMgr              *function.FunctionManager
	actorConcurrencyGroupMgr *ActorConcurrencyGroupManager

	// lastSyncedVersion is the function registry version at the last
	// FunctionManager sync. SubmitTask re-synchronizes only when the registry
	// version advances (a new function was registered), avoiding per-submission
	// full re-registration. Atomic because SubmitTask may be called from
	// multiple goroutines.
	lastSyncedVersion atomic.Uint64

	// syncMu serializes the FunctionManager re-sync so that concurrent
	// Submits that observe a stale version do not each run the full (idempotent
	// but wasteful) re-registration; only the first observer syncs.
	syncMu sync.Mutex

	// waitingTasks maps object IDs to tasks waiting for them
	waitingTasks      sync.Map // map[ids.ObjectID][]*taskSpec
	taskAndObjectLock sync.Mutex

	// namedActors stores named actors
	namedActors sync.Map // map[string]*namedActorInfo

	// placementGroups simulates placement group creation in-process. Each
	// created group is stored under its id and considered ready immediately.
	// Reads and writes are serialized through placementGroupMu.
	placementGroups  map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions
	placementGroupMu sync.Mutex
}

// namedActorInfo holds information about a named actor
type namedActorInfo struct {
	actorID ids.ActorID
	handle  interface{} // Could be ActorHandle or similar
}

// taskSpec holds task specification for local mode
type taskSpec struct {
	taskType             base.TaskType
	functionDescriptor   function.FunctionDescriptor
	args                 []function.FunctionArg
	numReturns           int
	actorID              ids.ActorID
	taskID               ids.TaskID
	jobID                ids.JobID
	name                 string
	concurrencyGroupName string
}

// NewLocalModeTaskSubmitter creates a new LocalModeTaskSubmitter.
// functionMgr, when non-nil, enables re-synchronizing late-registered
// functions on each submission (see SubmitTask). Tests may pass nil to
// disable that behavior.
func NewLocalModeTaskSubmitter(
	objectStore *localstore.LocalModeObjectStore,
	workerContext *LocalModeWorkerContext,
	taskExecutor *LocalModeTaskExecutor,
	functionMgr *function.FunctionManager,
	actorConcurrencyGroupMgr *ActorConcurrencyGroupManager,
) *LocalModeTaskSubmitter {
	return &LocalModeTaskSubmitter{
		objectStore:              objectStore,
		workerContext:            workerContext,
		taskExecutor:             taskExecutor,
		functionMgr:              functionMgr,
		actorConcurrencyGroupMgr: actorConcurrencyGroupMgr,
		placementGroups:          make(map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions),
	}
}

// syncUserFunctions re-synchronizes functions registered since the last sync
// into the local FunctionManager. api.Remote / api.RegisterActorClass register
// lazily at call time, so a function or actor constructor registered after the
// runtime Start()ed must become visible before a task that needs it executes.
// The registry version only advances when a function is actually (re)registered,
// so the sync is skipped on the common path (no new registrations) instead of
// re-wrapping every function on every submission.
//
// It is a no-op when functionMgr is nil (e.g. unit tests that construct the
// submitter directly).
func (s *LocalModeTaskSubmitter) syncUserFunctions() error {
	if s.functionMgr == nil {
		return nil
	}
	current := api.GetRegisteredFunctionsVersion()
	if current == s.lastSyncedVersion.Load() {
		return nil
	}
	// Double-check under the lock: only the first caller that observed the stale
	// version runs the full sync; concurrent callers block here, see the updated
	// version, and skip. This avoids N concurrent submissions each re-registering
	// every function.
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if current == s.lastSyncedVersion.Load() {
		return nil
	}
	if err := registerUserFunctions(s.taskExecutor); err != nil {
		return err
	}
	// Record the version captured before the sync. If a function is registered
	// concurrently while we sync, the version advances and the next submission
	// re-syncs (idempotent), so no registration is ever missed.
	s.lastSyncedVersion.Store(current)
	return nil
}

// SubmitTask submits a normal task to be executed.
func (s *LocalModeTaskSubmitter) SubmitTask(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
	options *submitter.TaskOptions,
) ([]ids.ObjectID, error) {
	if err := s.syncUserFunctions(); err != nil {
		return nil, err
	}

	jobID := s.workerContext.GetCurrentJobID()
	parentTaskID := ids.NilTaskID()
	taskID := ids.TaskIDForNormalTask(jobID, parentTaskID, rand.Uint64())

	name, _ := taskOptionsFields(options)
	spec := &taskSpec{
		taskType:           base.TaskTypeNormal,
		functionDescriptor: functionDescriptor,
		args:               args,
		numReturns:         numReturns,
		taskID:             taskID,
		jobID:              jobID,
		name:               name,
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
	// Validate new actor creation options (aligned with Java LocalMode).
	// Shared boundary rule + error message with the native backend and the api
	// layer via submitter.ActorCreationOptions.ValidateMaxPendingCalls.
	// NormalizeMaxPendingCalls resolves the "unset" zero value to unlimited so a
	// zero-value ActorCreationOptions is valid.
	if options != nil {
		if err := options.ValidateMaxPendingCalls(); err != nil {
			return ids.NilActorID(), err
		}
		options.NormalizeMaxPendingCalls()
	}

	// The actor constructor may have been registered after the runtime
	// Start()ed (api.RegisterActorClass is lazy, like api.Remote). Sync it into
	// the local FunctionManager so the ACTOR_CREATION_TASK can resolve it;
	// otherwise creation fails and dependent actor tasks wait forever.
	if err := s.syncUserFunctions(); err != nil {
		return ids.NilActorID(), err
	}

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

	// Pre-register the actor's declared concurrency groups so a later
	// WithConcurrencyGroup routes to a group that honors the declared
	// concurrency (aligned with Java, which pre-registers declared groups at
	// actor creation). MaxCalls <= 0 resolves to the actor's max concurrency.
	for _, cg := range options.ConcurrencyGroups {
		maxCalls := cg.MaxCalls
		if maxCalls <= 0 {
			maxCalls = maxConcurrency
		}
		s.actorConcurrencyGroupMgr.GetOrCreateNamedGroup(actorID, cg.Name, maxCalls)
	}

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

	name, concurrencyGroupName := taskOptionsFields(options)
	spec := &taskSpec{
		taskType:             base.TaskTypeActorTask,
		functionDescriptor:   functionDescriptor,
		args:                 args,
		numReturns:           numReturns,
		actorID:              actorID,
		taskID:               taskID,
		jobID:                jobID,
		name:                 name,
		concurrencyGroupName: concurrencyGroupName,
	}

	returnIds := s.getReturnIds(taskID, numReturns)
	s.submitTaskSpec(spec)

	return returnIds, nil
}

// taskOptionsFields extracts the task name and concurrency group name from
// options, returning "" for fields that are nil/unset.
func taskOptionsFields(options *submitter.TaskOptions) (name, concurrencyGroupName string) {
	if options == nil {
		return "", ""
	}
	return options.Name, options.ConcurrencyGroupName
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
		return object.NewNativeActorHandle(actorInfo.actorID, object.LanguageGo), nil
	}
	return nil, fmt.Errorf("actor not found: name=%s, namespace=%s", name, namespace)
}

// GetActorHandle retrieves an actor handle by its actor ID.
// It resolves the live actor instance stored in the executor's actor manager:
// only actors that were successfully constructed (and not yet killed/exited)
// can be resolved, which also avoids reporting failed creations as available.
// Local mode only hosts Go actors, so the returned handle always carries
// LanguageGo.
//
// Semantics note: unlike the cluster path (which keeps a never-removed
// by-creation registry and still resolves a killed actor), local mode drops
// the instance on KillActor/intentional exit, so a killed actor is NOT
// resolvable here. This matches AC7 (a killed actor returns an error) and is
// an intentional difference between the two runtimes.
func (s *LocalModeTaskSubmitter) GetActorHandle(actorID ids.ActorID) (submitter.ActorHandle, error) {
	if actorID.IsNil() {
		return nil, fmt.Errorf("get actor handle: actor ID is nil")
	}
	if _, ok := s.taskExecutor.GetActorInstance(actorID); ok {
		return object.NewNativeActorHandle(actorID, object.LanguageGo), nil
	}
	return nil, fmt.Errorf("get actor handle: actor %s not found", actorID.Hex())
}

// KillActor kills an actor from the driver side.
//
// Local mode has no C++ CoreWorker or GCS, so there is no automatic restart:
// the kill is always final (equivalent to Java's noRestart=true semantics),
// mirroring the intentional-exit cleanup path. The actor's concurrency groups
// and task context are torn down, and any named-actor registration is removed,
// so later submissions and GetActor lookups report the actor as unavailable.
// The noRestart parameter is accepted for API compatibility but has no effect
// (Java's local mode, RayDevRuntime, does not support kill at all).
//
// Note: like every local-mode actor call (executeInGroup waits synchronously),
// killing an actor that is currently stuck in a task blocks until that task
// finishes; local mode cannot force-kill a hung task.
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

	// Release the live actor instance so a killed/exited actor is garbage
	// collected; GetActorHandle resolves only live instances, so removing the
	// instance also makes it unresolvable.
	s.taskExecutor.RemoveActorInstance(actorID)
}

// submitTaskSpec submits a task specification for execution.
func (s *LocalModeTaskSubmitter) submitTaskSpec(spec *taskSpec) {
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

	// Log at debug level only when enabled so the hex encoding of taskID is
	// not eagerly evaluated on every task.
	if log.Log.V(2).Enabled() {
		log.Log.V(2).Info("executing task", "taskID", spec.taskID.Hex(),
			"taskType", spec.taskType, "name", spec.name)
	}

	switch spec.taskType {
	case base.TaskTypeActorCreation:
		// Construct the actor instance and store it under the actor ID. The
		// instance stays in-process (not serialized), mirroring Java's
		// LocalModeTaskExecutor where the constructor runs and the instance is
		// kept in the actor context. On failure the error falls through to the
		// common error-object path below: it is Put into the dummy object ID (the
		// creation task's sole return ID), so dependent actor tasks are released
		// and their Get surfaces the failure instead of hanging forever.
		goDesc, descErr := function.FromBaseFunctionDescriptor(spec.functionDescriptor)
		if descErr != nil {
			err = descErr
			break
		}
		if err = s.taskExecutor.ExecuteActorCreation(spec.actorID, goDesc, spec.args); err != nil {
			break
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
		// Route through the executor: a non-empty concurrencyGroupName selects
		// the named group, otherwise the actor's default group is used.
		returnObjects, err = s.taskExecutor.ExecuteActorTaskInGroup(
			spec.actorID, spec.concurrencyGroupName, spec.functionDescriptor, spec.args)
		if err != nil {
			if rayerrors.IsActorExitError(err) {
				// Intentional exit: tear down the actor's concurrency group,
				// context, and named-actor registration so later calls report
				// the actor as unavailable.
				s.removeActorState(spec.actorID)
				log.Log.Info("local-mode actor removed on intentional exit",
					"actorID", spec.actorID.Hex())
			}
		}

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
		// ActorExitError is an intentional exit and is excluded: its actor state
		// was already removed above.
		if !rayerrors.IsActorExitError(err) {
			log.Log.Error(err, "local-mode task failed", "taskID", spec.taskID.Hex(),
				"taskType", spec.taskType, "name", spec.name)
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

// CreatePlacementGroup simulates creating a placement group by storing the
// options in an in-process map and returning its generated id. The group is
// considered ready immediately.
func (s *LocalModeTaskSubmitter) CreatePlacementGroup(ctx context.Context, opts *submitter.PlacementGroupCreationOptions) (ids.PlacementGroupID, error) {
	if err := opts.Validate(); err != nil {
		return ids.NilPlacementGroupID(), err
	}
	id := ids.OfPlacementGroupID(ids.NewJobID())
	s.placementGroupMu.Lock()
	defer s.placementGroupMu.Unlock()
	s.placementGroups[id] = opts
	return id, nil
}

// RemovePlacementGroup removes a placement group from the in-process map.
// Removing an unknown group is a no-op success, matching local-mode semantics.
func (s *LocalModeTaskSubmitter) RemovePlacementGroup(ctx context.Context, id ids.PlacementGroupID) error {
	s.placementGroupMu.Lock()
	defer s.placementGroupMu.Unlock()
	delete(s.placementGroups, id)
	return nil
}

// WaitPlacementGroupReady blocks until the placement group is ready. In local
// mode a created group is ready immediately, so this only fails when the group
// does not exist.
func (s *LocalModeTaskSubmitter) WaitPlacementGroupReady(ctx context.Context, id ids.PlacementGroupID, timeout time.Duration) error {
	s.placementGroupMu.Lock()
	defer s.placementGroupMu.Unlock()
	if _, ok := s.placementGroups[id]; ok {
		return nil
	}
	return fmt.Errorf("placement group %s not ready within %v: %w", id, timeout, submitter.ErrPlacementGroupNotReady)
}

// GetPlacementGroupLocal implements api.PlacementGroupLocalStore: it returns
// the placement group stored under id, or (nil, false) when it does not exist.
func (s *LocalModeTaskSubmitter) GetPlacementGroupLocal(ctx context.Context, id ids.PlacementGroupID) (*submitter.PlacementGroupCreationOptions, bool) {
	s.placementGroupMu.Lock()
	defer s.placementGroupMu.Unlock()
	opts, ok := s.placementGroups[id]
	return opts, ok
}

// ListPlacementGroupsLocal implements api.PlacementGroupLocalStore: it returns
// all locally stored placement groups keyed by id.
func (s *LocalModeTaskSubmitter) ListPlacementGroupsLocal(ctx context.Context) map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions {
	s.placementGroupMu.Lock()
	defer s.placementGroupMu.Unlock()
	out := make(map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions, len(s.placementGroups))
	for id, opts := range s.placementGroups {
		out[id] = opts
	}
	return out
}

// Compile-time check that LocalModeTaskSubmitter implements the api facade's
// in-process placement group store, so local-mode placement group reads
// (Get/GetByName/GetAll) resolve without a GCS client.
var _ api.PlacementGroupLocalStore = (*LocalModeTaskSubmitter)(nil)

// Compile-time check to ensure LocalModeTaskSubmitter implements TaskSubmitter
var _ submitter.TaskSubmitter = (*LocalModeTaskSubmitter)(nil)

// Compile-time check that LocalModeTaskSubmitter satisfies the structural
// actorKiller capability probed by api.KillActor. Without it, a signature drift
// would silently turn the public API back into a kill_actor_not_supported error.
var _ interface {
	KillActor(ids.ActorID, bool) error
} = (*LocalModeTaskSubmitter)(nil)
