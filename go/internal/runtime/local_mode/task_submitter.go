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
	"fmt"
	"math/rand"
	"sync"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/internal/runtime/objectstore"
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
		// Serialize the error as a task-execution exception object and put it
		// into the return slot, so a caller Get()ing this ObjectRef receives the
		// failure instead of blocking forever on a never-ready object.
		excBytes, serErr := (&object.RayExceptionSerializer{}).ToBytes(
			object.NewRayTaskExecutionException(spec.taskID.String(), err, ""))
		if serErr == nil {
			excObj := &object.NativeRayObject{
				Data:     excBytes,
				Metadata: []byte(object.MetadataTypeTaskExecutionException),
			}
			if len(returnIds) > 0 {
				s.objectStore.PutRawWithID(excObj, &returnIds[0])
			}
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
