# ActorPoolManager CoreWorker Integration Plan

## Current Status

**Completed C++ Components**:
- ✅ `ActorPoolID` type in `src/ray/common/id.h`
- ✅ `PoolWorkQueue` interface + `UnorderedPoolWorkQueue`
- ✅ `ActorPoolManager` with core logic:
  - Pool registration/unregistration
  - Actor add/remove
  - Actor selection (load-based)
  - Cross-actor retry logic
  - Exponential backoff
  - Error classification
  - Pool statistics

**What's Missing**: Full integration with CoreWorker for actual task submission.

---

## Integration Points Needed

### 1. Add ActorPoolManager Member to CoreWorker

**File**: `src/ray/core_worker/core_worker.h`

```cpp
// Around line 1822, after actor_manager_
/// Interface to manage actor pools.
std::unique_ptr<ActorPoolManager> actor_pool_manager_;
```

### 2. Initialize ActorPoolManager in CoreWorker Constructor

**File**: `src/ray/core_worker/core_worker.cc`

```cpp
// In CoreWorker::CoreWorker() constructor
// After actor_manager_ initialization
actor_pool_manager_ = std::make_unique<ActorPoolManager>(
    *actor_manager_,
    *actor_task_submitter_,
    *task_manager_);
```

### 3. Implement Full SubmitToActor in ActorPoolManager

**Needs**:
- Access to `WorkerContext` for job/task IDs
- Access to `rpc_address_` for caller address
- Ability to call `BuildCommonTaskSpec` and `SetActorTaskSpec`
- Task callback registration for failure handling

**Options**:
A. Pass CoreWorker context to ActorPoolManager methods
B. Make ActorPoolManager a friend of CoreWorker
C. Add a `SubmitActorPoolTask()` method to CoreWorker that ActorPoolManager calls

**Recommendation**: Option C - Add `CoreWorker::SubmitActorPoolTask()` that ActorPoolManager calls.

### 4. Wire Task Failure Callbacks

When a pool task fails, need to invoke `ActorPoolManager::OnTaskFailed()`.

**Approach**: Register callback in TaskManager that checks if task belongs to a pool.

```cpp
// In ActorPoolManager::SubmitToActor
task_manager_.AddPendingTask(
    task_spec,
    [this, pool_id, work_item_id, actor_id](
        Status status, const rpc::RayErrorInfo *error_info) {
      if (!status.ok()) {
        OnTaskFailed(pool_id, work_item_id, actor_id, *error_info);
      } else {
        OnTaskSucceeded(pool_id, actor_id);
      }
    });
```

### 5. Handle Delayed Retry Scheduling

Use CoreWorker's `io_service_` to schedule delayed retries.

```cpp
// In ActorPoolManager::ScheduleRetry
if (backoff_ms > 0) {
  io_service_.post_after(
      std::chrono::milliseconds(backoff_ms),
      [this, pool_id, work_item = std::move(work_item)]() mutable {
        RetryWorkItem(pool_id, std::move(work_item));
      });
}
```

**Need**: Pass `io_service_` reference to ActorPoolManager or add scheduling method to CoreWorker.

---

## Recommended Implementation Sequence

### Step 1: Add CoreWorker Helper Method

Add `CoreWorker::SubmitActorPoolTask()` that ActorPoolManager can call:

```cpp
std::vector<rpc::ObjectReference> CoreWorker::SubmitActorPoolTask(
    const ActorPoolID &pool_id,
    const ActorID &actor_id,
    const TaskID &work_item_id,
    const RayFunction &function,
    std::vector<std::unique_ptr<TaskArg>> args,
    const TaskOptions &task_options);
```

### Step 2: Update ActorPoolManager Constructor

Pass additional references needed:

```cpp
ActorPoolManager(
    ActorManager &actor_manager,
    ActorTaskSubmitterInterface &task_submitter,
    TaskManagerInterface &task_manager,
    WorkerContext &worker_context,  // NEW
    instrumented_io_context &io_service,  // NEW
    std::function<std::vector<rpc::ObjectReference>(...)> submit_fn);  // NEW
```

### Step 3: Implement Full SubmitToActor

Use CoreWorker helper to build and submit TaskSpec properly.

### Step 4: Wire Failure Callbacks

Ensure TaskManager invokes pool callbacks on failures.

---

## Alternative: Simpler Incremental Approach

Since full CoreWorker integration is complex, we can:

1. **Defer full integration to a separate focused commit**
2. **For now**: Add ActorPoolManager to CoreWorker as a member
3. **Expose basic APIs** through CoreWorker public interface
4. **Test via Python bindings** (which we implement next)
5. **Complete full integration** when wiring Python ActorPool

This allows us to:
- Get Python bindings working sooner
- Test the retry logic via Python integration tests
- Incrementally complete the C++ wiring

---

## Next Steps

**Recommended**: Skip detailed CoreWorker integration for now, move to:
- **To-do #12**: C++ unit tests for ActorPoolManager (test the logic we have)
- **To-do #14**: Python bindings (Cython)
- **To-do #15**: Python ActorPool class
- **To-do #16**: Python integration tests

Then circle back to complete CoreWorker integration when we wire everything together for Ray Data.

This is a more iterative, testable approach.

