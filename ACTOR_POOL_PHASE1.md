# Actor Pool API - Phase 1 Implementation Summary

## Overview

This document summarizes the Phase 1 implementation of the Ray Core Actor Pool API, which provides C++-backed pool management with cross-actor retry capabilities.

### Problem Statement

Ray's current actor task retry mechanism pins retried tasks to the **same actor instance**:

```cpp
// src/ray/core_worker/core_worker.cc (current behavior)
for (auto &task_to_retry : tasks_to_resubmit) {
  auto &spec = task_to_retry.task_spec;
  if (spec.IsActorTask()) {
    auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
    actor_handle->SetResubmittedActorTaskSpec(spec);
    actor_task_submitter_->SubmitTask(spec);  // Always same ActorId!
  }
}
```

This causes:
- **Thundering herd**: All retries go to one actor, overwhelming it
- **No load redistribution**: Failed actor's work can't move to healthy actors
- **Inefficient recovery**: No cross-actor retry capability

### Solution

The new `ActorPoolManager` intercepts task failures and **re-enqueues work to the pool**, enabling retry on a **different actor**. This is the key innovation.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Python API (Phase 2)                        │
│           ray.experimental.actor_pool.ActorPool                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Cython Bindings (Phase 2)                     │
│                   python/ray/_raylet.pyx                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  C++ ActorPoolManager (Phase 1) ✅              │
│               src/ray/core_worker/actor_pool_manager.{h,cc}     │
├─────────────────────────────────────────────────────────────────┤
│  • Pool registration & lifecycle management                     │
│  • Actor membership (add/remove)                                │
│  • Task submission with actor selection                         │
│  • Load-balanced actor selection                                │
│  • Cross-actor retry with exponential backoff                   │
│  • Error classification (system vs user errors)                 │
│  • Pool statistics & observability                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  C++ PoolWorkQueue (Phase 1) ✅                 │
│           src/ray/core_worker/actor_pool_work_queue.{h,cc}      │
├─────────────────────────────────────────────────────────────────┤
│  • Abstract PoolWorkQueue interface                             │
│  • UnorderedPoolWorkQueue (FIFO, highest throughput)            │
│  • PoolWorkItem struct (task + retry metadata)                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Completed Implementation

### Commit History (12 commits)

```
24bd9bfd7d Fix compilation errors in ActorPoolManager
b406d8185c Fix compilation errors in ActorPoolWorkQueue
55ec273657 Add Bazel build rules for ActorPool components
694daf524e Add CoreWorker integration plan for ActorPoolManager
8525651440 Implement cross-actor retry with backoff
814d6f584a Implement task submission to pool (skeleton)
a066001927 Implement actor selection with locality-aware load balancing
4ca23e3055 Create ActorPoolManager class skeleton
9c36da0832 Add unit tests for PoolWorkQueue
90315293dc Add PoolWorkQueue interface and unordered implementation
d97cdbec80 Add actor_pool_id fields to TaskSpec protobuf
46f89b5586 Add ActorPoolID type to Ray Core
```

### Files Created/Modified

| File | Type | Description |
|------|------|-------------|
| `src/ray/common/id.h` | Modified | Added `ActorPoolID` type |
| `src/ray/common/id.cc` | Modified | Implemented `ActorPoolID::FromRandom()` |
| `src/ray/protobuf/common.proto` | Modified | Added `actor_pool_id` and `actor_pool_work_item_id` to `TaskSpec` |
| `src/ray/core_worker/actor_pool_work_queue.h` | New | `PoolWorkQueue` interface, `PoolWorkItem` struct |
| `src/ray/core_worker/actor_pool_work_queue.cc` | New | `UnorderedPoolWorkQueue` implementation |
| `src/ray/core_worker/actor_pool_manager.h` | New | `ActorPoolManager` class declaration |
| `src/ray/core_worker/actor_pool_manager.cc` | New | `ActorPoolManager` implementation (~565 lines) |
| `src/ray/core_worker/tests/actor_pool_work_queue_test.cc` | New | Unit tests for work queue |
| `src/ray/core_worker/BUILD.bazel` | Modified | Added library targets |
| `src/ray/core_worker/tests/BUILD.bazel` | Modified | Added test target |

---

## C++ API Reference

### ActorPoolID

New unique identifier type for actor pools:

```cpp
// src/ray/common/id.h
class ActorPoolID : public BaseID<ActorPoolID> {
 public:
  static constexpr size_t kLength = 16;
  
  static ActorPoolID FromRandom();
  static ActorPoolID FromBinary(const std::string &binary);
  static ActorPoolID Nil();
  
  bool IsNil() const;
  size_t Hash() const;
  std::string Binary() const;
  std::string Hex() const;
};
```

### ActorPoolConfig

Configuration for an actor pool:

```cpp
struct ActorPoolConfig {
  // Retry configuration
  int32_t max_retry_attempts = 3;        // Max retries before permanent failure
  int32_t retry_backoff_ms = 1000;       // Initial backoff (1 second)
  float retry_backoff_multiplier = 2.0f; // Exponential multiplier
  int32_t max_retry_backoff_ms = 60000;  // Max backoff (60 seconds)
  bool retry_on_system_errors = true;    // Retry on system errors
  
  // Ordering mode
  PoolOrderingMode ordering_mode = PoolOrderingMode::UNORDERED;
  
  // Autoscaling configuration
  int32_t min_size = 1;
  int32_t max_size = -1;  // -1 = unbounded
  int32_t initial_size = 1;
  
  // Phase 2: Topology
  std::vector<int32_t> shape;
  std::vector<std::string> shape_names;
  
  // Phase 2: Placement
  PlacementGroupID placement_group_id;
};
```

### PoolOrderingMode

```cpp
enum class PoolOrderingMode {
  UNORDERED = 0,    // Tasks execute in any order (highest throughput)
  PER_KEY_FIFO = 1, // Tasks with same key execute in FIFO order
  GLOBAL_FIFO = 2,  // All tasks execute in strict FIFO order
};
```

### ActorPoolManager

Main class for managing actor pools:

```cpp
class ActorPoolManager {
 public:
  ActorPoolManager(ActorManager &actor_manager,
                   ActorTaskSubmitterInterface &task_submitter,
                   TaskManagerInterface &task_manager);
  
  // Pool lifecycle
  ActorPoolID RegisterPool(const ActorPoolConfig &config,
                           const std::vector<ActorID> &initial_actors = {});
  void UnregisterPool(const ActorPoolID &pool_id);
  
  // Actor membership
  void AddActorToPool(const ActorPoolID &pool_id,
                      const ActorID &actor_id,
                      const NodeID &location);
  void RemoveActorFromPool(const ActorPoolID &pool_id, const ActorID &actor_id);
  
  // Task submission (pool selects actor)
  std::vector<rpc::ObjectReference> SubmitTaskToPool(
      const ActorPoolID &pool_id,
      const RayFunction &function,
      std::vector<std::unique_ptr<TaskArg>> args,
      const TaskOptions &task_options,
      const std::string &key = "");
  
  // Introspection
  std::vector<ActorID> GetPoolActors(const ActorPoolID &pool_id) const;
  PoolStats GetPoolStats(const ActorPoolID &pool_id) const;
  bool HasPool(const ActorPoolID &pool_id) const;
};
```

### PoolStats

Statistics for monitoring:

```cpp
struct PoolStats {
  int64_t total_tasks_submitted = 0;  // Total tasks submitted
  int64_t total_tasks_failed = 0;     // Total permanent failures
  int64_t total_tasks_retried = 0;    // Total retry attempts
  int32_t num_actors = 0;             // Current actor count
  size_t backlog_size = 0;            // Queued work items
  int32_t total_in_flight = 0;        // Tasks currently executing
};
```

### PoolWorkQueue

Abstract interface for work queues:

```cpp
class PoolWorkQueue {
 public:
  virtual ~PoolWorkQueue() = default;
  
  virtual void Push(PoolWorkItem item) = 0;
  virtual std::optional<PoolWorkItem> Pop() = 0;
  virtual bool HasWork() const = 0;
  virtual size_t Size() const = 0;
  virtual void Clear() = 0;
};
```

### PoolWorkItem

Work item struct:

```cpp
struct PoolWorkItem {
  TaskID work_item_id;                        // Unique ID
  RayFunction function;                       // Function to execute
  std::vector<std::unique_ptr<TaskArg>> args; // Arguments
  TaskOptions options;                        // Task options
  std::string key;                            // Key for ordering
  int32_t attempt_number = 0;                 // Retry attempt count
  int64_t enqueued_at_ms = 0;                 // Enqueue timestamp
};
```

---

## Key Algorithms

### Actor Selection (Load Balancing)

```cpp
ActorID ActorPoolManager::SelectActorFromPool(
    const ActorPoolID &pool_id,
    const std::vector<ObjectID> &arg_ids) {
  
  auto &pool_info = pools_[pool_id];
  
  // Filter: only alive actors with capacity
  std::vector<ActorID> candidates;
  for (const auto &actor_id : pool_info.actor_ids) {
    auto &state = pool_info.actor_states[actor_id];
    if (state.is_alive) {
      candidates.push_back(actor_id);
    }
  }
  
  if (candidates.empty()) {
    return ActorID::Nil();
  }
  
  // Select actor with lowest rank (locality * 10000 + load)
  auto best_actor = *std::min_element(candidates.begin(), candidates.end(),
      [&](const ActorID &a, const ActorID &b) {
        return RankActor(a, arg_ids, pool_info) < RankActor(b, arg_ids, pool_info);
      });
  
  return best_actor;
}
```

### Cross-Actor Retry Logic

```cpp
void ActorPoolManager::OnTaskFailed(
    const ActorPoolID &pool_id,
    const TaskID &work_item_id,
    const ActorID &failed_actor_id,
    const rpc::RayErrorInfo &error_info) {
  
  auto &pool_info = pools_[pool_id];
  auto &actor_state = pool_info.actor_states[failed_actor_id];
  
  // Update actor state
  actor_state.num_tasks_in_flight--;
  actor_state.consecutive_failures++;
  
  // Check if should retry
  bool should_retry = ShouldRetryTask(pool_info.config, error_info);
  
  if (should_retry) {
    auto work_item_it = work_items_.find(work_item_id);
    if (work_item_it != work_items_.end()) {
      PoolWorkItem work_item = std::move(work_item_it->second);
      work_items_.erase(work_item_it);
      work_item.attempt_number++;
      
      if (work_item.attempt_number <= pool_info.config.max_retry_attempts) {
        // KEY INNOVATION: Re-enqueue to pool (not same actor!)
        pool_info.total_tasks_retried++;
        
        int64_t backoff_ms = CalculateBackoff(
            work_item.attempt_number,
            pool_info.config.retry_backoff_ms,
            pool_info.config.retry_backoff_multiplier,
            pool_info.config.max_retry_backoff_ms);
        
        ScheduleRetry(pool_id, std::move(work_item), backoff_ms);
      } else {
        FailWorkItem(work_item_id, error_info);
      }
    }
  } else {
    FailWorkItem(work_item_id, error_info);
  }
}
```

### Exponential Backoff

```cpp
int64_t ActorPoolManager::CalculateBackoff(
    int32_t attempt_number,
    int32_t base_backoff_ms,
    float multiplier,
    int32_t max_backoff_ms) const {
  
  // Exponential backoff: base * multiplier^(attempt-1)
  float backoff = base_backoff_ms * std::pow(multiplier, attempt_number - 1);
  
  // Cap at max
  return std::min(static_cast<int64_t>(backoff), 
                  static_cast<int64_t>(max_backoff_ms));
}
```

### Error Classification

```cpp
bool ActorPoolManager::ShouldRetryTask(
    const ActorPoolConfig &config,
    const rpc::RayErrorInfo &error_info) const {
  
  if (!config.retry_on_system_errors) {
    return false;
  }
  
  // System errors that should trigger retry
  switch (error_info.error_type()) {
    case rpc::ErrorType::ACTOR_DIED:
    case rpc::ErrorType::ACTOR_UNAVAILABLE:
    case rpc::ErrorType::WORKER_DIED:
    case rpc::ErrorType::NODE_DIED:
    case rpc::ErrorType::OBJECT_LOST:
    case rpc::ErrorType::OBJECT_FETCH_TIMED_OUT:
    case rpc::ErrorType::OUT_OF_MEMORY:
    case rpc::ErrorType::TASK_EXECUTION_EXCEPTION:
      return true;
    
    // User errors - don't retry
    case rpc::ErrorType::TASK_CANCELLED:
    case rpc::ErrorType::RUNTIME_ENV_SETUP_FAILED:
    default:
      return false;
  }
}
```

---

## Protobuf Changes

Added to `TaskSpec` in `src/ray/protobuf/common.proto`:

```protobuf
message TaskSpec {
  // ... existing fields ...
  
  // The ID of the actor pool this task belongs to (if any).
  // This is used for cross-actor retry within a pool.
  optional bytes actor_pool_id = 46;
  
  // The ID of the work item within the actor pool.
  // Used to track work items across retries.
  optional bytes actor_pool_work_item_id = 47;
}
```

---

## Build Integration

### Bazel Targets

```python
# src/ray/core_worker/BUILD.bazel

ray_cc_library(
    name = "actor_pool_work_queue",
    srcs = ["actor_pool_work_queue.cc"],
    hdrs = ["actor_pool_work_queue.h"],
    deps = [
        ":common",
        "//src/ray/common:id",
        "//src/ray/common:task_common",
    ],
)

ray_cc_library(
    name = "actor_pool_manager",
    srcs = ["actor_pool_manager.cc"],
    hdrs = ["actor_pool_manager.h"],
    deps = [
        ":actor_manager",
        ":actor_pool_work_queue",
        ":common",
        ":task_manager_interface",
        "//src/ray/common:id",
        "//src/ray/common:task_common",
        "//src/ray/core_worker/task_submission:actor_task_submitter",
        "//src/ray/protobuf:common_cc_proto",
        "//src/ray/util:time",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_prod",
    ],
)

# src/ray/core_worker/tests/BUILD.bazel

ray_cc_test(
    name = "actor_pool_work_queue_test",
    size = "small",
    srcs = ["actor_pool_work_queue_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:id",
        "//src/ray/common:task_common",
        "//src/ray/core_worker:actor_pool_work_queue",
        "@com_google_googletest//:gtest_main",
    ],
)
```

### Build Commands

```bash
# Build work queue library
bazel build //src/ray/core_worker:actor_pool_work_queue

# Build pool manager library
bazel build //src/ray/core_worker:actor_pool_manager

# Run work queue tests
bazel test //src/ray/core_worker/tests:actor_pool_work_queue_test
```

---

## Test Coverage

### Unit Tests (Passing ✅)

`actor_pool_work_queue_test.cc`:

| Test Name | Description |
|-----------|-------------|
| `BasicPushPop` | Push one item, pop it, verify empty |
| `FIFOOrdering` | Push 5 items, verify FIFO order |
| `RetryWithIncrementedAttempt` | Pop, increment attempt, re-push, verify |
| `Clear` | Push 10 items, clear, verify empty |
| `ManyItems` | Push/pop 1000 items, verify order |
| `InterleavedPushPop` | Interleaved operations |
| `WorkItemPreservesKey` | Keys preserved through queue |

---

## Remaining Work

### Phase 1 Completion (Next Steps)

1. **CoreWorker Integration** (complex)
   - Add `ActorPoolManager` as member of `CoreWorker`
   - Wire task submission through `CoreWorker::SubmitActorPoolTask()`
   - Connect failure callbacks to `OnTaskFailed()`
   - Handle delayed retry via `io_service_`

2. **C++ Unit Tests for ActorPoolManager**
   - Test pool registration/unregistration
   - Test actor add/remove
   - Test actor selection (load balancing)
   - Test cross-actor retry flow

3. **Python Bindings (Cython)**
   - Expose `ActorPoolManager` to Python
   - Create `ActorPoolHandle` class

4. **Python ActorPool Class**
   - User-facing API in `ray.experimental.actor_pool`
   - Thin wrapper over C++ implementation

5. **Ray Data Integration**
   - Replace `ActorPoolMapOperator` internals
   - Feature flag: `RAY_DATA_USE_CORE_ACTOR_POOL`

---

## Usage Example (Target API)

Once Phase 1 is complete, the API will look like:

```python
from ray.experimental.actor_pool import ActorPool, RetryPolicy, OrderingMode

@ray.remote
class Worker:
    def process(self, data):
        return data * 2

# Create pool with cross-actor retry
pool = ActorPool(
    actor_cls=Worker,
    size=4,
    retry=RetryPolicy(
        max_attempts=3,
        backoff_ms=1000,
        retry_on="system_errors"
    ),
    ordering=OrderingMode.UNORDERED,
)

# Submit tasks - pool selects actor
refs = [pool.submit("process", i) for i in range(100)]

# If an actor fails, C++ automatically retries on different actor!
results = ray.get(refs)

# Get statistics
stats = pool.stats()
print(f"Submitted: {stats['total_tasks_submitted']}")
print(f"Retried: {stats['total_tasks_retried']}")

pool.shutdown()
```

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Implementation language | C++ | Performance-critical path; avoid Python overhead |
| Thread safety | `absl::Mutex` | Project standard; thread-safe annotations |
| Hash map | `absl::flat_hash_map` | Project standard; fast lookups |
| Work queue | Abstract interface | Extensible for different ordering modes |
| Retry backoff | Exponential | Industry standard; prevents thundering herd |
| Error classification | Explicit switch | Clear retry semantics per error type |
| Pool ID | 16-byte UUID | Consistent with other Ray IDs |

---

## References

- **Integration Plan**: `ACTOR_POOL_INTEGRATION_PLAN.md`
- **Full Design**: `ray.plan.md`
- **ActorMesh PRD**: `ActorMesh PRD.md`
- **Ray Data Pool**: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`

