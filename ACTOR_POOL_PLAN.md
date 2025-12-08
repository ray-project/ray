# Ray Core Actor Pool API - C++ Implementation from Start

## Strategic Decision: C++ From Phase 1

**Rationale** (from team discussion):

- Pure Python implementation doesn't add significant value
- Python for ergonomic user-facing API
- C++ for pool management, task routing, retry logic
- Get correctness AND performance from the start

**Architecture** (from whiteboard):

```
Python API (ray.experimental.actor_pool)
    ↓
Cython bindings (_raylet.pyx)
    ↓
C++ ActorPool (core_worker)
    ├─ Pool registration & management
    ├─ Task submission & routing
    ├─ Cross-actor retry logic
    ├─ Load balancing / scheduling
    └─ Health tracking & autoscaling
```

---

## Current Limitations (with code evidence)

### 1. Actor-bound retries (PRIMARY ISSUE)

**Problem**: When an actor task fails, Ray Core resubmits it to the *same* actor instance.

**Evidence**:

```800:822:src/ray/core_worker/core_worker.cc
  // Retry tasks.
  for (auto &task_to_retry : tasks_to_resubmit) {
    auto &spec = task_to_retry.task_spec;
    if (spec.IsActorTask()) {
      auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
      actor_handle->SetResubmittedActorTaskSpec(spec);
      actor_task_submitter_->SubmitTask(spec);  // Resubmits to SAME ActorId
```

The retry logic fetches the `ActorHandle` by `spec.ActorId()` and resubmits to that specific actor. **There is no way to retry on a different actor** because the TaskSpec is already bound to one ActorId.

**C++ Solution**: `ActorPoolManager` in C++ intercepts failures, re-enqueues work, and submits to different actor in pool.

### 2. Application-layer load balancing overhead

**Ray Data** and **Ray Serve** both implement complex scheduling in Python with manual state tracking, RPC probes, and callbacks.

**C++ Solution**: Leverage existing `ClientQueue` state in `ActorTaskSubmitter` for zero-overhead load balancing.

---

## C++ Architecture Design

### Core C++ Components

#### 1. ActorPoolID & Registry

```cpp
// src/ray/common/id.h
class ActorPoolID : public UniqueID {
 public:
  static ActorPoolID FromRandom();
  static ActorPoolID FromBinary(const std::string &binary);
  // ... standard UniqueID interface
};

// src/ray/core_worker/actor_pool_manager.h
class ActorPoolManager {
 public:
  ActorPoolManager(ActorManager &actor_manager,
                   ActorTaskSubmitter &task_submitter);
  
  // Register a new pool
  ActorPoolID RegisterPool(
      const ActorPoolConfig &config,
      const std::vector<ActorID> &initial_actors = {});
  
  // Add/remove actors from pool
  void AddActorToPool(const ActorPoolID &pool_id, const ActorID &actor_id);
  void RemoveActorFromPool(const ActorPoolID &pool_id, const ActorID &actor_id);
  
  // Submit task to pool (C++ picks actor)
  std::vector<rpc::ObjectReference> SubmitTaskToPool(
      const ActorPoolID &pool_id,
      const RayFunction &function,
      const std::vector<std::unique_ptr<TaskArg>> &args,
      const TaskOptions &task_options);
  
  // Pool introspection
  std::vector<ActorID> GetPoolActors(const ActorPoolID &pool_id) const;
  PoolStats GetPoolStats(const ActorPoolID &pool_id) const;
  
 private:
  // Select best actor from pool based on load & locality
  ActorID SelectActorFromPool(const ActorPoolID &pool_id,
                               const std::vector<ObjectID> &arg_ids);
  
  // Handle task failure & retry
  void OnTaskFailed(const ActorPoolID &pool_id,
                    const TaskID &task_id,
                    const ActorID &failed_actor_id,
                    const rpc::RayErrorInfo &error_info);
  
  ActorManager &actor_manager_;
  ActorTaskSubmitter &task_submitter_;
  
  // Pool registry
  absl::flat_hash_map<ActorPoolID, ActorPoolInfo> pools_;
  absl::flat_hash_map<ActorID, ActorPoolID> actor_to_pool_;
  
  // Per-pool work queues
  absl::flat_hash_map<ActorPoolID, std::unique_ptr<PoolWorkQueue>> work_queues_;
};

struct ActorPoolConfig {
  // Retry configuration
  int32_t max_retry_attempts = 3;
  int32_t retry_backoff_ms = 1000;
  bool retry_on_system_errors = true;
  
  // Ordering
  PoolOrderingMode ordering_mode = PoolOrderingMode::UNORDERED;
  
  // Autoscaling
  int32_t min_size = 1;
  int32_t max_size = -1;  // -1 = unbounded
  int32_t initial_size = 1;
  
  // Topology (Phase 2)
  std::vector<int32_t> shape;
  std::vector<std::string> shape_names;
  
  // Placement (Phase 2)
  PlacementGroupID placement_group_id;
};

struct ActorPoolInfo {
  ActorPoolConfig config;
  std::vector<ActorID> actor_ids;
  absl::flat_hash_map<ActorID, ActorPoolActorState> actor_states;
  int64_t total_tasks_submitted = 0;
  int64_t total_tasks_failed = 0;
  int64_t total_tasks_retried = 0;
};

struct ActorPoolActorState {
  int32_t num_tasks_in_flight = 0;
  NodeID location;
  bool is_alive = true;
  int32_t consecutive_failures = 0;
};
```

#### 2. Pool Work Queue

```cpp
// src/ray/core_worker/actor_pool_work_queue.h
class PoolWorkQueue {
 public:
  virtual ~PoolWorkQueue() = default;
  
  // Enqueue work
  virtual void Push(const PoolWorkItem &item) = 0;
  
  // Dequeue work (returns nullopt if no work available)
  virtual std::optional<PoolWorkItem> Pop() = 0;
  
  // Check if work is available
  virtual bool HasWork() const = 0;
  
  // Queue depth
  virtual size_t Size() const = 0;
};

// Unordered queue (Phase 1)
class UnorderedPoolWorkQueue : public PoolWorkQueue {
  std::deque<PoolWorkItem> queue_;
};

// Per-key ordered queue (Phase 2)
class PerKeyOrderedPoolWorkQueue : public PoolWorkQueue {
  absl::flat_hash_map<std::string, std::deque<PoolWorkItem>> per_key_queues_;
  absl::flat_hash_map<std::string, int32_t> key_in_flight_;
  int32_t max_in_flight_per_key_ = 1;
};

struct PoolWorkItem {
  TaskID task_id;
  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;
  std::string key;  // For per-key ordering
  int32_t attempt_number = 0;
  int64_t enqueued_at_ms;
};
```

#### 3. Task Submission Flow (C++)

```cpp
// In ActorPoolManager::SubmitTaskToPool()
std::vector<rpc::ObjectReference> ActorPoolManager::SubmitTaskToPool(
    const ActorPoolID &pool_id,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options) {
  
  auto pool_it = pools_.find(pool_id);
  RAY_CHECK(pool_it != pools_.end()) << "Pool not found: " << pool_id;
  
  auto &pool_info = pool_it->second;
  auto &work_queue = work_queues_[pool_id];
  
  // Create work item
  PoolWorkItem work_item{
    .task_id = TaskID::ForNormalTask(/*...*/),
    .function = function,
    .args = std::move(args),
    .options = task_options,
    .attempt_number = 0,
    .enqueued_at_ms = current_time_ms()
  };
  
  // Select actor from pool
  ActorID selected_actor = SelectActorFromPool(pool_id, GetArgObjectIDs(args));
  
  if (selected_actor.IsNil()) {
    // No actors available, enqueue work
    work_queue->Push(std::move(work_item));
    return {};
  }
  
  // Submit to selected actor
  return SubmitToActor(pool_id, selected_actor, std::move(work_item));
}

std::vector<rpc::ObjectReference> ActorPoolManager::SubmitToActor(
    const ActorPoolID &pool_id,
    const ActorID &actor_id,
    PoolWorkItem work_item) {
  
  // Build TaskSpec (similar to CoreWorker::SubmitActorTask)
  TaskSpecBuilder builder;
  // ... build task spec ...
  
  // CRITICAL: Set pool metadata in TaskSpec
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  task_spec.SetActorPoolId(pool_id);
  task_spec.SetActorPoolWorkItemId(work_item.task_id);
  
  // Track in-flight
  auto &pool_info = pools_[pool_id];
  pool_info.actor_states[actor_id].num_tasks_in_flight++;
  pool_info.total_tasks_submitted++;
  
  // Register failure callback
  RegisterTaskCallback(task_spec.TaskId(), [this, pool_id, actor_id, work_item](
      Status status, const rpc::RayErrorInfo *error_info) {
    if (!status.ok()) {
      OnTaskFailed(pool_id, work_item.task_id, actor_id, *error_info);
    } else {
      OnTaskSucceeded(pool_id, actor_id);
    }
  });
  
  // Submit via ActorTaskSubmitter
  task_submitter_.SubmitTask(task_spec);
  
  return task_spec.ReturnRefs();
}
```

#### 4. Cross-Actor Retry Logic (C++)

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
  
  // Classify error
  bool should_retry = ShouldRetryTask(pool_info.config, error_info);
  
  if (should_retry) {
    // Retrieve original work item (need to track these)
    auto work_item = GetWorkItem(work_item_id);
    work_item.attempt_number++;
    
    if (work_item.attempt_number <= pool_info.config.max_retry_attempts) {
      // RE-ENQUEUE to pool (KEY INNOVATION)
      pool_info.total_tasks_retried++;
      
      // Backoff before retry
      int64_t backoff_ms = CalculateBackoff(work_item.attempt_number, 
                                            pool_info.config.retry_backoff_ms);
      
      // Schedule retry
      ScheduleRetry(pool_id, std::move(work_item), backoff_ms);
    } else {
      // Max retries exceeded, fail permanently
      FailWorkItem(work_item_id, error_info);
    }
  } else {
    // Non-retriable error, fail immediately
    FailWorkItem(work_item_id, error_info);
  }
}

void ActorPoolManager::ScheduleRetry(
    const ActorPoolID &pool_id,
    PoolWorkItem work_item,
    int64_t backoff_ms) {
  
  if (backoff_ms > 0) {
    // Schedule delayed retry
    io_service_.post_after(
        std::chrono::milliseconds(backoff_ms),
        [this, pool_id, work_item = std::move(work_item)]() mutable {
          RetryWorkItem(pool_id, std::move(work_item));
        });
  } else {
    // Immediate retry
    RetryWorkItem(pool_id, std::move(work_item));
  }
}

void ActorPoolManager::RetryWorkItem(
    const ActorPoolID &pool_id,
    PoolWorkItem work_item) {
  
  // Select DIFFERENT actor (likely, due to load balancing)
  ActorID selected_actor = SelectActorFromPool(pool_id, GetArgObjectIDs(work_item.args));
  
  if (selected_actor.IsNil()) {
    // No actors available, back to queue
    work_queues_[pool_id]->Push(std::move(work_item));
  } else {
    // Submit to (likely different) actor
    SubmitToActor(pool_id, selected_actor, std::move(work_item));
  }
}
```

#### 5. Actor Selection (Load Balancing)

```cpp
ActorID ActorPoolManager::SelectActorFromPool(
    const ActorPoolID &pool_id,
    const std::vector<ObjectID> &arg_ids) {
  
  auto &pool_info = pools_[pool_id];
  
  // Filter: only alive actors with capacity
  std::vector<ActorID> candidates;
  for (const auto &actor_id : pool_info.actor_ids) {
    auto &state = pool_info.actor_states[actor_id];
    if (state.is_alive && state.num_tasks_in_flight < GetMaxConcurrency(actor_id)) {
      candidates.push_back(actor_id);
    }
  }
  
  if (candidates.empty()) {
    return ActorID::Nil();
  }
  
  // Rank candidates by (locality, load)
  auto best_actor = *std::min_element(candidates.begin(), candidates.end(),
      [&](const ActorID &a, const ActorID &b) {
        return RankActor(a, arg_ids, pool_info) < RankActor(b, arg_ids, pool_info);
      });
  
  return best_actor;
}

int32_t ActorPoolManager::RankActor(
    const ActorID &actor_id,
    const std::vector<ObjectID> &arg_ids,
    const ActorPoolInfo &pool_info) {
  
  auto &state = pool_info.actor_states[actor_id];
  
  // Lower is better
  int32_t locality_rank = GetLocalityRank(state.location, arg_ids);  // 0=local, 1=same-AZ, 2=remote
  int32_t load = state.num_tasks_in_flight;
  
  // Prioritize locality, break ties with load
  return locality_rank * 10000 + load;
}
```

---

## Python API (Thin Wrapper over C++)

### Python Bindings (Cython)

```python
# python/ray/_raylet.pyx

cdef class ActorPoolHandle:
    cdef:
        CActorPoolID c_pool_id
        shared_ptr[CActorPoolManager] c_pool_manager
    
    def submit(self, method_name, *args, key=None, **kwargs):
        """Submit task to pool."""
        # Convert to C++ types
        c_function = ...
        c_args = ...
        c_options = ...
        
        # Call C++
        cdef vector[CObjectReference] refs
        with nogil:
            refs = self.c_pool_manager.get().SubmitTaskToPool(
                self.c_pool_id, c_function, c_args, c_options)
        
        # Convert back to Python ObjectRefs
        return [ObjectRef(ref.object_id()) for ref in refs]
    
    def map(self, method_name, items, key_fn=None):
        """Map over items."""
        refs = []
        for item in items:
            key = key_fn(item) if key_fn else None
            ref = self.submit(method_name, item, key=key)
            refs.append(ref)
        return refs
    
    def stats(self):
        """Get pool statistics."""
        cdef CPoolStats c_stats
        with nogil:
            c_stats = self.c_pool_manager.get().GetPoolStats(self.c_pool_id)
        
        return {
            "total_tasks_submitted": c_stats.total_tasks_submitted,
            "total_tasks_failed": c_stats.total_tasks_failed,
            "total_tasks_retried": c_stats.total_tasks_retried,
            "num_actors": c_stats.num_actors,
            "backlog_size": c_stats.backlog_size,
        }
    
    def shutdown(self, force=False, grace_period_s=30):
        """Shutdown pool."""
        # Kill actors, cleanup C++ pool
        pass
```

### Python User API

```python
# python/ray/experimental/actor_pool.py

class ActorPool:
    """Ray Core Actor Pool (C++-backed)."""
    
    def __init__(
        self,
        actor_cls: Type,
        size: Optional[int] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        initial_size: Optional[int] = None,
        actor_args: Tuple = (),
        actor_kwargs: Dict = {},
        actor_options: Dict[str, Any] = {},
        retry: Optional[RetryPolicy] = None,
        ordering: OrderingMode = OrderingMode.UNORDERED,
        # Phase 2
        shape: Optional[Tuple[int, ...]] = None,
        shape_names: Optional[Tuple[str, ...]] = None,
        placement_group: Optional[PlacementGroup] = None,
        **kwargs
    ):
        # Convert Python config to C++ ActorPoolConfig
        config = self._build_config(
            retry=retry,
            ordering=ordering,
            min_size=min_size or size or 1,
            max_size=max_size or size or -1,
            initial_size=initial_size or size or min_size or 1,
            shape=shape,
            shape_names=shape_names,
        )
        
        # Register pool in C++
        self._handle = ray._raylet.register_actor_pool(config)
        
        # Create initial actors
        self._actor_cls = ray.remote(**actor_options)(actor_cls)
        for _ in range(config.initial_size):
            actor = self._actor_cls.remote(*actor_args, **actor_kwargs)
            self._handle.add_actor(actor)
    
    def submit(self, method_name: str, *args, key: Optional[str] = None, **kwargs):
        """Submit task to pool (C++ picks actor)."""
        return self._handle.submit(method_name, *args, key=key, **kwargs)
    
    def map(self, method_name: str, items, key_fn=None):
        """Map over items."""
        return self._handle.map(method_name, items, key_fn=key_fn)
    
    def stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return self._handle.stats()
    
    @property
    def actors(self) -> List[ActorHandle]:
        """Direct access to actor handles."""
        return self._handle.get_actors()
    
    def scale(self, delta: int):
        """Scale pool by delta actors."""
        if delta > 0:
            for _ in range(delta):
                actor = self._actor_cls.remote(*self._actor_args, **self._actor_kwargs)
                self._handle.add_actor(actor)
        elif delta < 0:
            self._handle.remove_actors(abs(delta))
    
    def shutdown(self, force=False, grace_period_s=30):
        """Shutdown pool."""
        self._handle.shutdown(force=force, grace_period_s=grace_period_s)
```

---

## Revised Phased Rollout Plan

### Phase 0: Comprehensive RFC (3-4 weeks)

- **Unified API spec** covering both task queue and SPMD mesh use cases
- **C++ architecture design**:
  - ActorPoolManager class hierarchy
  - PoolWorkQueue interface
  - Task submission & retry flow
  - Integration points with existing ActorTaskSubmitter, ActorManager
- **Protobuf schema changes**:
  - Add `actor_pool_id` to TaskSpec
  - Add `actor_pool_work_item_id` for tracking retries
- **Python bindings spec** (Cython interface)
- Review with Ray Data, Ray Serve, Ray Core teams

### Phase 1: C++ MVP for Ray Data (6-8 weeks)

**Goal**: C++ ActorPoolManager with Python bindings, replace Ray Data internal pool

**C++ Implementation**:

- `ActorPoolManager` class in `src/ray/core_worker/`
- `ActorPoolID` type in `src/ray/common/id.h`
- `UnorderedPoolWorkQueue` implementation
- Cross-actor retry logic in `OnTaskFailed()`
- Load balancing via `SelectActorFromPool()`
- Integration with existing `ActorTaskSubmitter`, `ActorManager`

**Python Bindings**:

- Cython bindings in `python/ray/_raylet.pyx`
- Python `ActorPool` class in `python/ray/experimental/actor_pool.py`

**Features**:

- Pool registration & management
- Task submission to pool (C++ picks actor)
- Cross-actor retry (pool-level, not actor-bound)
- Unordered execution
- Locality-aware scheduling
- Basic autoscaling (min/max/initial size)

**Integration**:

- Ray Data uses C++ ActorPool behind `RAY_DATA_USE_CORE_ACTOR_POOL` flag
- Parity tests
- Benchmarks (should be faster than Python-only due to C++ scheduling)

**Success criteria**:

- Ray Data workload with actor failures → work redistributes in C++
- 5-10% performance improvement vs current Data implementation (C++ overhead lower than Python)

### Phase 2: Topology & Placement Groups (4-6 weeks)

**Goal**: GPU SPMD workloads support

**C++ additions**:

- Actor topology support in `ActorPoolConfig`:
  - `shape`, `shape_names` fields
  - Rank calculation and exposure
- Placement group integration
- `PerKeyOrderedPoolWorkQueue` implementation
- Per-key ordering enforcement in `SelectActorFromPool()`

**Python API additions**:

- `actors_with_ranks()` method
- `shape`, `shape_names` properties
- `placement_group` parameter

**Use cases enabled**:

- vLLM DP+TP pools in Ray Data
- Ray Train worker groups with ranks

### Phase 3: Advanced Patterns & Serve Integration (4-6 weeks)

**Goal**: Ray Serve adoption + SPMD helpers

**C++ additions**:

- Dispatch/collect support (optional, for data sharding)
- Broadcast operations
- Dashboard metrics export

**Python API additions**:

- `broadcast()`, `run_all()` methods
- `run_sharded()` with dispatch/collect functions

**Integration**:

- Ray Serve uses C++ ActorPool for replica management
- Dashboard shows pool-level metrics

### Phase 4: Public API & Ecosystem (3-4 weeks)

**Goal**: Promote to public, deprecate old APIs

- Move to `ray.actor_pool` (public namespace)
- Deprecate `ray.util.ActorPool` and `ray.util.ActorGroup`
- Comprehensive documentation
- Blog post
- External validation

---

## Success Criteria

1. **Phase 1**: Ray Data migrates to C++ ActorPool with performance improvement
2. **Phase 2**: GPU workloads can use topology features
3. **Phase 3**: Ray Serve adopts C++ pool
4. **Phase 4**: External teams adopt unified API
5. **Overall**: Single C++-backed Core API replaces all fragmented implementations

---

## Key Design Decisions

1. ✅ **C++ implementation from Phase 1** - Not pure Python
2. ✅ **Python ergonomic API** - Thin wrapper over C++
3. ✅ **Leverage existing C++ infrastructure** - ActorTaskSubmitter, ActorManager, ClientQueue
4. ✅ **Pool-level retry in C++** - Intercept failures, re-enqueue, submit to different actor
5. ✅ **Unified API** - Covers both task queue and SPMD mesh use cases
6. ✅ **Comprehensive design, incremental implementation** - RFC covers full vision

---

## Implementation TODOs

### Phase 1 (C++ MVP)

1. Add `ActorPoolID` type to `src/ray/common/id.h`
2. Create `ActorPoolManager` class in `src/ray/core_worker/actor_pool_manager.{h,cc}`
3. Create `PoolWorkQueue` interface and `UnorderedPoolWorkQueue` in `src/ray/core_worker/actor_pool_work_queue.{h,cc}`
4. Extend `TaskSpec` protobuf to include `actor_pool_id`
5. Implement pool registration, task submission, retry logic
6. Add Cython bindings in `python/ray/_raylet.pyx`
7. Create Python `ActorPool` class in `python/ray/experimental/actor_pool.py`
8. Unit tests (C++ and Python)
9. Ray Data integration behind flag
10. Benchmarks & parity tests

---

## Related Links

- ActorMesh PRD: `ActorMesh PRD.md`
- Ray Data ActorPoolMapOperator: `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`
- Ray Serve PowerOfTwoChoicesRouter: `python/ray/serve/_private/request_router/pow_2_router.py`
- AutoscalingActorPool interface: `python/ray/data/_internal/actor_autoscaler/autoscaling_actor_pool.py`

