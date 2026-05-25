# RFC: Scaling the Ray Data StreamingExecutor scheduling thread

## Status

Draft. Design proposal for review before code.

## TL;DR

The StreamingExecutor's scheduling thread has two structurally similar
bottlenecks that both stem from the same anti-pattern: **the scheduler
asks every operator the same question every iteration, instead of letting
the relevant code path report what changed.**

| subsystem      | today's anti-pattern                                                                                                                            | this RFC                                                                                                            |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Data path**  | `on_data_ready` issues `ray.get(meta_ref)` once *per ready pair* across *every* ready task; N small RPCs per scheduler iteration                | Drain each ready task's gen up to its budget — looking up `size_bytes` inline via the Ray Core primitive (no RPC) so we stop pulling at the budget — then issue **one** batched `ray.get(meta_refs)` over what was drained |
| **Control path** | `update_usages` re-derives every op's resource bundle from scratch *on every dispatch* (~20 k calls per iteration at 5000 workers) | Ops push deltas at mutation sites (emit / dispatch / done) into a single live global aggregate on `ResourceManager`; per-op re-summation is gone, and the `_update_allocated_budgets` algorithm is preserved but runs on scalar-array workspace + cached invariants — **exact equivalence, ~10× cheaper per call** |

On the wide-schema `worker_scaling` release-test matrix (m5.2xlarge × {36,
72, 143, 358} nodes for {500, 1000, 2000, 5000} workers), the two parts
together are projected to cut max scheduling-loop time by ~40-50 % at the
1000-2000 worker scale and ~25-30 % at 5000 workers, where the dispatch
side dominates.

Both parts are independent. Either can land alone. They're presented
together because they share the same architectural shift (poll → push,
batch where you can't push) and the same primary metric.

## Motivation

A representative scheduler-thread breakdown today, captured by py-spy on the
2000_tasks variant of the wide-schema `worker_scaling` release test (600
columns, 143 m5.2xlarge nodes, `num_cpus=0.5`):

| frame (inclusive, of ~198 s scheduler thread) | seconds | share |
| --- | ---: | ---: |
| `_scheduling_loop_step` (total)               | 193 s | 97 % |
| `process_completed_tasks`                     |  96 s | 48 % |
|   └─ `on_data_ready`                          |  87 s | 44 % |
|        └─ `ray.get_objects` (leaf)            |  68 s | 35 % |
| `update_usages` + `_update_allocated_budgets` |  40 s | 20 % |
|   └─ `safe_round` (leaf, inside update_usages)|  17 s |  9 % |
| `dispatch_next_task` (`remote()` + bookkeeping)|  41 s | 21 % |

At 5000 workers (358 nodes) the same shape blows up:

| frame                                         | seconds | share of ~600 s |
| --- | ---: | ---: |
| `process_completed_tasks`                     | 315 s | 52 % |
|   └─ `on_data_ready`                          | 294 s | 49 % |
| `update_usages` + `_update_allocated_budgets` | 110 s | 18 % |
| `dispatch_next_task`                          | 120 s | 20 % |

Both `on_data_ready` and `update_usages` are doing redundant work that
scales with workload size, not with how much state actually changed.
This RFC removes that redundancy.

## Background: when each scheduler hook runs

The scheduling thread runs `_scheduling_loop_step` in a tight loop. Each
iteration:

```
_scheduling_loop_step()                                  [streaming_executor.py:460]
   │
   ├── update_usages()                                   ← [1] PRE-PROCESS recompute
   │
   ├── process_completed_tasks()                         [streaming_executor_state.py]
   │      │
   │      ├── ray.wait(active_task_refs) → ready set
   │      │
   │      └── for each ready DataOpTask:
   │             task.on_data_ready(budget)              ← [A] PULLS pairs + ray.get + EMITS
   │
   ├── update_usages()                                   ← [2] POST-PROCESS recompute
   │
   └── while True:                                       ← dispatch phase
          op = select_operator_to_run(...)
          if op is None: break
          topology[op].dispatch_next_task()              ← [B] SUBMITS a remote task
          update_usages()                                ← [3] PER-DISPATCH recompute (HOT)
```

### `on_data_ready` — site [A]

```python
# inside DataOpTask.on_data_ready(max_bytes_to_read)
while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
    # 1. Pull block_ref from streaming gen (timeout=0).
    # 2. Pull meta_ref from streaming gen (small timeout).
    # 3. ray.get(meta_ref, timeout=METADATA_GET_TIMEOUT_S)         ← ONE RPC per pair
    # 4. pickle.loads → BlockMetadataWithSchema
    # 5. Emit RefBundle via _output_ready_callback
    # 6. bytes_read += meta.size_bytes
```

- **Trigger:** the task's streaming generator returned ready from `ray.wait`.
- **Frequency:** once per iteration per task whose gen surfaced new outputs.
  Each call may emit multiple RefBundles bounded by `max_bytes_to_read`.
- **State mutated:** the op's output queue; the task's `_pending_block_ref` /
  `_pending_meta_ref` slots.
- **Cost driver:** **one `ray.get` per pair**. With ~1-2 ms fixed Python +
  Cython + raylet RPC overhead per call, an iteration that ingests M pairs
  across all tasks pays M × ~1.5 ms. At 5000 workers × ~10 blocks each =
  ~50 k pairs across the run, this is the largest single cost on the
  thread.

### `dispatch_next_task` — site [B]

- **Trigger:** `select_operator_to_run` chose this op; its budget &
  backpressure cleared.
- **Frequency:** N times per iteration where N = ops dispatched before
  `select_operator_to_run` returns `None`. Typically tens to hundreds.
- **State mutated:** op's in-flight task list, pending counter, resource
  bundle reservation.
- **Resource impact:** **dispatching adds the task's resource bundle to the
  op's `running_logical_usage()`**. Observable today only via the
  per-dispatch `update_usages` poll [3] that follows.

### `update_usages` — sites [1], [2], [3]

Frequency: **2 + dispatch_count per scheduler iteration**. On the
5000_tasks workload that's ~20 002 calls per scheduler tick.

```python
def update_usages(self):
    # Wipe per-op + global usage caches…
    self._global_usage = ExecutionResources(0, 0, 0)
    self._op_usages.clear()
    …
    for op, state in reversed(self._topology.items()):
        op_usage         = op.current_logical_usage()       # ExecutionResources()
        op_running_usage = op.running_logical_usage()       # ExecutionResources()
        op_pending_usage = op.pending_logical_usage()       # ExecutionResources()
        used_obj_store   = self._estimate_object_store_memory_usage(op, state)
        op_usage         = op_usage.copy(object_store_memory=used_obj_store)
        op_running_usage = op_running_usage.copy(...)
        self._global_usage         = self._global_usage.add(op_usage)
        self._global_running_usage = self._global_running_usage.add(op_running_usage)
        self._global_pending_usage = self._global_pending_usage.add(op_pending_usage)
        op._metrics.obj_store_mem_used = op_usage.object_store_memory
    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

Every `ExecutionResources` constructor calls `safe_round` 4× (`cpu`, `gpu`,
`object_store_memory`, `memory`). At ~5 constructions per op × 3 ops × 20 k
dispatch-loop iterations = **~1.2 M `safe_round` calls per scheduler
iteration**. This matches the observed ~17 s of leaf `safe_round` time and
the ~40-110 s inclusive cost of `update_usages` depending on scale.

There's a TODO from the original author acknowledging the redundancy:

```python
# TODO(hchen): This method will be called frequently during the execution loop.
# And some computations are redundant. We should either remove redundant
# computations or remove this method entirely and compute usages on demand.
```

The redundancy:

- Call [1] before `process_completed_tasks` and call [3] from the previous
  iteration's last dispatch sandwich the period when *nothing mutates op
  state* — call [1] is always a no-op repeat of the prior [3].
- Each call of type [3] re-derives **every** op's slot when only *one* op's
  slot — the one just dispatched — actually changed. With ~3 ops in the
  topology, that's a 3× over-fetch per dispatch.
- `on_data_ready` already knows by how many bytes the op's output queue
  grew (`meta.size_bytes`), but doesn't tell anyone — the next
  `update_usages` re-walks the output queue to find out.

## Part 1: Data path — batch the metadata `ray.get`, size-aware

### Design

Restructure `process_completed_tasks` so each scheduler iteration
issues **one** batched `ray.get(meta_refs, ...)` instead of N per-pair
calls. The drain itself is budget-aware: as each `block_ref` is pulled
from the gen, look up its `size_bytes` via the Ray Core primitive
inline and stop pulling when the running sum exceeds the task's
budget. The batched `ray.get` then covers exactly what was drained —
no speculative over-fetch, no separate trim pass, no cross-iteration
cache state.

```
process_completed_tasks(topology, ...)
   │
   ├── ray.wait(active_task_refs) → ready set
   │
   ├── # 1. Drain each ready task's gen up to its per-task budget.
   │   #    For each pulled block_ref, look up size_bytes inline,
   │   #    queue (block_ref, size_bytes, meta_ref) on the task,
   │   #    and collect meta_ref into to_fetch.
   │   to_fetch = []
   │   for task in ready_tasks:
   │       bytes_read = 0
   │       while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
   │           block_ref  = gen._next_sync(timeout=0)
   │           if block_ref.is_nil(): break              # gen not ready
   │           size_bytes = ray.experimental.get_object_sizes([block_ref])[0]
   │           meta_ref   = gen._next_sync(timeout=METADATA_WAIT_TIMEOUT_S)
   │           task._pending.append((block_ref, size_bytes, meta_ref))
   │           to_fetch.append(meta_ref)
   │           bytes_read += size_bytes
   │
   ├── # 2. ONE batched ray.get covering only what was drained.
   │   bytes_by_meta_ref = dict(zip(to_fetch, ray.get(to_fetch, timeout=…)))
   │
   └── # 3. Each task pops queue pairs in order and emits.
       for task in ready_tasks:
           task.on_data_ready(budget, bytes_by_meta_ref)
```

`RefBundle` and the streaming generator protocol are **unchanged**.
Every site that reads `bundle.metadata.X` (size_bytes, num_rows,
exec_stats, task_exec_stats, input_files) keeps working unmodified.

### 1a. `DataOpTask` queues `(block_ref, size_bytes, meta_ref)` triples

Replace the single `_pending_block_ref` / `_pending_meta_ref` scalar
slots with a `Deque[(block_ref, size_bytes, meta_ref)]` queue, plus a
`_partial_block_ref` stash for the half-pulled-pair case (the gen
yields block then metadata; they can arrive in separate scheduler
ticks).

```python
class DataOpTask:
    _pending: Deque[Tuple[ObjectRef[Block], int, ObjectRef[BlockMetadata]]]
    _partial_block_ref: ObjectRef[Block]
    _gen_exhausted: bool
    _gen_errored_block: Optional[ObjectRef]

    def drain_up_to(self, max_bytes_to_read) -> List[ObjectRef[BlockMetadata]]:
        """Pull (block_ref, meta_ref) pairs from the gen until either
        the gen has no more ready output or the running sum of
        size_bytes (from the Ray Core primitive) exceeds
        max_bytes_to_read. Each pulled triple is appended to _pending.
        Returns the meta_refs added this iteration so the caller can
        collect them into a batched ray.get list."""
        ...

    def on_data_ready(self, max_bytes_to_read, bytes_by_meta_ref=None):
        """Pop queued triples in order, look up the deserialized
        metadata in bytes_by_meta_ref, emit RefBundles. Per-ref
        ray.get fallback on cache miss."""
        ...
```

Why a queue rather than a scalar: pulling from the generator is
**destructive** — once we pull, the ref is gone from the stream.
Triples that don't get emitted this iteration (because the batched
`ray.get` timed out, or because the per-op budget in `on_data_ready`
emits only a prefix) must persist on the task. Per-iteration state
lives in the caller-supplied `bytes_by_meta_ref` dict.

### 1b. Assumed dependency: cheap per-ref `size_bytes` from Ray Core

The design depends on a Ray Core primitive that returns each ref's
byte size **without** issuing an RPC or fetching the object data, and
that is **reliably populated** by the time the streaming generator
surfaces the ref to the driver:

```python
sizes = ray.experimental.get_object_sizes(block_refs)   # List[int], one per ref
```

How Ray Core exposes this — whether via a focused helper, an extension
to an existing API, an inlined field on `ObjectRef`, or something else
— is a question for the Ray Core team and out of scope for this RFC.
This RFC assumes the primitive exists and is reliable for streaming-gen
owned refs; see the open questions section.

The size lets us **trim the metadata fetch up front**, so what we
`ray.get` is exactly what we emit this iteration. No cross-iteration
cache, no speculative over-fetch.

### Per-call `ray.get` fallback

`on_data_ready` keeps a per-ref `ray.get(meta_ref)` + `GetTimeoutError`
warning as a cache-miss fallback. The fallback runs when:

1. The batched `ray.get` raised `GetTimeoutError` and no bytes were
   cached for the iteration — falls through to per-ref so the worker
   crash / node preemption surfaces with the standard warning and
   retry-next-iteration semantics.
2. `on_data_ready` was called outside `process_completed_tasks` (tests,
   debugging) with no `bytes_by_meta_ref`.

A convenience `fetch_and_emit(max_bytes_to_read)` wraps `drain_up_to`
+ per-ref `ray.get` + `on_data_ready` for direct callers (tests).

### Expected impact (data path)

Prototype measurements on the wide-schema `worker_scaling` matrix:

| variant     | master | this RFC (data path only) | reduction | speedup |
| ----------- | -----: | --: | --: | --: |
| 500_actors  |  0.89 s|  0.69 s | 22.9 % | 1.30× |
| 500_tasks   |  2.40 s|  1.58 s | 34.3 % | 1.52× |
| 1000_actors |  1.69 s|  1.28 s | 24.6 % | 1.33× |
| **1000_tasks** |  6.58 s|  3.72 s | **43.4 %** | **1.77×** |
| 2000_actors |  3.19 s|  2.33 s | 26.9 % | 1.37× |
| 2000_tasks  | 14.63 s|  8.96 s | 38.8 % | 1.63× |
| 5000_actors |  9.83 s|  8.75 s | 11.0 % | 1.12× |
| 5000_tasks  | 33.61 s| 28.69 s | 14.6 % | 1.17× |

Two observations from the py-spy breakdowns:

- **Wins peak at 1000–2000 workers.** Per-call `ray.get` overhead was
  the binding constraint there; `on_data_ready` inclusive share drops
  from 42–49 % to 4–35 % after batching.
- **Dampens at 5000 workers.** The batched `ray.get(N refs)` fans out
  to all 358 nodes and blocks on the slowest ref; `get_objects` leaf
  time actually *grows* for the tasks variant (164 s → 218 s). The
  dispatch side starts to dominate — Part 2 below.

Cost: the budget-aware drain (`_next_sync` + inline size lookup) adds
2-3 % of scheduler-thread time across all variants. Small price for
the batching infrastructure.

Compared to a per-pair `ray.get` baseline, the size-aware drain also
keeps the per-iteration over-fetch ceiling at ~1 pair past budget
(the running sum can overshoot by one when the next pulled block
crosses the budget). Without size info we'd either over-fetch up to
the entire gen backlog (and need to carry cached bytes across
iterations) or under-utilize the batched call.

## Part 2: Control path — push, don't poll

### Design

Invert the control flow on resource accounting. Each event that
changes resource usage applies the **delta** at the mutation site,
to both the per-op cache and a single **global running aggregate**
on `ResourceManager`. Because the scheduler thread is single-threaded
and every mutation site already knows its delta, threading the delta
to the global is free. `update_usages` then becomes **O(1)** — no
per-op summation, just (optionally) `_update_allocated_budgets`.

#### 2a. New op-level hooks on `PhysicalOperator`

Each hook updates two places:

1. The op's local cache (so per-op readers like
   `current_logical_usage()` and the per-op `obj_store_mem_used`
   metric stay correct).
2. The `ResourceManager`'s global aggregate (so `update_usages`
   doesn't have to re-sum).

**Hook frequency matters.** At 5000 workers a scheduler iteration
fires ~20 000 `on_task_dispatched`, ~20 000 `on_task_completed`, and
hundreds-to-thousands of `on_output_bytes_*` calls. If each hook
builds a new `ExecutionResources` via `add` / `subtract` /
`copy`, the constructor + `safe_round` overhead adds **~2-5 s of
hook cost per iteration** — that would eat most of what we saved on
the allocator. So the cached state is stored as **scalar fields**,
not `ExecutionResources` instances; each hook is 1-4 float adds.
`ExecutionResources` is materialized only when an external caller
reads via a property.

```python
class PhysicalOperator:
    # Cached state as plain scalars. Each hook is 1-4 float adds; no
    # ExecutionResources construction, no safe_round per call.
    _cached_logical_cpu:    float
    _cached_logical_gpu:    float
    _cached_logical_memory: float
    _cached_running_cpu:    float
    _cached_running_gpu:    float
    _cached_running_memory: float
    _cached_pending_cpu:    float
    _cached_pending_gpu:    float
    _cached_pending_memory: float
    _cached_obj_store_mem_bytes: int

    # Bound at op construction so missing the global-update side is a
    # deref error, not a silent skew.
    _resource_manager: ResourceManager

    def on_output_bytes_added(self, n: int) -> None:
        self._cached_obj_store_mem_bytes                 += n
        self._resource_manager._global_obj_store_mem_bytes += n

    def on_output_bytes_consumed(self, n: int) -> None:
        self._cached_obj_store_mem_bytes                 -= n
        self._resource_manager._global_obj_store_mem_bytes -= n

    def on_task_dispatched(self, task_bundle: ExecutionResources) -> None:
        self._cached_running_cpu    += task_bundle.cpu
        self._cached_running_gpu    += task_bundle.gpu
        self._cached_running_memory += task_bundle.memory
        self._cached_pending_cpu    -= task_bundle.cpu
        self._cached_pending_gpu    -= task_bundle.gpu
        self._cached_pending_memory -= task_bundle.memory
        rm = self._resource_manager
        rm._global_running_cpu    += task_bundle.cpu
        rm._global_running_gpu    += task_bundle.gpu
        rm._global_running_memory += task_bundle.memory
        rm._global_pending_cpu    -= task_bundle.cpu
        rm._global_pending_gpu    -= task_bundle.gpu
        rm._global_pending_memory -= task_bundle.memory

    def on_task_completed(self, task_bundle: ExecutionResources) -> None:
        self._cached_running_cpu    -= task_bundle.cpu
        self._cached_running_gpu    -= task_bundle.gpu
        self._cached_running_memory -= task_bundle.memory
        rm = self._resource_manager
        rm._global_running_cpu    -= task_bundle.cpu
        rm._global_running_gpu    -= task_bundle.gpu
        rm._global_running_memory -= task_bundle.memory

    # Readers materialize ExecutionResources on demand. Constructor +
    # safe_round runs at most once per get_*_usage() call, not on
    # every hook firing.
    def current_logical_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._cached_logical_cpu,
            gpu=self._cached_logical_gpu,
            memory=self._cached_logical_memory,
        )
```

#### 2b. `ResourceManager` keeps the global aggregate live

```python
class ResourceManager:
    # Plain scalar fields, mirroring per-op storage. Updated by hooks.
    _global_running_cpu:           float
    _global_running_gpu:           float
    _global_running_memory:        float
    _global_pending_cpu:           float
    _global_pending_gpu:           float
    _global_pending_memory:        float
    _global_obj_store_mem_bytes:   int

    def get_global_running_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._global_running_cpu,
            gpu=self._global_running_gpu,
            memory=self._global_running_memory,
            object_store_memory=self._global_obj_store_mem_bytes,
        )

    def update_usages(self) -> None:
        # Aggregates are already current. The allocator's
        # _update_allocated_budgets still runs per dispatch (see 2c)
        # but now consumes the live scalar aggregate instead of
        # recomputing it.
        if self._op_resource_allocator is not None:
            self._update_allocated_budgets()
```

The per-op summation (the ~80 s of "construct N×K `ExecutionResources`
+ call `safe_round` 4× each" in today's profile) is gone. So is the
matching cost in the hooks themselves — even though they're now
called ~40 000 times per iteration at 5000 workers, the cumulative
cost of all hook firings is **~50–100 ms**. What remains is
`_update_allocated_budgets` itself, which we make cheap in 2c.

Safety note: this is sound because the scheduler thread is
single-threaded — every mutation site (`on_data_ready`,
`dispatch_next_task`, `task_done_callback`, output-queue pop) runs on
the scheduler thread or is funnelled through it. No locking required
on the global aggregate.

#### 2c. Make `_update_allocated_budgets` cheap per call (exact equivalence)

Today's `update_budgets` is ~1.5 ms per call × 20 000 calls/iter ≈
30 s at 5000 workers. Per-call work:

1. Two passes over `eligible_ops` (reserved-portion, then shared-pool
   distribution with borrow / cap logic).
2. Many `ExecutionResources` constructor calls — each invokes
   `safe_round` 4× on cpu / gpu / memory / object_store_memory.
3. Calls to `subtract`, `add`, `copy`, `scale`, `min`, `max` — each
   builds a new `ExecutionResources` instance.

We keep the **algorithm exactly as today** (same fair-share
distribution, same borrow / cap, same final `_op_budgets`) and just
shrink the constant factor:

- **Inputs that don't change per dispatch get cached.** `_total_shared`,
  `_op_reserved`, `eligible_ops`, `_get_completed_ops_usage()`,
  `get_global_limits()`. `eligible_ops` is invalidated when an op
  finishes; `_op_reserved` is invalidated when `_update_reservation`
  runs (i.e., when `limits` changes — every 10 s). The hot path reads
  the cached values directly, skipping the recompute.
- **Per-op work runs on scalar fields, not `ExecutionResources`
  instances.** The allocator keeps `cpu / gpu / memory / object_store`
  as parallel float arrays indexed by op-id. The arithmetic in the two
  passes becomes plain `float` math — no constructor allocation, no
  `safe_round` (rounding is deferred until a caller materializes an
  `ExecutionResources` via `get_budget(op)`).
- **`_global_running_usage` is read from the live aggregate** (Part
  2b) rather than re-summed from per-op caches.

Net effect: same `_op_budgets` after every dispatch as today —
bit-identical given the same inputs — but ~10× cheaper per call.

```python
class ReservationOpResourceAllocator:
    # Cached invariants (refreshed when their inputs change).
    _total_shared_cpu: float
    _total_shared_obj_store: float
    ...
    _op_reserved_cpu: List[float]            # parallel to _eligible_ops
    _op_reserved_obj_store: List[float]
    _eligible_ops: List[PhysicalOperator]

    # Per-call workspace (no allocation).
    _op_budget_cpu: List[float]
    _op_budget_obj_store: List[float]

    def update_budgets(self, *, limits: ExecutionResources) -> None:
        # Same two-pass algorithm as today, on scalar arrays. No
        # ExecutionResources constructors in the hot path.
        ...
```

Projected: ~30 s → ~2-3 s at 5000 workers, with exact equivalence to
today's per-dispatch budget values.

#### 2d. Call-site changes

```python
# DataOpTask.on_data_ready, after emitting each RefBundle:
self._output_ready_callback(RefBundle(...))
self._op.on_output_bytes_added(meta.size_bytes)               # new

# OpState.dispatch_next_task, after submitting:
op.submit_task(task)
op.on_task_dispatched(task.task_resource_bundle)              # new (per-op + global delta)

# DataOpTask.task_done_callback wrapper:
op.on_task_completed(task.task_resource_bundle)               # new

# downstream op popping from input queue:
bundle = input_op.output_queue.pop()
input_op.on_output_bytes_consumed(bundle.size_bytes)          # new
```

The two functions `on_data_ready` and `update_usages` stop being
two functions: emitting a RefBundle *is* updating resource usage,
just observed from different sides.

### Expected impact (control path)

| frame (at 5000_tasks)                    | today  | projected | reasoning |
| ---                                      | ---:   | ---:      | --- |
| `update_usages` (inclusive, excluding allocator) |  ~80 s |  <1 s | per-op summation gone; live global aggregate read directly |
| `_update_allocated_budgets`              |  ~30 s |  ~2-3 s   | same algorithm, scalar-array workspace + cached invariants — no per-call `ExecutionResources` construction |
| `safe_round` (leaf, both)                |  ~46 s |  <3 s     | only when callers materialize `ExecutionResources` via `get_*_usage()`, not on every hook firing |
| **new** hook overhead (~40 k calls/iter) |   —    |  ~50-100 ms | scalar-field deltas, no `ExecutionResources` constructor in the hot path |

Projected scheduler-thread reduction at 5000 workers: **~140 s → ~5 s
on the `update_usages` + `_update_allocated_budgets` family**, ~22 %
of the 600 s total at that scale, with the new hook overhead adding
**<100 ms** on top. The wins compound with N_ops *and* with dispatch
count per iteration — at higher op-count or higher dispatch-density
topologies they grow.

**Equivalence guarantee:** `_op_budgets` after every dispatch is
bit-identical to today's values given the same inputs. The
optimization is purely a constant-factor reduction (skip redundant
re-summation, use scalar fields instead of `ExecutionResources`
constructors); the fair-share algorithm, the borrow / cap logic, and
the per-dispatch call cadence are all preserved.

**Why the hooks don't undo the win:** every dispatch / completion /
emit / pop fires a hook (~40 000 calls per iteration at 5000
workers), so a naive implementation that built a fresh
`ExecutionResources` per call would add ~2-5 s of hook overhead and
eat most of the savings. Storing the cached state as scalar floats
keeps each hook at 1-4 plain adds (<1 µs); the cumulative cost
across all hook firings is well under 100 ms. `ExecutionResources`
is materialized only at the boundary where a caller reads via
`get_*_usage()` — at most a handful of times per iteration.

## Combined effect

Each part attacks a different cost; they compose.

| 5000_tasks scheduler thread | today | + Part 1 | + Parts 1 & 2 |
| --- | ---: | ---: | ---: |
| `process_completed_tasks`            | 315 s |  ~280 s | ~280 s   |
| `update_usages` + `_update_allocated_budgets` | 140 s | ~140 s |  ~5 s   |
| `dispatch_next_task`                 | 120 s |  ~120 s | ~120 s   |
| **Total**                            | ~600 s| ~580 s | **~440 s** |

The data-path win is concentrated in mid-range scale (1000-2000
workers, where `on_data_ready` per-call `ray.get` overhead
dominates); the control-path win is concentrated at the high end
(5000+ workers, where `update_usages` dominates). Together they
cover the whole scaling range.

For `max_scheduling_loop` (the metric users actually feel), prototype
data-path measurements show 1.3-1.77× reduction at 500-2000 workers,
dampening to 1.12-1.17× at 5000. Adding the control-path refactor
should restore the win at 5000 — projected combined 1.3-1.5× on the
tasks variant and 1.4× on the actors variant.

## Scope

### Part 1: data path (~500 LoC)

| file | changes |
| --- | --- |
| Ray Core API surface for per-ref `size_bytes` (exact shape TBD with Core team) | exposes the cheap, reliable size lookup that the budget-aware trim depends on |
| `data/_internal/execution/interfaces/physical_operator.py` | `DataOpTask` switches from scalar `_pending_*_ref` slots to a `Deque[(block_ref, size_bytes, meta_ref)]` queue + partial-block stash; new `drain_up_to(max_bytes_to_read)` pulls inline with the size lookup; `on_data_ready` takes `bytes_by_meta_ref` dict; per-ref `ray.get` fallback for cache miss |
| `data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks` runs the 3-step flow above (budget-aware drain with inline size lookup → batched `ray.get` → emit) |
| `data/tests/test_streaming_executor.py` + `tests/util.py` | tests updated to new `on_data_ready` signature; new tests for budget-aware drain + cache-miss fallback |

`RefBundle` layout and the streaming-generator protocol are
**unchanged**. The 68 production sites that read
`bundle.metadata.*` keep working as-is.

### Part 2: control path (~300-600 LoC)

| file | changes |
| --- | --- |
| `data/_internal/execution/resource_manager.py` | `update_usages` becomes aggregator-only; per-op poll loop removed |
| `data/_internal/execution/interfaces/physical_operator.py` | 4 new hooks (`on_output_bytes_added` / `consumed`, `on_task_dispatched` / `completed`); 4 new cached fields; readers (`current_logical_usage` etc.) become trivial properties over the cache |
| `data/_internal/execution/streaming_executor.py` | dispatch loop calls `op.on_task_dispatched(...)` after `dispatch_next_task` |
| `data/_internal/execution/interfaces/physical_operator.py` (DataOpTask) | `on_data_ready` calls `self._op.on_output_bytes_added(meta.size_bytes)` after each emit |
| Op subclasses with custom resource accounting (`MapOperator`, `LimitOperator`, `OutputSplitter`, `ActorPoolMapOperator`, ...) | migrate `current_logical_usage` overrides to push deltas via the hooks |
| Downstream-consume sites (output queue pops, external consumer reads) | call `op.on_output_bytes_consumed(...)` |
| Tests | every mock that replaced `current_logical_usage` updates to push deltas or set cached fields |

## Rollout

Both parts are independent. Suggested order:

1. **Ray Core size primitive first.** Part 1's budget-aware trim
   depends on it; landing it first unblocks the Data-side work.
2. **Part 1 (data path).** Self-contained, measured wins, smaller
   blast radius (touches the data-fetch path, not the resource
   manager).
3. **Part 2 (control path).** Independent of Part 1 and the Core
   primitive. Could land before Part 1 if convenient.

Either Data-side part can ship without the other. If Part 2's
op-subclass migration runs long, Part 1 carries most of the mid-range
wins in production while Part 2 lands incrementally.

## Alternatives considered

### Data path

- **Per-pair `ray.get` with smaller fixed overhead** — would require
  Ray Core changes to the `ray.get` codepath; large blast radius for
  a marginal improvement.
- **Hard cap on batch size** (`MAX_REFS_PER_BATCH=256`) — bounds the
  blast radius of one slow ref, but the trimmed-by-budget design
  already caps the batch at what fits the per-op output budget.

Note: removing the metadata `ray.get` from the scheduler thread
*entirely* is not a viable option. The scheduler thread consumes
fields from `BlockMetadata` beyond `size_bytes` / `num_rows`:

- `streaming_executor_state.py` reads `ref.schema` to validate and
  unify schemas across bundles emitted into an op's output queue.
  `RefBundle.schema` is populated today from the same
  `pickle.loads(BlockMetadataWithSchema)` that produces the
  metadata.
- `DataOpTask.on_data_ready` reads `self._last_block_meta.task_exec_stats`
  on `StopIteration` to pass into `task_done_callback`.
- `OpRuntimeMetrics` reads `meta.exec_stats.node_id` to attribute
  per-node metrics when bundles are emitted on the scheduler thread.

These consumers run on the scheduler thread synchronously with the
emit. They need the materialized `BlockMetadata`, not an opaque ref.
The size-aware batched `ray.get` design keeps all of them working
unchanged.

### Control path

- **Incremental `update_usages_for_op(op)`** — keep the poll structure
  but recompute only the op that just changed at site [3]. ~30-50 LoC
  change; captures ~70 % of the Part-2 win.
  - *Pros:* tiny diff, no API changes, low review risk. Could land as
    a stepping-stone before the full push-model refactor.
  - *Cons:* leaves the architectural mismatch in place. Any future
    caller that mutates op state still has to remember to call
    `update_usages_for_op` — same trap, slightly cheaper. Doesn't
    address per-op `ExecutionResources` constructor cost.
- **Inline / fast-path `safe_round`** — skip `safe_round` when input is
  already canonicalized. ~10 LoC. Cuts `safe_round` leaf cost by ~80 %.
  - *Pros:* trivially small.
  - *Cons:* addresses symptom not cause; we still construct millions of
    `ExecutionResources` per iteration that aren't semantically new.
- **Memoize `ExecutionResources` by tuple key** — return the same
  instance for the same args.
  - *Pros:* works without API changes.
  - *Cons:* dict-lookup overhead approaches per-construction cost;
    cache bookkeeping becomes its own footgun.

## Risks

### Part 1
1. **Dependence on the Ray Core size primitive being reliable.** If
   the assumed cheap `size_bytes` lookup ever returns stale or missing
   values for streaming-gen owned refs, the budget-aware trim
   degrades: we either over-fetch (treat unknown size as 0 and pull
   everything) or under-fetch (treat unknown as ∞ and emit nothing).
   Mitigation: see "Open questions" — Core team to specify the
   guaranteed-reliable surface; the per-ref `ray.get(meta_ref)`
   fallback in `on_data_ready` recovers `size_bytes` on cache miss.
2. **One slow ref still blocks the batched call.** The batched
   `ray.get(meta_refs, timeout=…)` blocks on the slowest ref in the
   batch; at 5000 workers measurements show the leaf `get_objects`
   time *growing* (164 s → 218 s for the tasks variant). Mitigation:
   the budget trim caps the batch at "what fits the per-op output
   budget", typically much smaller than the gen backlog; pairs that
   don't make it persist on the task queue and are retried next
   iteration. `GetTimeoutError` falls through to per-ref `ray.get`
   with the existing warning + retry-next-iteration semantics.
3. **Queue state on `DataOpTask`.** The per-task pending-pairs deque
   adds state that must be drained correctly on task completion /
   error / cancellation. Mitigation: drain in `task_done_callback`;
   covered by new tests for budget-aware drain + cache-miss fallback.

### Part 2
1. **Cache + global-aggregate drift.** Any future code path that
   mutates op state without going through a hook silently corrupts
   both the per-op cache *and* the global aggregate. The global makes
   the drift especially nasty because a single missed delta poisons
   `_update_allocated_budgets` for the rest of the dataset.
   Mitigation: (a) keep `current_logical_usage` etc. as canonical
   readers backed by the cache, so anyone reading sees the cached
   value (no temptation to compute from raw state); (b) add a
   debug-mode invariant check that periodically recomputes the
   global from scratch and asserts equality; (c) bind the
   `ResourceManager` reference at op-construction so missing the
   global-update side is a `None` deref, not a silent skew.
2. **Op subclass migration.** Each subclass override needs auditing.
   Mitigation: do one op at a time; the cached field defaults to a
   freshly-recomputed value if the subclass hasn't migrated yet
   (gradual rollout).
3. **External consumers.** `iter_batches` / `streaming_split` pop bytes
   from the output queue outside the scheduler thread. Need to either
   fire the `on_output_bytes_consumed` hook from those paths or model
   "externally-consumed bytes" separately. Either way, an explicit
   decision rather than today's implicit recomputation. Because the
   global aggregate is unlocked (single-thread assumption), any
   off-scheduler-thread mutation must funnel through a thread-safe
   queue rather than calling the hook directly.

## Open questions

- **For the Ray Core team:** what is the right Core-side surface for
  cheap, no-RPC, reliably-populated per-ref `size_bytes`? Part 1
  assumes such a primitive exists and is reliable for streaming-gen
  owned refs at the moment the driver receives the ref. The Python
  entry point and the underlying implementation are both Core-team
  decisions; this RFC assumes the result.
- **Batch `ray.get` timeout policy.** Today's per-pair call uses
  `METADATA_GET_TIMEOUT_S`. For the batched call we need to pick:
  a single shared timeout for the whole batch (simpler, but one slow
  ref starves everything else), or partition the batch by op and use
  a per-op timeout. Probably the former with the per-ref fallback
  catching pathological cases.
- Should `_cached_logical_usage` be a single `ExecutionResources` or
  four separate scalar fields (cpu / gpu / memory / obj_store)?
  Constructors are the bottleneck, so probably scalars;
  `ExecutionResources` materialized on read.
- Does `_update_allocated_budgets` benefit from the cached values too,
  or is it a separate cost worth profiling independently?
- Backwards compatibility: maintain old method signatures
  (`current_logical_usage()` etc.) indefinitely as cached-value
  readers, or deprecate them?

## Out of scope

- Changes to `_update_allocated_budgets` budget-allocation algorithm
  itself.
- Changes to `ray.wait` / `ray.get` Ray Core primitives.
- The other 5000-worker scaling pain (`dispatch_next_task`'s
  `remote()` overhead). Separate follow-up.
- Op-fairness / preemption logic.
