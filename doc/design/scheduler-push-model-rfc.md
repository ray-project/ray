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
| **Data path**  | `on_data_ready` issues `ray.get(meta_ref)` once *per ready pair* across *every* ready task; N small RPCs per scheduler iteration                | Drain all gens first, look up block sizes locally, trim by per-op budget, then issue **one** batched `ray.get`     |
| **Control path** | `update_usages` re-derives every op's resource bundle from scratch *on every dispatch* (~20 k calls per iteration at 5000 workers)             | Ops push resource deltas at mutation sites (emit / dispatch / done); `update_usages` shrinks to an aggregator      |

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

## Part 1: Data path — batch the per-pair `ray.get`

### Design

Restructure `process_completed_tasks` so that each scheduler iteration
issues **one** batched `ray.get` covering exactly the metadata for the
pairs that will be emitted, then hands the bytes to each task for emit.

```
process_completed_tasks(topology, ...)
   │
   ├── ray.wait(active_task_refs) → ready set
   │
   ├── # 1. Drain each ready task's gen into its pending-pairs queue.
   │   for task in ready_tasks:
   │       while task.peek_pending_pair() is not None: pass
   │
   ├── # 2. Look up each queued block's size (local, no RPC).
   │   sizes = ray.experimental.get_object_sizes(all_pending_block_refs)
   │
   ├── # 3. Per op, trim each task's queued pairs by remaining budget.
   │   #    Collect the prefix of meta_refs whose blocks fit.
   │   to_fetch = [...]
   │
   ├── # 4. ONE batched ray.get covering the trimmed union.
   │   bytes_by_meta_ref = dict(zip(to_fetch, ray.get(to_fetch, timeout=…)))
   │
   └── # 5. Each task pops queue pairs in order and emits.
       for task in ready_tasks:
           task.on_data_ready(budget, bytes_by_meta_ref)
```

Two structural changes underneath:

#### 1a. `DataOpTask` switches to a queue-of-pairs

Replace the single `_pending_block_ref` / `_pending_meta_ref` scalar slots
with a `Deque[(block_ref, meta_ref)]` queue, plus a `_partial_block_ref`
stash for the half-pulled-pair case. Add `peek_pending_pair()` as a
drainable method (callable in a loop until it returns `None`).

```python
class DataOpTask:
    _pending_pairs:   Deque[(block_ref, meta_ref)]   # pulled from gen, awaiting emission
    _partial_block_ref: ObjectRef                    # half-pulled state
    _gen_exhausted:   bool
    _gen_errored_block: Optional[ObjectRef]

    def peek_pending_pair(self) -> Optional[Tuple[ObjectRef, ObjectRef]]:
        """Pull one (block_ref, meta_ref) into _pending_pairs; return it.
        Returns None if no full pair is immediately ready. Loop to drain."""
        ...

    def on_data_ready(self, max_bytes_to_read, bytes_by_meta_ref=None):
        """Pop queued pairs in order, look up bytes in the caller-supplied
        dict, emit RefBundles. Per-ref ray.get fallback on cache miss."""
        ...
```

Why a queue rather than a scalar: pulling from the generator is
**destructive** — once we peek, the ref is gone from the stream. Pairs
that don't get emitted this iteration (because they didn't fit the budget,
or the batched `ray.get` timed out) must persist on the task. Per-iteration
state lives only in the caller-supplied `bytes_by_meta_ref` dict.

#### 1b. New `ray.experimental.get_object_sizes(refs)` helper

```python
def get_object_sizes(obj_refs: List[ObjectRef]) -> List[Optional[int]]:
    """Return the byte size of each object as known to Ray Core, or None
    if the size isn't yet known (the producing task hasn't sealed the
    object). Local-only; no RPCs."""
    locations = get_local_object_locations(obj_refs)
    return [(locations.get(r) or {}).get("object_size") for r in obj_refs]
```

Thin wrapper over the existing `get_local_object_locations` (which already
reads `object_size` from the local reference counter — no new RPC, no new
Ray Core state, just a focused Python entry point onto data Ray Core
already maintains for OOM / spill / scheduling decisions).

The size lets us **trim the metadata fetch up front**, so what we
`ray.get` is exactly what we emit this iteration. No cross-iteration
cache, no speculative over-fetch.

#### 1c. Graceful `None` handling

`get_object_sizes` returns `None` for refs whose owner hasn't yet
published a size (small race window between the gen yielding a ref and
the worker's `SealOwnedObject` report reaching the driver). Treat `None`
as "fetch this pair anyway" — the per-pair `ray.get` happens, the budget
is still enforced on the emit side via `bytes_read += meta.size_bytes`.
Same behavior as today for that pair; the precise-trim win is realized
for the pairs where the size is known.

### Per-call ray.get fallback

`on_data_ready` keeps a per-ref `ray.get(meta_ref)` + `GetTimeoutError`
warning as a cache-miss fallback. The fallback runs when:

1. The batched `ray.get` raised `GetTimeoutError` and no bytes were
   cached for the iteration — falls through to per-ref so the worker
   crash / node preemption surfaces with the standard warning and
   retry-next-iteration semantics.
2. `on_data_ready` was called outside `process_completed_tasks` (tests,
   debugging) with no `bytes_by_meta_ref`.

A convenience `fetch_and_drain(max_bytes_to_read)` wraps `peek` +
`on_data_ready` for direct callers (tests).

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
- **Dampens at 5000 workers.** The batched `ray.get(5000 refs)` fans out
  to all 358 nodes and blocks on the slowest ref; `get_objects` leaf time
  actually *grows* for the tasks variant (164 s → 218 s). The dispatch
  side starts to dominate — Part 2 below.

Cost: `peek_pending_pair` adds 2-3 % of scheduler-thread time across all
variants. Small price for the batching infrastructure.

## Part 2: Control path — push, don't poll

### Design

Invert the control flow on resource accounting. Each event that changes
per-op resource usage updates the `ResourceManager`'s cache **at the
mutation site**, by delta. The manager keeps a per-op cache that's always
current. `update_usages` shrinks to an aggregation pass + budget
allocator.

#### 2a. New op-level hooks on `PhysicalOperator`

```python
class PhysicalOperator:
    # Maintained by the hooks; readers (current_logical_usage etc.)
    # become trivial properties over these fields.
    _cached_logical_usage:        ExecutionResources
    _cached_running_usage:        ExecutionResources
    _cached_pending_usage:        ExecutionResources
    _cached_obj_store_mem_bytes:  int

    def on_output_bytes_added(self, n: int) -> None:
        """Called by DataOpTask.on_data_ready after each RefBundle emit."""
        self._cached_obj_store_mem_bytes += n

    def on_output_bytes_consumed(self, n: int) -> None:
        """Called when a downstream op pops bytes from this op's output
        queue, or when an external consumer reads via iter_batches /
        streaming_split."""
        self._cached_obj_store_mem_bytes -= n

    def on_task_dispatched(self, task_bundle: ExecutionResources) -> None:
        """Called by dispatch_next_task after submitting a remote task."""
        self._cached_running_usage = self._cached_running_usage.add(task_bundle)
        self._cached_pending_usage = self._cached_pending_usage.subtract(task_bundle)

    def on_task_completed(self, task_bundle: ExecutionResources) -> None:
        """Called from task_done_callback."""
        self._cached_running_usage = self._cached_running_usage.subtract(task_bundle)
```

#### 2b. `ResourceManager.update_usages` becomes an aggregator

```python
def update_usages(self) -> None:
    # No re-derivation; just sum the cached values that the hooks maintained.
    self._global_usage         = sum_resources(op._cached_logical_usage for op in self._topology)
    self._global_running_usage = sum_resources(op._cached_running_usage for op in self._topology)
    self._global_pending_usage = sum_resources(op._cached_pending_usage for op in self._topology)
    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

Hot path becomes O(N_ops) scalar additions — not O(N_ops × constructors
× `safe_round` calls). `_update_allocated_budgets` still runs (it does
the across-op fair-share work), but it now reads from the cached
aggregates instead of re-deriving them.

#### 2c. Call-site changes

```python
# DataOpTask.on_data_ready, after emitting each RefBundle:
self._output_ready_callback(RefBundle(...))
self._op.on_output_bytes_added(meta.size_bytes)               # new

# OpState.dispatch_next_task, after submitting:
op.submit_task(task)
op.on_task_dispatched(task.task_resource_bundle)              # new

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
| `update_usages` (inclusive)              | ~110 s | ~10-20 s  | drops to "sum N scalars per call" + budget allocator |
| `safe_round` (leaf)                      |  ~46 s |   <5 s    | called only at construction, not in hot path |
| `_update_allocated_budgets`              |  ~30 s | ~20-25 s  | still runs, reads cached values |

Projected scheduler-thread reduction at 5000 workers: **~110 s → ~30 s
update_usages**, ~14 % of the 600 s total at that scale.

## Combined effect

Each part attacks a different cost; they compose.

| 5000_tasks scheduler thread | today | + Part 1 | + Parts 1 & 2 |
| --- | ---: | ---: | ---: |
| `process_completed_tasks`  | 315 s |  ~280 s | ~280 s   |
| `update_usages`            | 110 s |  ~110 s |  ~30 s   |
| `dispatch_next_task`       | 120 s |  ~120 s | ~120 s   |
| **Total**                  | ~600 s| ~580 s | **~500 s** |

The data-path win is concentrated in mid-range scale (1000-2000 workers,
where `on_data_ready` dominates); the control-path win is concentrated
at the high end (5000+ workers, where `update_usages` dominates).
Together they cover the whole scaling range.

For `max_scheduling_loop` (the metric users actually feel), prototype
data-path measurements show 1.3-1.77× reduction at 500-2000 workers,
dampening to 1.12-1.17× at 5000. Adding the control-path refactor should
restore the win at 5000 — projected combined 1.3-1.5× on the tasks
variant and 1.4× on the actors variant.

## Scope

### Part 1: data path (~500 LoC)

| file | changes |
| --- | --- |
| `ray/experimental/locations.py` + `__init__.py` | new `get_object_sizes(refs)` helper |
| `data/_internal/execution/interfaces/physical_operator.py` | `DataOpTask` switches from scalar `_pending_*_ref` to queue + partial; new `peek_pending_pair` + drainable semantics; `on_data_ready` takes `bytes_by_meta_ref` dict; per-ref fallback for cache miss; `fetch_and_drain` convenience |
| `data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks` runs the 5-step flow above |
| `data/tests/test_streaming_executor.py` + `tests/util.py` | tests updated to new `on_data_ready` signature; new tests for peek-drain + size-aware trim + cache-miss fallback |

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

1. **Part 1 first.** Self-contained, measured wins, smaller blast radius
   (touches the data-fetch path, not the resource manager). Lands as its
   own PR with the queue-based `DataOpTask` + `get_object_sizes` helper.
2. **Part 2 follow-up.** Separate review attention (touches every op
   subclass that overrides `*_logical_usage`); benefits from Part 1's
   release-test measurements to baseline against.

Either can ship without the other. If Part 2's op-subclass migration runs
long, Part 1 carries most of the mid-range win in production while Part 2
lands incrementally.

## Alternatives considered

### Data path

- **Per-pair `ray.get` with smaller fixed overhead** — would require
  Ray Core changes to the `ray.get` codepath; large blast radius for
  a marginal improvement.
- **Streaming generator protocol change** to yield `(block_ref, size,
  meta_ref)` triples — biggest API surface change; requires every Ray
  Data task generator to comply.
- **Hard cap on batch size** (`MAX_REFS_PER_BATCH=256`) — bounds the
  blast radius but doesn't make the fetch budget-aware.

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
1. **`get_object_sizes` race window.** A streaming-gen ref can be
   delivered to the driver before the owner-side `SealOwnedObject`
   report arrives, returning `None` from `get_object_sizes`.
   Mitigation: `None` falls through to "fetch this pair anyway",
   identical behavior to today's per-pair `ray.get`. Pure optimization,
   no correctness dependency.
2. **Batched `ray.get` stragglers.** One slow ref blocks the whole
   batched call. Mitigation: per-ref fallback inside `on_data_ready`
   handles `GetTimeoutError`. As a future-future tweak, cap the batch
   size at ~1000 refs.

### Part 2
1. **Cache drift.** Any future code path that mutates op state without
   calling a hook silently corrupts the cached usage. Mitigation: keep
   `current_logical_usage` etc. as canonical readers backed by the
   cache; add a debug-mode invariant check that periodically compares
   cached vs. freshly-recomputed values.
2. **Op subclass migration.** Each subclass override needs auditing.
   Mitigation: do one op at a time; the cached field defaults to a
   freshly-recomputed value if the subclass hasn't migrated yet
   (gradual rollout).
3. **External consumers.** `iter_batches` / `streaming_split` pop bytes
   from the output queue outside the scheduler thread. Need to either
   fire the `on_output_bytes_consumed` hook from those paths or model
   "externally-consumed bytes" separately. Either way, an explicit
   decision rather than today's implicit recomputation.

## Open questions

- Should `_cached_logical_usage` be a single `ExecutionResources` or
  four separate scalar fields (cpu / gpu / memory / obj_store)?
  Constructors are the bottleneck, so probably scalars;
  `ExecutionResources` materialized on read.
- Does `_update_allocated_budgets` benefit from the cached values too,
  or is it a separate cost worth profiling independently?
- Backwards compatibility: maintain old method signatures
  (`current_logical_usage()` etc.) indefinitely as cached-value
  readers, or deprecate them?
- For Part 1: cap batch size at a constant `MAX_REFS_PER_BATCH` or
  size-aware "fetch only what fits ~B bytes of cumulative metadata"?
  Latter avoids straggler effects without per-iteration count caps.

## Out of scope

- Changes to `_update_allocated_budgets` budget-allocation algorithm
  itself.
- Changes to `ray.wait` / `ray.get` Ray Core primitives.
- The other 5000-worker scaling pain (`dispatch_next_task`'s
  `remote()` overhead). Separate follow-up.
- Op-fairness / preemption logic.
