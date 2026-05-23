# RFC: Push-model resource accounting in the Ray Data StreamingExecutor scheduler loop

## Status

Draft. Not implementation; a design proposal for review before code.

## Motivation

After the metadata-fetch batching landed (#63483), py-spy on the wide-schema
`worker_scaling` release tests shows the StreamingExecutor scheduling thread's
remaining dominant cost is **resource accounting**, not data movement:

| frame (5000_tasks, inclusive of scheduler thread) | seconds | share |
| --- | ---: | ---: |
| `_scheduling_loop_step` (total)                   |  ~600 s | 100 % |
| `update_usages` + `_update_allocated_budgets`     |  ~110 s |  ~18 % |
| `safe_round` (leaf hot spot inside update_usages) |   ~46 s |  ~8 %  |
| `process_completed_tasks` (post-PR #63483)        |  ~280 s |  ~47 % |
| `dispatch_next_task` (`remote()` + bookkeeping)   |  ~120 s |  ~20 % |

`update_usages` is called **three times per scheduler iteration** — once before
`process_completed_tasks`, once after, and **once per dispatched task** inside
the dispatch loop. At 5000 workers × ~10 blocks each, that's ~20 k full
re-computations of every operator's resource bundle per scheduler iteration,
even though each individual mutation only changes one operator's slot.

This RFC proposes inverting the control flow: ops push their resource deltas
at the points of mutation; the `ResourceManager` becomes a passive aggregator;
`update_usages` shrinks to a near-no-op.

## When each scheduler hook is called today

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
   │      ├── for each ready DataOpTask:
   │      │     while task.peek_pending_pair() is not None: pass
   │      │
   │      ├── ray.experimental.get_object_sizes(block_refs)   ← #63483 size-aware trim
   │      ├── per-op budget trim
   │      ├── ray.get(trimmed_meta_refs, timeout=...)         ← ONE batched fetch
   │      │
   │      └── for each ready DataOpTask:
   │             task.on_data_ready(budget, bytes_by_meta_ref)   ← [A] EMITS RefBundles
   │
   ├── update_usages()                                   ← [2] POST-PROCESS recompute
   │
   └── while True:                                       ← dispatch phase
          op = select_operator_to_run(...)
          if op is None: break
          topology[op].dispatch_next_task()              ← [B] SUBMITS a remote task
          update_usages()                                ← [3] PER-DISPATCH recompute (hot)
```

### `on_data_ready` (per-task, called by `process_completed_tasks`)

- **Trigger:** `ray.wait` returned the task's streaming generator as ready
  (i.e., the producing worker yielded one or more new outputs).
- **Frequency:** once per iteration per task whose generator surfaced new
  blocks. Each call may emit multiple RefBundles (subject to
  `max_bytes_to_read` budget).
- **State mutated:** the op's output queue (`op.output_queue.append(bundle)`)
  and the per-task `_pending_pairs` deque.
- **Resource impact:** **emitting a RefBundle increases the op's
  object-store-memory usage by `meta.size_bytes`**. Today this delta is
  observable only by polling `_estimate_object_store_memory_usage(op)`
  inside the next `update_usages` call.

### `dispatch_next_task` (per-op, called from the scheduler's dispatch loop)

- **Trigger:** `select_operator_to_run` chose this op based on current
  budgets and backpressure. Decision input includes every op's
  `current_logical_usage()` — i.e., the cached output of `update_usages`.
- **Frequency:** N times per iteration, where N is the number of ops the
  scheduler decides to dispatch to before `select_operator_to_run` returns
  `None`. Typical: tens to hundreds per iteration.
- **State mutated:** the op's internal task list (a new in-flight task), the
  op's pending-task counter, the resource bundle reservation.
- **Resource impact:** **dispatching a task adds the task's resource bundle
  (CPU/GPU/memory) to the op's `running_logical_usage()`**. Today this
  delta is observable only via the per-dispatch `update_usages` poll that
  follows.

### `update_usages` (cross-op, called by the scheduler 3+ times per iteration)

- **Trigger sites:** call [1] before `process_completed_tasks`, [2] after,
  and [3] after every `dispatch_next_task`.
- **Frequency:** **2 + dispatch_count per scheduler iteration**. On the
  5000_tasks workload that's ~20 002 calls per scheduler tick.
- **What it does:** wipes `_op_usages`, `_op_running_usages`,
  `_op_pending_usages`, `_global_usage`, `_global_running_usage`,
  `_global_pending_usage`, then for each op in topology (reversed):
  - calls `op.current_logical_usage()` (constructs `ExecutionResources`)
  - calls `op.running_logical_usage()` (constructs another)
  - calls `op.pending_logical_usage()` (constructs another)
  - calls `_estimate_object_store_memory_usage(op, state)` (walks the
    output queue size)
  - `op_usage.copy(object_store_memory=...)` (constructs another)
  - 3× `global.add(op_usage)` (constructs another)
  - sets `op._metrics.obj_store_mem_used`
  - calls `_update_allocated_budgets()` if there's an allocator
- **Cost:** every `ExecutionResources` constructor calls `safe_round` 4×
  (`cpu/gpu/object_store_memory/memory`). At ~5 constructions per op × 3 ops
  × 20 k dispatch-loop iterations = **~1.2 M `safe_round` calls per
  scheduler iteration**. This matches the observed ~17 s of leaf
  `safe_round` time and the ~110 s inclusive cost of `update_usages` at
  5000 workers.

The author left a TODO acknowledging this:

```python
def update_usages(self):
    """Recalculate resource usages."""
    # TODO(hchen): This method will be called frequently during the execution loop.
    # And some computations are redundant. We should either remove redundant
    # computations or remove this method entirely and compute usages on demand.
```

## The redundancy

Calls [1] and [2] bracket `process_completed_tasks` but nothing between [3] of
the previous iteration and [1] of the current iteration mutates op state, so
[1] is *always* a no-op repeat of [3] from the prior iteration.

Calls of type [3] each only need to refresh **one** op's slot — the one
that just dispatched — but the implementation re-computes every op's slot
every time.

## Proposal: push deltas at the mutation sites

Invert the control flow. Each event that changes per-op resource usage tells
the `ResourceManager` directly, by delta. The manager keeps a per-op cache
that's always current. `update_usages` shrinks to a final aggregation +
budget-allocator update.

### New op-level hooks (PhysicalOperator API)

```python
class PhysicalOperator:
    # Maintained by the manager; ops update via the hooks below.
    _cached_logical_usage: ExecutionResources
    _cached_running_usage: ExecutionResources
    _cached_pending_usage: ExecutionResources
    _cached_obj_store_mem_bytes: int

    def on_output_bytes_added(self, n: int) -> None:
        """Called by DataOpTask.on_data_ready after each RefBundle emit."""
        self._cached_obj_store_mem_bytes += n

    def on_output_bytes_consumed(self, n: int) -> None:
        """Called when a downstream operator pops bytes from this op's
        output queue (or when an external consumer reads via
        iter_batches / streaming_split)."""
        self._cached_obj_store_mem_bytes -= n

    def on_task_dispatched(self, task_bundle: ExecutionResources) -> None:
        """Called by dispatch_next_task after submitting a remote task."""
        self._cached_running_usage = self._cached_running_usage.add(task_bundle)
        self._cached_pending_usage = self._cached_pending_usage.subtract(task_bundle)

    def on_task_completed(self, task_bundle: ExecutionResources) -> None:
        """Called from task_done_callback."""
        self._cached_running_usage = self._cached_running_usage.subtract(task_bundle)
```

### ResourceManager becomes an aggregator

```python
def update_usages(self) -> None:
    # No re-derivation; just sum the cached values that the hooks maintained.
    self._global_usage = ExecutionResources.sum(
        op._cached_logical_usage for op in self._topology
    )
    self._global_running_usage = ExecutionResources.sum(
        op._cached_running_usage for op in self._topology
    )
    self._global_pending_usage = ExecutionResources.sum(
        op._cached_pending_usage for op in self._topology
    )
    if self._op_resource_allocator is not None:
        self._update_allocated_budgets()
```

The hot path becomes O(N_ops) scalar additions, not O(N_ops × constructors ×
safe_round_calls).

`_update_allocated_budgets` stays — it does the across-op fair-share work
that no single op can do alone — but it now reads from the cached aggregates
instead of re-deriving them.

### Call-site changes

```python
# DataOpTask.on_data_ready, after emitting each RefBundle:
self._output_ready_callback(RefBundle(...))
self._op.on_output_bytes_added(meta.size_bytes)   # new

# OpState.dispatch_next_task, after submitting:
op.submit_task(task)
op.on_task_dispatched(task.task_resource_bundle)   # new

# DataOpTask.task_done_callback wrapper:
op.on_task_completed(task.task_resource_bundle)    # new

# downstream operator pulling from its input queue:
bundle = input_op.output_queue.pop()
input_op.on_output_bytes_consumed(bundle.size_bytes)   # new
```

## Expected wins

| frame                                    | today (5000_tasks) | projected (after RFC) | reasoning |
| ---                                      | ---:               | ---:                  | --- |
| `update_usages` (inclusive)              | ~110 s             | ~10-20 s              | drops to "sum N scalars per call" + budget allocator |
| `safe_round` (leaf)                      | ~46 s              | <5 s                  | called only at construction time, not in the hot path |
| `_update_allocated_budgets`              | ~30 s              | ~20-25 s              | still runs, reads cached values; small constant-factor save |

Estimated total scheduler-thread reduction at 5000 workers: **~110 s → ~30
s** (~14 % of the 600 s total). Combined with #63483's data-side wins, max
scheduling loop should drop another ~15-25 % vs. post-#63483.

## Scope

| layer | changes |
| --- | --- |
| `ResourceManager` | new aggregator-only `update_usages`; keeps `_update_allocated_budgets`; removes per-op poll loop |
| `PhysicalOperator` | adds 4 hooks (`on_output_bytes_added/consumed`, `on_task_dispatched/completed`) + 4 cached fields; existing `current_logical_usage`/`running_logical_usage`/`pending_logical_usage` either delete (pure reads of cache) or become trivial reader properties |
| `DataOpTask.on_data_ready` | calls `op.on_output_bytes_added(meta.size_bytes)` after each emit |
| `OpState.dispatch_next_task` | calls `op.on_task_dispatched(...)` after submit |
| `OpTask.task_done_callback` paths | call `op.on_task_completed(...)` |
| Downstream-consume sites (output queue pops, external consumer reads) | call `op.on_output_bytes_consumed(...)` |
| Op subclasses with custom resource accounting (`ActorPoolMapOperator`, `LimitOperator`, `OutputSplitter`, ...) | migrate their `current_logical_usage` overrides to push deltas via the hooks |
| Tests | every `mock` that replaced `current_logical_usage` needs updating to push deltas or set cached fields |

Estimated diff size: **~300-600 LoC** across `_internal/execution/` files.
The most invasive part is the op-subclass migration — each subclass that
overrides one of the `*_logical_usage` methods needs to flip from "compute
on demand" to "push on mutation".

## Alternatives considered

### Alt 1: incremental `update_usages_for_op(op)`

Keep the poll structure but recompute only the op that just changed at sites
[3]. ~30-50 LoC change; captures ~70 % of the win.

- Pros: tiny diff, no API changes, low review risk.
- Cons: leaves the architectural mismatch in place. Any future caller that
  mutates op state still has to remember to call `update_usages_for_op` —
  same trap, just slightly cheaper. Doesn't address `safe_round` per-op
  constructor cost.

### Alt 2: inline / fast-path `safe_round`

Skip `safe_round` when the input is already canonicalized (the steady-state
case). ~10 LoC change in `execution_options.py`.

- Pros: trivially small. Cuts `safe_round` leaf cost by ~80 %.
- Cons: addresses the symptom (`safe_round` is expensive), not the cause
  (we construct millions of `ExecutionResources` per iteration that aren't
  semantically new). Doesn't unblock anything downstream.

### Alt 3: cache `ExecutionResources` by tuple key

Memoize construction: `ExecutionResources(cpu=0.5, gpu=0, ...)` returns the
same instance for the same args.

- Pros: works without API changes.
- Cons: dict-lookup overhead approaches per-construction cost; bookkeeping
  for the cache becomes its own footgun.

**My take:** Alt 1 ("incremental update_usages_for_op") is the right
**interim** fix that can land while this RFC is reviewed. The push-model
refactor is the right **long-term** architecture. Both are independent of
#63483.

## Risks

1. **Cache drift.** Any future code path that mutates op state without
   calling a hook will silently corrupt the cached usage. Mitigation: keep
   `current_logical_usage` etc. as the canonical readers (now backed by the
   cache); add a debug-mode invariant check that compares cached vs.
   freshly-recomputed values per N iterations.
2. **Op subclass migration.** Each subclass override needs auditing.
   Mitigation: do them one at a time; the cached field defaults to a
   freshly-recomputed value if the subclass hasn't migrated yet (gradual
   rollout).
3. **External consumers.** `iter_batches` / `streaming_split` pop bytes from
   the output queue outside the scheduler thread. Need to either fire the
   `on_output_bytes_consumed` hook from those code paths or model
   "externally-consumed bytes" separately. Either way, an explicit decision
   rather than today's implicit recomputation.

## Open questions

- Should `_cached_logical_usage` be a single `ExecutionResources` or four
  separate scalar fields (cpu / gpu / memory / obj_store)? Constructors are
  the bottleneck, so probably scalars; `ExecutionResources` could be
  materialized on read.
- `_update_allocated_budgets` reads `get_completed_ops_usage()` — does that
  also benefit from the cached values, or is it a separate cost?
- Backwards compatibility: should we maintain the old method signatures
  (`current_logical_usage()` etc.) as cached-value readers indefinitely,
  or deprecate them?

## Out of scope

- Changes to `_update_allocated_budgets` budget-allocation algorithm
  itself. That's a separate optimization.
- Changes to the data-fetch side (already covered by #63483).
- Changes to `ray.wait` / `ray.get` core primitives.
