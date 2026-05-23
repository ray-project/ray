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
| **Data path**  | `on_data_ready` issues `ray.get(meta_ref)` once *per ready pair* across *every* ready task; the scheduler thread blocks on metadata fetch + deserialization | Defer the metadata fetch entirely. Scheduler carries `size_bytes` (from Ray Core) and `num_rows` (inline from the streaming gen) as scalars on the `RefBundle`; downstream consumers that need full `BlockMetadata` (stats reporting, lineage) `ray.get` the `meta_ref` lazily, off the scheduler thread |
| **Control path** | `update_usages` re-derives every op's resource bundle from scratch *on every dispatch* (~20 k calls per iteration at 5000 workers)             | Ops push resource deltas at mutation sites (emit / dispatch / done); `update_usages` shrinks to an aggregator      |

On the wide-schema `worker_scaling` release-test matrix (m5.2xlarge × {36,
72, 143, 358} nodes for {500, 1000, 2000, 5000} workers), the two parts
together are projected to cut max scheduling-loop time by ~40-60 % across
the matrix.

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

## Part 1: Data path — defer the metadata fetch entirely

### Design

The scheduler thread **never `ray.get`s metadata**. It carries the two
scalar fields it actually needs from each block (`size_bytes` and
`num_rows`) as inline values on the `RefBundle`. The full
`BlockMetadata` (exec stats, input files, task exec stats) lives behind
a `meta_ref` that downstream consumers — stats reporting, lineage
tracking — fetch lazily, off the scheduler thread.

```
process_completed_tasks(topology, ...)
   │
   ├── ray.wait(active_task_refs) → ready set
   │
   ├── for each ready DataOpTask:
   │      while task.peek_pending_pair() is not None: pass     ← drain gen
   │
   ├── # No ray.get on metadata. We already have:
   │   #   - block_ref           (from the gen)
   │   #   - num_rows scalar     (inline from the gen — see 1b)
   │   #   - meta_ref            (from the gen, kept opaque)
   │   #   - size_bytes scalar   (from Ray Core — see 1c)
   │
   └── for each ready DataOpTask:
          task.on_data_ready(budget)                            ← pure assemble + emit
              │
              ├── pop queued pair (block_ref, num_rows, meta_ref)
              ├── size_bytes ← ray.experimental.get_object_sizes([block_ref])[0]
              ├── build RefBundle entry (block_ref, size_bytes, num_rows, meta_ref)
              ├── emit RefBundle
              └── bytes_read += size_bytes
```

### 1a. `DataOpTask` queue holds `(block_ref, num_rows, meta_ref)` triples

Replace the single `_pending_block_ref` / `_pending_meta_ref` scalar
slots with a `Deque[(block_ref, num_rows, meta_ref)]` queue, plus a
`_partial` stash for the half-pulled-pair case (the gen yields the
three components in order; they can arrive in separate scheduler
ticks). `peek_pending_pair()` is a drainable method (call in a loop
until it returns `None`).

```python
class DataOpTask:
    _pending: Deque[Tuple[ObjectRef[Block], int, ObjectRef[BlockMetadata]]]
    _partial: ...                                  # half-pulled state
    _gen_exhausted: bool
    _gen_errored_block: Optional[ObjectRef]

    def peek_pending_pair(self) -> Optional[Tuple[ObjectRef, int, ObjectRef]]:
        """Pull one (block_ref, num_rows, meta_ref) triple from the gen
        into _pending. Returns None if not immediately ready."""
        ...

    def on_data_ready(self, max_bytes_to_read) -> int:
        """Pop queued triples, look up size_bytes via the Core primitive,
        build RefBundle entries, emit."""
        ...
```

Why a queue rather than a scalar: pulling from the generator is
**destructive** — once we peek, the values are gone from the stream.
Triples that don't get emitted this iteration (budget exhausted) must
persist on the task.

### 1b. Streaming generator protocol: yield `num_rows` inline

Today the producing worker yields two values per block:

```python
yield block                                            # ObjectRef
yield pickle.dumps(BlockMetadataWithSchema(...))       # ObjectRef
```

Proposed:

```python
yield block                                            # ObjectRef
yield num_rows                                         # int — small object, inlined
yield pickle.dumps(BlockMetadataWithSchema(...))       # ObjectRef — fetched lazily
```

The `num_rows` value is a small Python `int`, which Ray Core already
inlines into the `ObjectRef` itself (objects under
`max_direct_call_object_size`, ~100 KB by default — way more than an
int needs). `ray.get` on an inlined ref is a local memory copy with
no object store interaction.

The full metadata is still produced and stored — we just don't fetch it
on the scheduler thread.

This is a Ray-Data-internal protocol change to the streaming generator
contract; no Ray Core change involved. It does require updating every
task generator that participates in `DataOpTask` to yield the
three-tuple instead of the pair. The existing TODO in
`DataOpTask.__init__` calls this protocol out as something to evolve.

### 1c. Assumed dependency: cheap per-ref `size_bytes` from Ray Core

The design depends on a Ray Core primitive that returns each ref's byte
size **without** issuing an RPC or fetching the object data, and that is
**reliably populated** by the time the streaming generator surfaces the
ref to the driver. Surface used in the rest of this document:

```python
sizes = ray.experimental.get_object_sizes(block_refs)   # List[int], one per ref
```

How Ray Core exposes this — whether via a focused helper, an extension
to an existing API, an inlined field on `ObjectRef`, or something else
— is a question for the Ray Core team and out of scope for this RFC.
This RFC assumes the primitive exists and is reliable for streaming-gen
owned refs; see the open questions section.

### 1d. `RefBundle` layout change

`RefBundle.blocks` today is `Tuple[Tuple[ObjectRef[Block],
BlockMetadata], ...]` — each entry carries a fully-dereferenced
`BlockMetadata` object. The proposed shape:

```python
@dataclass(frozen=True)
class BlockEntry:
    block_ref:  ObjectRef[Block]
    size_bytes: int                  # from Ray Core
    num_rows:   int                  # inlined by the streaming gen
    _meta_ref:  ObjectRef[BlockMetadata]   # opaque; fetched on demand

    @cached_property
    def metadata(self) -> BlockMetadata:
        """Lazy: fetched the first time a consumer asks. Only stats /
        lineage paths exercise this; scheduler-thread code never does."""
        return ray.get(self._meta_ref)
```

`RefBundle` then carries `Tuple[BlockEntry, ...]` instead of
`Tuple[Tuple[ObjectRef, BlockMetadata], ...]`. Schema stays where it is
— `RefBundle.schema` is already a top-level field (see 1f below for
how it's populated without a scheduler-thread `ray.get`).

`RefBundle.size_bytes()` and `RefBundle.num_rows()` (today both walk
each block's metadata) become direct sums of the inline scalars. No
dereference, no `ray.get`.

### 1e. Lazy metadata access for stats / lineage consumers

The 68 production sites that read `bundle.metadata.*` fall into two
groups:

- **Scheduler hot path (read scalars):** `size_bytes`, `num_rows`.
  After this RFC: direct field reads, no `ray.get`.
- **Stats / lineage consumers (read non-scalar fields):**
  `exec_stats`, `task_exec_stats`, `input_files`. After this RFC: pay
  a `ray.get` on `_meta_ref` when accessing — but this fires once per
  op completion, not per block per scheduler iteration. Off the
  scheduler thread.

Examples:
- `map_operator._output_blocks_stats.extend(to_stats(bundle.metadata))`
  fires when the op finishes producing — one batched
  `ray.get(meta_refs)` materializes all the metadata at once.
- `input_data_buffer` reads `input_files` once at op construction; one
  `ray.get` per op.
- Per-block scheduler-loop logging (which currently reads
  `metadata.num_rows` / `metadata.size_bytes` for progress bar
  updates) — already covered by the scalar fields.

### 1f. Schema delivery

`RefBundle.schema` is today populated by the scheduler when it
`pickle.loads`-es the `BlockMetadataWithSchema` payload. With the
metadata fetch deferred, schema needs another delivery path. Options:

1. **Worker yields schema once per task** (in the gen protocol, as a
   fourth optional inline value before the first block). The scheduler
   caches it per-task / per-op; subsequent yields don't repeat it.
   Schema is identical across all blocks of a single map task, so a
   single inline yield amortizes naturally.
2. **Fetch on-demand from the first block's `meta_ref`.** Downstream
   ops that need schema validation pay one `ray.get` at op-bind time;
   subsequent blocks reuse the cached schema.

Option 1 is cleaner if we're already changing the gen protocol for
`num_rows`. See open questions.

### Per-call `ray.get` fallback (error path only)

`on_data_ready` keeps a per-ref `ray.get(meta_ref)` + `GetTimeoutError`
warning as a fallback for the rare case the Core size primitive
returns `None` (Open Questions item 1) — falls through to the
pre-refactor behavior with the standard per-ref warning and
retry-next-iteration semantics. In the steady-state happy path this
fallback is dead code; it exists for diagnostic visibility when
something has gone wrong upstream.

A convenience `fetch_and_drain(max_bytes_to_read)` wraps `peek` +
`on_data_ready` for direct callers (tests).

### Expected impact (data path)

Pre-refactor py-spy breakdown for `on_data_ready` (5000_tasks):

| frame                              | inclusive | what it is |
| --- | ---: | --- |
| `on_data_ready` (total)            | 294 s |  |
|   `ray.get_objects` (leaf)         | 164 s | per-pair `ray.get(meta_ref)` |
|   `pickle.loads` (metadata)        |  ~25 s| deserialize `BlockMetadataWithSchema` |
|   `Schema.__setstate__` etc.       |  ~10 s| schema deserialize on cache miss |
|   `RefBundle` construction + emit  |  ~15 s| `_output_ready_callback` + bookkeeping |
|   loop overhead                    |  ~80 s| `_next_sync` gen pulls, queue mgmt |

After this RFC, the scheduler thread does:

- `ray.experimental.get_object_sizes(block_refs)` — cheap local lookup.
- Queue pops + `RefBundle` construction.
- `bytes_read += size_bytes` (scalar).

**No `ray.get`, no `pickle.loads`, no schema deserialize.** Projected
`on_data_ready`:

| variant     | master `on_data_ready` | projected | reduction |
| ----------- | ---: | ---: | ---: |
| 1000_tasks  |  40 s |  ~3 s | ~92 % |
| 2000_tasks  |  87 s |  ~6 s | ~93 % |
| 5000_tasks  | 294 s | ~20 s | ~93 % |

The savings concentrate at scale (more pairs ingested per iteration =
more avoided `ray.get`s).

Cost paid elsewhere: stats / lineage paths now do an extra `ray.get`
per op completion. For a topology of ~3 ops, that's 3 `ray.get` calls
per dataset, batched. Negligible compared to the per-pair savings —
and these run off the scheduler thread.

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
| `process_completed_tasks`  | 315 s |  ~40 s  | ~40 s   |
| `update_usages`            | 110 s |  ~110 s |  ~30 s   |
| `dispatch_next_task`       | 120 s |  ~120 s | ~120 s   |
| `safe_round` (leaf)        |  ~46 s|   ~46 s |   <5 s  |
| **Total scheduler thread** | ~600 s| ~320 s | **~200 s** |

The data-path win is dramatic at every scale once the per-pair
`ray.get` and pickle-loads are out of the hot path. The control-path
win compounds at the high end where `update_usages` dominates after
the data side shrinks. Together: an estimated **3× reduction in total
scheduler-thread time** at 5000 workers.

For `max_scheduling_loop` (the metric users actually feel), prototype
batched-`ray.get` measurements (without deferred metadata) showed
1.3-1.77× reduction at 500-2000 workers, dampening to 1.12-1.17× at
5000. With deferred metadata + push-model resource accounting, the
projected combined improvement is **1.5-2.5× across the whole
scaling range**, with the largest absolute savings at 5000 workers.

## Scope

### Part 1: data path (~600-800 LoC)

| file | changes |
| --- | --- |
| Ray Core API surface for per-ref `size_bytes` (exact shape TBD with Core team) | exposes the cheap, reliable size lookup |
| Streaming-generator protocol contract (Ray Data internal) | adds inline `num_rows` (and optionally schema) to the yield sequence; updates the workers that produce blocks for `DataOpTask` (map, read, etc.) |
| `data/_internal/execution/interfaces/ref_bundle.py` | `RefBundle.blocks` becomes `Tuple[BlockEntry, ...]`; `BlockEntry` carries inline `size_bytes` + `num_rows` + opaque `_meta_ref` + lazy `metadata` property |
| `data/_internal/execution/interfaces/physical_operator.py` | `DataOpTask` queue becomes `Deque[(block_ref, num_rows, meta_ref)]`; `peek_pending_pair` drainable; `on_data_ready` does scalar-only emit, no `ray.get` in steady state |
| `data/_internal/execution/streaming_executor_state.py` | `process_completed_tasks` no longer batches `ray.get` on metadata |
| Stats / lineage call sites that read `bundle.metadata.exec_stats` etc. | migrate to the lazy `BlockEntry.metadata` accessor; ideally batch their own `ray.get` over many entries at op-completion time |
| `data/tests/test_streaming_executor.py` + `tests/util.py` | tests updated to the new `RefBundle` layout and gen protocol; new tests for inline `num_rows` delivery + lazy metadata access |

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

1. **Ray Core size primitive first.** Part 1 depends on it; landing it
   first unblocks the Data-side work and lets us validate
   reliability on the streaming-gen use case before relying on it.
2. **Part 1 (data path).** Streaming-gen protocol change + `RefBundle`
   layout + `DataOpTask` queue + lazy `BlockEntry.metadata`. Lands as
   its own PR.
3. **Part 2 (control path).** Independent of Parts 1 and the Core
   primitive. Could land before Part 1 if convenient.

Either Data-side part can ship without the other. If Part 2's
op-subclass migration runs long, Part 1 carries most of the wide-schema
wins in production while Part 2 lands incrementally.

## Alternatives considered

### Data path

- **Batched `ray.get` of metadata in `process_completed_tasks`** — keep
  fetching metadata in the scheduler thread but collapse N small
  `ray.get` calls into one batched call per scheduler iteration. This
  was the earlier shape of the same proposal and a measured prototype
  showed 1.3-1.77× max-loop reduction at 500-2000 workers, dampening
  to 1.12-1.17× at 5000.
  - *Pros:* no Ray Core dependency (uses existing `ray.get`); no
    streaming-gen protocol change; existing `RefBundle` layout
    preserved.
  - *Cons:* metadata fetch is still on the scheduler thread, just
    batched. At 5000 workers the batched call itself becomes the
    bottleneck (one slow ref blocks the whole call; `get_objects` leaf
    time *grows* — measured 164 s → 218 s for tasks variant). Solves
    the per-call overhead, doesn't solve the "metadata is on the
    critical path at all" question. Adds cross-iteration cache state to
    handle over-fetch.
- **Per-pair `ray.get` with smaller fixed overhead** — would require
  Ray Core changes to the `ray.get` codepath; large blast radius for
  a marginal improvement.
- **Hard cap on batch size** (`MAX_REFS_PER_BATCH=256`) — bounds the
  blast radius but still fetches metadata on the scheduler thread.

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
1. **Streaming-gen protocol change is invasive.** Every task generator
   that participates in `DataOpTask` (map, read, hash-shuffle, ...)
   has to yield the new three-tuple instead of the pair. Mitigation:
   make the inline `num_rows` optional initially — `peek_pending_pair`
   tolerates pairs (the old shape) and falls back to `ray.get` on the
   meta_ref to obtain `num_rows`. Migrate generators one at a time,
   measure each.
2. **Lazy metadata fetch shifts cost to consumers.** Stats / lineage
   paths now pay a `ray.get` per `meta_ref`. Mitigation: most
   consumers operate on whole bundles at op-completion time, so they
   can batch their own `ray.get(meta_refs)`. The total work is no
   higher than before — it's just off the scheduler thread.
3. **Dependence on the Ray Core size primitive being reliable.** If
   the assumed cheap `size_bytes` lookup ever returns stale or missing
   values for streaming-gen owned refs, the scheduler hot path falls
   through to `ray.get(meta_ref)` to recover `size_bytes`. Mitigation:
   see "Open questions" — Core team to specify the guaranteed-reliable
   surface.
4. **Schema delivery without metadata fetch.** Two options sketched in
   1f; the gen-protocol option is preferred but adds yet another
   inline value. Mitigation: prototype both, measure which is
   cleaner.

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

- **For the Ray Core team:** what is the right Core-side surface for
  cheap, no-RPC, reliably-populated per-ref `size_bytes`? Part 1
  assumes such a primitive exists and is reliable for streaming-gen
  owned refs at the moment the driver receives the ref. The Python
  entry point and the underlying implementation are both Core-team
  decisions; this RFC assumes the result.
- **Schema delivery without scheduler-thread `ray.get`:** option 1
  (worker yields schema once per task in the gen protocol) or option
  2 (lazy fetch from the first block's `meta_ref` at op-bind time)?
  Pick one before implementation.
- **Lazy `BlockEntry.metadata` access pattern:** should the stats /
  lineage consumers batch their `ray.get` over all of an op's blocks
  at op-completion time, or fetch per-block as needed? Probably
  batched-at-completion for efficiency; needs an explicit code-style
  recommendation.
- **`BlockEntry` shape:** dataclass with named fields, or just a
  four-tuple `(block_ref, size_bytes, num_rows, meta_ref)`? Dataclass
  is clearer; tuple is cheaper to construct. Hot path, so possibly
  tuple.
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
