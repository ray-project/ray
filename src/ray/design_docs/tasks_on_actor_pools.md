# Ray Tasks on Actor Pools — Design Spec

**Status:** Draft / RFC
**Scope:** Re-implement the Ray *task* API (`@ray.remote` on a function, `.remote()`,
`.options()`, ObjectRef composition) as a **Python layer on top of the actor backend**,
so that the core scheduler only has to reason about actors.

---

## 1. Motivation

Today `f.remote()` and `actor.method.remote()` are two independent submission paths in
the core worker and the raylet:

| Concern | Normal task | Actor task |
|---|---|---|
| Submitter | `NormalTaskSubmitter` (`src/ray/core_worker/task_submission/normal_task_submitter.cc`) | `ActorTaskSubmitter` (`.../actor_task_submitter.cc`) |
| Placement | Raylet worker lease (`RequestWorkerLease`), spillback, backlog reporting | Fixed at actor creation |
| Worker | Leased from `WorkerPool`, resources per lease | Long-lived, lifetime resources |
| Retries | `max_retries` (default 3) + lineage reconstruction | `max_task_retries` (default 0) |

The normal-task path is the source of much of the core's complexity: worker leases,
lease spillback, worker-pool sizing, task backlog reporting, lineage/resubmission,
and object reconstruction. If tasks can be expressed as *actor calls onto a managed
pool of actors*, the backend keeps only one scheduling concept (actors), and the task
programming model becomes a Python library.

There is also a concrete performance upside: a normal task costs a raylet round trip to
acquire a worker lease before the task can be pushed. An actor task is a direct
caller→worker gRPC. Small-task submission throughput and latency should improve once
the pool is warm.

## 2. Goals and non-goals

### 2.1 Goals

- G1. `@ray.remote` on a plain function returns something whose `.remote()` yields a
  **real `ray.ObjectRef`**, synchronously, without blocking on scheduling.
- G2. Refs returned by pool tasks compose exactly like task refs today: `ray.get`,
  `ray.wait`, passing as arguments to other tasks/actor methods, `ray.cancel`,
  nesting, `ObjectRefGenerator` for streaming returns.
- G3. Nothing below the Python layer needs to know a "task" exists. Every submission
  is an actor task on an actor the layer created.
- G4. Resource requests (`num_cpus`, `num_gpus`, `memory`, custom resources),
  `runtime_env`, and scheduling strategies keep their meaning.
- G5. Failures are surfaced as exceptions at `ray.get` time with tracebacks that are
  indistinguishable-in-spirit from today's `RayTaskError`.

### 2.2 Non-goals (hard constraints)

- **NG1. No automatic task retries.** `max_retries` / `retry_exceptions` are not
  implemented. A task executes **at most once**.
- **NG2. No lineage reconstruction.** A lost object is a permanent error; the layer
  never re-executes a function to recreate an object.
- NG3. No cross-language tasks (Java/C++ `f.remote()`) in v1.
- NG4. No change to the actor API itself.
- NG5. Not a drop-in replacement for `ray.remote` in v1 — it ships as a separate,
  opt-in namespace (see §14).

Because of NG1/NG2, several semantics that today are *masked* by retries become
user-visible. §8 specifies exactly what the user sees instead, and §12/Q4 is the most
important open question the no-retry constraint creates.

---

## 3. Compatibility baseline: what the task API guarantees today

This section is the checklist the implementation is graded against. Each item cites the
code that establishes it.

### 3.1 Submission and ref identity

- **T1.** `f.remote(...)` never blocks on scheduling. It returns `ObjectRef` (or a list
  of refs for `num_returns=N`, or an `ObjectRefGenerator` for
  `num_returns="streaming"`) immediately.
  (`python/ray/remote_function.py`, `_remote()` → `core_worker.submit_task`.)
- **T2.** The returned refs are **owned by the caller**. Ownership determines object
  lifetime and who reports `ObjectLostError` / `OwnerDiedError`.
- **T3.** Submission order does not imply execution order. Tasks are unordered.
- **T4.** Submission is effectively unbounded; there is no `max_pending_calls`
  equivalent for tasks.

### 3.2 Arguments and dependency resolution

- **T5.** Top-level `ObjectRef` arguments are resolved before execution and the
  function body sees values, not refs. Refs nested inside containers are *not*
  resolved (the callee sees an `ObjectRef`).
- **T6.** Dependency resolution happens on the **caller**, before the task is placed;
  no worker is occupied while dependencies are pending.
  (`DependencyResolver::ResolveDependencies`.)
- **T7.** Large arguments are auto-`put` into the object store by the caller; small
  ones are inlined into the task spec.
- **T8.** The raylet prefers nodes that already hold the task's arguments
  (locality-aware lease policy, `lease_policy_->GetBestNodeForLease`).

### 3.3 Return values

- **T9.** `num_returns=1` (default), `num_returns=N`, `num_returns="dynamic"`,
  `num_returns="streaming"` for generator functions, with
  `_generator_backpressure_num_objects` and `_num_objects_per_yield`.
- **T10.** Returning an `ObjectRef` from a task produces a ref-to-ref; `ray.get` does
  not auto-dereference.

### 3.4 Resources and scheduling

- **T11.** Per-invocation resources: `num_cpus` (default **1** for tasks),
  `num_gpus`, `memory`, `resources={}`, `accelerator_type`.
  (`_task_only_options` in `python/ray/_common/ray_option_utils.py`.)
- **T12.** Per-invocation `scheduling_strategy`: `"DEFAULT"`, `"SPREAD"`,
  `NodeAffinitySchedulingStrategy`, `PlacementGroupSchedulingStrategy`,
  `NodeLabelSchedulingStrategy`; plus `label_selector` and `fallback_strategy`.
- **T13.** Placement group inheritance: a task launched from inside a PG-scheduled
  parent captures the parent's PG when `placement_group_capture_child_tasks` is set.
  (`_configure_placement_group_based_on_context`.)
- **T14.** Per-invocation `runtime_env`, which may start a fresh worker process with a
  different environment.
- **T15.** Pending tasks are visible to the autoscaler as resource demand
  (`ReportWorkerBacklog` → raylet resource load → autoscaler).
- **T16.** **A task blocked in `ray.get` releases its CPU resources**, which is what
  makes nested/recursive parallelism deadlock-free.
  (`LocalLeaseManager::ReleaseCpuResourcesFromBlockedWorker`, gated on
  `WorkerContext::ShouldReleaseResourcesOnBlockingCalls()`, which explicitly returns
  **false for actor calls**: *"We only support lifetime resources for actors ... thus
  we don't need to release resources for actor calls."*)

### 3.5 Isolation and worker lifecycle

- **T17.** `max_calls=N` recycles the worker process after N invocations.
- **T18.** When `num_gpus > 0` and `max_calls` is unset, Ray **forces `max_calls=1`**
  (`python/ray/remote_function.py`) — i.e. GPU tasks get a fresh process per call by
  default, to avoid CUDA context/memory leakage.
- **T19.** `CUDA_VISIBLE_DEVICES` is set per lease from the task's GPU allocation.
- **T20.** Workers *are* reused across tasks in the same job, so process-global state
  already leaks between tasks today. Parity here is easier than it looks.

### 3.6 Failure and cancellation

- **T21.** Application exception → `RayTaskError` wrapping the cause, re-raised at
  `ray.get` as a dual-inherited exception so `except MyError:` still works
  (`RayTaskError.as_instanceof_cause`, `make_dual_exception_instance`).
- **T22.** Ray-internal frames are stripped from the user-visible traceback
  (`_RAY_CORE_INTERNAL_FRAME_MARKERS`, `_is_hidden_internal_frame`).
- **T23.** Worker crash → `WorkerCrashedError`; OOM kill → `OutOfMemoryError`;
  node death → `NodeDiedError`; bad `runtime_env` → `RuntimeEnvSetupError`.
  With `max_retries=0`, exactly **one** task dies per crashed worker.
- **T24.** `ray.cancel(ref)` cancels a queued task, raises `KeyboardInterrupt` in a
  running task with `force=False`, and kills the worker with `force=True`
  (`WorkerCrashedError`). `recursive=True` cancels children.
- **T25.** Owner death → the object is lost, readers get `OwnerDiedError`.

### 3.7 Observability

- **T26.** `ray list tasks` / `ray summary tasks` show `type=NORMAL_TASK`, the
  function name, and a state machine that distinguishes
  `PENDING_ARGS_AVAIL` / `PENDING_NODE_ASSIGNMENT` / `RUNNING`. The
  `PENDING_NODE_ASSIGNMENT` state is *the* debugging signal for "my cluster is too
  small".
- **T27.** `ray.get_runtime_context()` inside a task exposes `get_task_id()`,
  `get_task_name()`, `get_task_function_name()`, `get_assigned_resources()`.
- **T28.** `RAY_PDB` / `ray.util.pdb.set_trace()` uses `worker.debugger_breakpoint`,
  which is carried on the **normal-task** submission path only
  (`CoreWorker.submit_task` takes `debugger_breakpoint`; `submit_actor_task` does not).

---

## 4. What the actor backend gives us for free vs. what we must rebuild

| Task guarantee | Free on actor tasks? | Notes |
|---|---|---|
| T1 non-blocking submit | ✅ | `submit_actor_task` returns refs synchronously |
| T2 caller ownership | ✅ | Actor-task returns are caller-owned, same as tasks |
| T5–T7 arg semantics | ✅ | Same `DependencyResolver`, same inline/put policy |
| T9 num_returns / streaming | ✅ | `submit_actor_task` takes `num_returns`; `"dynamic"` and `"streaming"` both accepted (`python/ray/actor.py`) |
| T10 ref-to-ref | ✅ | identical |
| T21/T22 error wrapping | ⚠️ | Free, but the traceback gains pool dispatch frames that must be stripped |
| T3 unordered execution | ⚠️ | Actor tasks are **ordered by default**. Requires `allow_out_of_order_execution=True` |
| T24 cancel | ⚠️ | Queued cancel works; `force=True` is **rejected** for actor tasks; graceful cancel only sets a flag readable via `is_canceled()` — it does not raise |
| T11–T13 per-task resources/strategy | ❌ | Actor tasks have no per-call resources or scheduling strategy. Must become pool identity |
| T14 per-task runtime_env | ❌ | Actor-creation-time only. Must become pool identity |
| T8 locality | ❌ | Must be reimplemented in the pool's actor-selection policy |
| T15 autoscaler demand | ❌ | A Python-side queue is invisible. Must be projected into *actor* demand |
| T16 release-on-block | ❌ | **Actors never release CPU when blocked.** This is the deadlock hazard (§12/Q6) |
| T17/T18 max_calls, GPU freshness | ❌ | Must become "recycle the actor after N tasks" |
| T23 blast radius of a crash | ❌ | An actor crash fails *every* task in its inbox, not just the running one (§8, §12/Q4) |
| T26 pending-state observability | ❌ | Pool queue depth must be exported |
| T28 debugger-on-submit | ❌ | Not carried by `submit_actor_task` |

---

## 5. Architecture options

The single decision that shapes everything else is: **at what moment is a task bound to
a specific actor?** A `ray.ObjectRef` cannot be created empty and filled later — there
is no public or private "promise" primitive (`CoreWorker.put_object` always mints a
fresh ID; there is no fulfill-a-preallocated-ref API). Therefore *the ref can only be
produced by an actual actor-task submission*, and "return a ref immediately" implies
"pick the actor immediately" unless something else stands in between.

### Design A — Direct binding (push)

```
f.remote(x)  ->  pool.select_actor()  ->  actor.__ray_execute__.remote(fd, x)  ->  ref
```

The pool picks an actor synchronously and returns that actor task's ref.

- **+** Zero added hops, zero added copies, native refs, full `ray.get`/`ray.wait`
  parity, no new ownership edges.
- **+** Simplest possible failure story: Ray already fails the ref when the actor dies.
- **−** Binding is **final**. There is no work stealing, no re-dispatch, and no
  "wait for the pool to scale up before choosing".
- **−** Consequence: the entire pending queue must live in the actors' inboxes, so
  an actor crash fails every queued task on it (§8.3).
- **−** Locality decisions must be made before dependency resolution completes.

### Design B — Broker indirection (deferred binding)

```
f.remote(x) -> broker.run.remote(fd, [x_ref_boxed]) -> ref        # caller-owned
                broker (async actor) awaits a free worker
                broker calls worker.__ray_execute__.remote(fd, x_ref) and returns value
```

Boxing ref arguments in a list prevents the broker from dereferencing them, so the
*worker* still fetches args directly and locality can be decided late.

- **+** True deferred binding: work stealing, late locality, "scale up then place",
  and — importantly under NG1 — **safe re-dispatch of tasks that provably never
  started**, which is scheduling, not retrying.
- **−** Every **result** round-trips through the broker: one extra hop, one extra
  serialization, one extra object-store copy. Fatal for large returns.
- **−** The broker is a throughput bottleneck and a new failure domain; objects
  in flight are owned by the broker, so broker death loses them.
- **−** `ray.cancel` on the caller-facing ref cancels the *broker* task, requiring
  manual propagation to the worker task.

### Design C — New core primitive (unresolved object)

Add a core-worker API to mint an ObjectRef now and have another worker fulfill it
later (conceptually what dynamic generator returns already do in the other direction).
This gives A's cost profile with B's flexibility.

- **+** Strictly best semantics.
- **−** Requires backend work, which is exactly what this project is trying to avoid.

### Recommendation

**Ship Design A.** Its limitation (final binding) is bounded and controllable via the
per-actor in-flight window (§9.3), and Ray Data has run the same push/least-loaded
design in production for years
(`python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`).
Design B should be kept as a documented alternative for the specific case of many
small results with highly skewed durations; Design C is the long-term convergence
point and should be listed as future work, not v1 scope.

Everything below specifies Design A.

---

## 6. Detailed design

### 6.1 Components

```
ray.pool_tasks.remote(...)          # decorator -> PooledFunction
PooledFunction                      # holds default options; .options(), .remote(), .bind()
PoolKey                             # normalized (resources, env, placement) identity
PoolRegistry                        # PoolKey -> TaskPool, per job, per driver/worker
TaskPool                            # actor set + dispatch policy + autoscaling
_TaskWorker                         # the @ray.remote class the pool instantiates
```

`_TaskWorker` is a single actor class with one public method:

```python
@ray.remote
class _TaskWorker:
    def __ray_execute__(self, header: bytes, *flat_args):
        # header = (function_descriptor, arg_layout, num_returns, ...)
        ...
```

Every pooled task in the cluster is an invocation of `_TaskWorker.__ray_execute__`.
Per-call `name=` is set to the user's function name so task events stay readable.

### 6.2 Pool identity (`PoolKey`)

A `PoolKey` is the normalized set of options that **cannot** vary per actor task and
therefore must be baked into the actor:

```
PoolKey = (
  num_cpus, num_gpus, memory, object_store_memory, resources(frozen),
  accelerator_type,
  runtime_env (serialized, canonicalized),
  scheduling_strategy (incl. placement group id + bundle index),
  label_selector, fallback_strategy,
  max_concurrency, isolation_mode,
  explicit user `pool=` name if given,
)
```

Rules:

- **P1.** Two invocations with equal `PoolKey` share a pool. Unequal keys get separate
  pools. Pools are created lazily on first submission.
- **P2.** `PoolKey` is computed after applying the same defaults the task path applies:
  `num_cpus` defaults to **1** (task default, not the actor default of 0), and
  placement group capture (`_configure_placement_group_based_on_context`) is resolved
  at submit time so a task submitted inside a PG lands in a PG-scoped pool. This
  preserves T13.
- **P3.** Users may override sharing with `.options(pool="name")` to force a shared
  pool for functions with compatible shapes, or to isolate a hot function.
- **P4.** The number of distinct pools is bounded (`max_pools`, default e.g. 64); a
  submission that would exceed it raises rather than silently fragmenting the cluster.

*This is a fidelity/utilization tradeoff, not a free win — see §13/TR4.*

### 6.3 Function delivery

Do **not** pickle the user function into every call. Reuse the existing export path:

- **F1.** On first submission, `PooledFunction` exports itself exactly like
  `RemoteFunction` does: `FunctionActorManager.export()` writes the pickled function to
  the GCS function table under
  `(job_id, function_descriptor.function_id)`, including the
  `collision_identifier` used to detect redefinition
  (`python/ray/_private/function_manager.py`).
- **F2.** Each call passes only the `PythonFunctionDescriptor` in the header.
- **F3.** `_TaskWorker.__ray_execute__` resolves it via
  `FunctionActorManager.get_execution_info(job_id, function_descriptor)`, which already
  caches locally and blocks-with-timeout on a miss (`_wait_for_function`).
- **F4.** Tracing injection (`_inject_tracing_into_function`) must be applied at export
  time, matching `RemoteFunction._remote`.

Benefit: closure size, function versioning, redefinition detection, and
`load_code_from_local` all behave exactly as they do for tasks today.

### 6.4 Argument marshalling

- **A1.** Arguments are flattened with `ray._common.signature.flatten_args` against the
  user function's signature — the same call `RemoteFunction._remote` makes — and the
  flat list is passed as **positional** arguments to `__ray_execute__`.
  This is what preserves T5–T7: the core worker's dependency resolver sees each
  `ObjectRef` as a top-level argument and resolves it, refs nested in containers stay
  unresolved, and the inline-vs-put decision is unchanged.
- **A2.** `__ray_execute__` reconstructs `(args, kwargs)` with
  `ray._common.signature.recover_args` before calling the user function.
- **A3.** The header carries the arg layout and must itself be small enough to inline.

### 6.5 Submission path

```
PooledFunction.remote(*args, **kwargs):
  1. options   = merge(decorator options, .options() overrides)
  2. validate  = reject unsupported options per §7.6
  3. key       = PoolKey(options, current placement-group context)
  4. pool      = PoolRegistry.get_or_create(key)
  5. actor     = pool.select(args_hint)          # never blocks on cluster resources
  6. header    = (fd, layout, num_returns, task_name)
  7. ref(s)    = actor.__ray_execute__.options(
                     name=fn_name,
                     num_returns=num_returns,
                     enable_task_events=...,
                     _generator_backpressure_num_objects=...,
                 ).remote(header, *flat_args)
  8. pool.on_dispatch(actor, ref)                # bookkeeping for load + drain
  9. return ref / [refs] / ObjectRefGenerator
```

Step 5 must be non-blocking even when the pool is at `max_size` and every actor is
busy — it selects the least-loaded actor and enqueues into that actor's inbox
(see §9.3 for the in-flight window and §12/Q3 for the backpressure option).

### 6.6 Actor configuration

`_TaskWorker` actors are always created with:

| Option | Value | Why |
|---|---|---|
| `max_restarts` | `0` | Restart semantics must not leak into task semantics (NG1). A dead actor is replaced by a *new* actor, not restarted. |
| `max_task_retries` | `0` | NG1. |
| `allow_out_of_order_execution` | `True` | Restores T3 and prevents one task's slow dependency resolution from blocking every other task in the same inbox (`SequentialActorSubmitQueue` sends strictly by sequence number). |
| `max_concurrency` | `1` by default | CPU-parity: one task per actor at a time. `>1` is opt-in (§12/Q6). |
| `max_pending_calls` | `-1` by default | Pool enforces its own window; see §12/Q3. |
| `enable_task_events` | inherited from the task option | Observability parity. |
| resources / `runtime_env` / strategy | from `PoolKey` | §6.2. |

---

## 7. Normative semantics

Requirements are numbered so they can be turned into tests one-for-one.

### 7.1 Submission

- **S1.** `f.remote(...)` MUST return without waiting on any cluster resource, actor
  startup, or dependency resolution.
- **S2.** The returned value MUST be a genuine `ray.ObjectRef` (or `list[ObjectRef]`,
  or `ObjectRefGenerator`) usable with `ray.get`, `ray.wait`, `ray.cancel`, as a task
  or actor-method argument, and serializable across processes with the normal
  borrowed-reference rules.
- **S3.** Returned refs MUST be owned by the submitting worker/driver (satisfied
  automatically by Design A).
- **S4.** Execution order MUST NOT be guaranteed, including for tasks submitted to the
  same actor.
- **S5.** Calling the decorated function directly MUST raise `TypeError` with the same
  message shape as `RemoteFunction.__call__`.

### 7.2 Arguments

- **S6.** Top-level `ObjectRef` args MUST be dereferenced before the user function
  runs; refs nested inside containers MUST NOT be.
- **S7.** No pool actor may be occupied while a task's dependencies are unresolved.
  (Satisfied by the caller-side resolver plus `allow_out_of_order_execution=True`.)
- **S8.** A dependency-resolution failure (e.g. the arg's owner died) MUST fail the
  task with the same error type the task path produces today, and MUST NOT be retried.

### 7.3 Returns

- **S9.** `num_returns` MUST support `1`, `N`, `"dynamic"`, and `"streaming"`, with the
  same defaulting rule: `None` → `"streaming"` for generator functions, `1` otherwise.
- **S10.** `_generator_backpressure_num_objects` and `_num_objects_per_yield` MUST be
  forwarded to `submit_actor_task`.
- **S11.** A blocked streaming producer occupies its actor slot. With
  `max_concurrency=1` this is exactly today's behavior (a blocked producer occupies its
  worker), so no special handling is required — but the pool MUST count such an actor
  as busy for autoscaling and MUST NOT select it for new work.
- **S12.** Returning an `ObjectRef` MUST produce a ref-to-ref (no auto-deref).

### 7.4 Resources, placement, environment

- **S13.** `num_cpus` MUST default to 1.
- **S14.** All of `num_cpus`, `num_gpus`, `memory`, `object_store_memory`, `resources`,
  `accelerator_type`, `runtime_env`, `scheduling_strategy`, `label_selector`,
  `fallback_strategy` MUST be honored **as pool-actor properties** (§6.2), and the
  documentation MUST state that they are now *reserved for the actor's lifetime*
  rather than for the duration of one call.
- **S15.** Placement-group capture MUST be resolved at submit time so that a pooled
  task submitted from inside a PG-scheduled parent runs in that PG.
- **S16.** `CUDA_VISIBLE_DEVICES` inside a pooled task MUST reflect the actor's GPU
  allocation. Because the actor is long-lived, `ray.get_gpu_ids()` is stable across
  tasks on the same actor — a documented divergence from T19's per-call allocation.

### 7.5 Isolation and worker recycling

- **S17.** `max_calls=N` MUST be honored as "after this actor has executed N pooled
  tasks, drain and replace it".
- **S18.** The implicit `num_gpus>0 ⇒ max_calls=1` rule (T18) MUST NOT be inherited by
  default, because it would make a GPU pool pointless. Instead the layer MUST emit a
  one-time warning the first time a GPU-requesting function is submitted without an
  explicit `max_calls`, stating that the process is now reused and that CUDA state
  persists across calls. `max_calls=1` MUST remain available for users who need the
  old behavior.
- **S19.** The pool MUST NOT attempt to isolate process-global state between tasks
  beyond what `max_calls` provides (parity with T20).

### 7.6 Options that are not supported

- **S20.** `max_retries` and `retry_exceptions` MUST be rejected with a clear error
  when set to anything other than `0` / `False` (NG1). Silently ignoring them would be
  worse than failing, since callers would believe they have retry coverage.
- **S21.** `placement_group`, `placement_group_bundle_index`, and
  `placement_group_capture_child_tasks` are accepted only via the already-preferred
  `scheduling_strategy=PlacementGroupSchedulingStrategy(...)` form; the deprecated
  direct arguments MUST produce the same deprecation warning as today.
- **S22.** Any option not in the supported table MUST raise at decoration or
  `.options()` time, not at submit time.

### 7.7 Cancellation

- **S23.** `ray.cancel(ref)` on a task the pool has dispatched but the actor has not
  started MUST prevent execution (supported today by
  `ActorSubmitQueue::MarkTaskCanceled`), and `ray.get` MUST raise
  `TaskCancelledError`.
- **S24.** `ray.cancel(ref, force=False)` on a **running** pooled task sets the
  cancellation flag observable via `ray.get_runtime_context().is_canceled()`. It does
  **not** raise `KeyboardInterrupt` in the task body. This is a documented regression
  against T24 and MUST be called out in the API docs.
- **S25.** `ray.cancel(ref, force=True)` — see §12/Q5. Whatever is chosen, the
  behavior MUST be explicit; it MUST NOT silently degrade to `force=False`.
- **S26.** `recursive=True` MUST propagate to child tasks submitted from within the
  pooled task.

---

## 8. Failure model

This is the section the no-retry / no-lineage constraint governs. The rule is:
**every failure becomes an exception raised at `ray.get` / iteration time on the
affected refs, and nothing is ever re-executed.**

### 8.1 Application errors

- **E1.** An exception raised by the user function MUST surface as `RayTaskError`
  wrapping the original cause, re-raised via `as_instanceof_cause()` so that
  `except UserError:` at the call site still catches it.
- **E2.** The traceback MUST NOT contain pool-layer frames. Implementation: add the
  pool module path (e.g. `ray/pool_tasks/`) to the internal-frame markers used by
  `_is_hidden_internal_frame` (`python/ray/exceptions.py`), the same way Ray Data hides
  `ray/data/_internal/` frames. `__ray_execute__` must also avoid contributing a frame
  by re-raising with the original traceback preserved.
- **E3.** `RayTaskError.function_name` MUST be the user function's name, not
  `__ray_execute__`.

### 8.2 Actor (worker) death while a task is running

Ray reports actor death as `ActorDiedError` carrying an `ActorDeathCause`. The layer
MUST translate it into the task-shaped error the user expects (T23):

| Death cause | Error raised on `ray.get` |
|---|---|
| Worker process crash / segfault | `WorkerCrashedError` |
| Killed by the memory monitor | `OutOfMemoryError` |
| Node died / preempted | `NodeDiedError` (with `preempted` surfaced) |
| `runtime_env` setup failed | `RuntimeEnvSetupError` |
| Actor `__init__` raised | `RuntimeEnvSetupError` / `RayTaskError` from the init failure |
| Anything else | `ActorDiedError` passed through, with pool context appended |

- **E4.** The raised error MUST include the pool name, the actor id, and the node id,
  because "which of my 200 pool actors died" is otherwise unanswerable.
- **E5.** The layer MUST NOT re-execute the task under any of these causes.

### 8.3 Blast radius — the important consequence

Today, one crashed worker fails exactly **one** task (with `max_retries=0`). Under
Design A, a crashed actor fails **every task in that actor's inbox**, including tasks
that had not started and would have run fine elsewhere. And because the ref is already
bound to that actor task, the pool *cannot* re-dispatch them — a new dispatch would
mint a new ObjectRef that the caller does not hold.

Therefore:

- **E6.** The per-actor in-flight window `W` (§9.3) is exactly the blast radius of an
  actor crash. `W=1` gives parity with today at the cost of a submission round trip of
  idle time between tasks; larger `W` pipelines better and fails more.
- **E7.** The chosen `W` MUST be documented as a durability parameter, not just a
  performance knob, and the default is §12/Q4.
- **E8.** Errors raised for the collateral tasks MUST be distinguishable from the
  error raised for the task that was actually running, so that users can tell "my code
  crashed the worker" from "I was standing next to code that did". Proposal: a
  `TaskNotRunError` / `cause=` marker on the collateral refs.

### 8.4 Transient unavailability

- **E9.** `ActorUnavailableError` (transient RPC failure to a live actor) is
  *ambiguous*: the task may or may not have executed. Under NG1 the layer MUST NOT
  retry, so it MUST surface a failure. It MUST be a distinct, documented error type
  (not silently mapped to "crashed"), because at-most-once means the caller has to
  decide.

### 8.5 Object loss

- **E10.** An object whose owner died, or which was evicted and cannot be
  reconstructed, MUST raise `OwnerDiedError` / `ObjectLostError` unchanged. NG2 means
  there is no reconstruction path; the error message MUST NOT suggest one.

### 8.6 Pool-level failure containment

- **E11.** Repeated actor-creation failures (bad `runtime_env`, `__init__` raising)
  MUST trip a circuit breaker after N consecutive failures, after which pending
  submissions to that pool fail fast with the underlying cause rather than looping.
  Prior art: `DataContext.max_consecutive_actor_init_deaths` in the Ray Data actor
  pool.
- **E12.** A function that reliably crashes its worker MUST NOT cause unbounded actor
  churn. The circuit breaker in E11 applies to crash-on-execute as well, keyed by
  pool.
- **E13.** `ray.kill()` called by a user on a pool actor MUST be treated as a normal
  actor death (E4) and the actor replaced.

---

## 9. Pool lifecycle, sizing, and autoscaling

### 9.1 Creation

- **L1.** Pools are created lazily on first submission for a `PoolKey` and are scoped
  to the job by default. Actor names are namespaced by job id so two drivers do not
  collide.
- **L2.** Initial size: `min_size` (default 0) actors, plus enough actors to cover the
  current pending demand up to `max_size`.
- **L3.** `max_size` default: unbounded in terms of the pool's own policy, bounded in
  practice by cluster resources. The pool MUST NOT block submission when actors cannot
  be placed — pending actors are the mechanism that tells the autoscaler to add nodes
  (§9.4).

### 9.2 Actor selection policy

- **L4.** Default: least-in-flight, tie-broken by locality. Ray Data's `_ActorPool`
  keeps a per-node heap of alive actors ranked by `num_tasks_in_flight`; the same
  structure applies.
- **L5.** Locality: the pool maintains a `ref → producing node` map for refs produced
  by its own tasks, giving a free locality hint with no extra RPC. For refs it did not
  produce and that exceed a size threshold, it MAY call
  `ray.experimental.get_object_locations`. Locality MUST be defeasible — never leave an
  actor idle to preserve locality when the local actor's window is full.
- **L6.** An actor is ineligible for selection when it is draining (§9.5), when its
  in-flight window is full, or when it is not yet `ALIVE`.
- **L7.** If every actor's window is full, behavior is governed by §12/Q3.

### 9.3 In-flight window

- **L8.** Each actor accepts at most `W` outstanding pooled tasks
  (`max_concurrency + queue_depth`). `W` bounds both the pipelining benefit and the
  crash blast radius (E6).
- **L9.** The window MUST be enforced by the pool, not by `max_pending_calls`,
  because exceeding `max_pending_calls` raises `PendingCallsLimitExceeded` at submit
  time — a task-API-visible error that has no analogue today.

### 9.4 Autoscaling

- **L10.** The pool's target size is
  `clamp(ceil((pending + running) / tasks_per_actor), min_size, max_size)`.
- **L11.** Scale-up MUST create the actors even when the cluster has no room for them.
  Pending *actors* are visible to the cluster autoscaler as resource demand; a Python
  queue is not. This is how T15 is restored, and it is the reason `max_size` should be
  demand-driven rather than conservative.
- **L12.** Scale-up MUST be rate-limited and MUST respect the circuit breaker (E11).

### 9.5 Scale-down and draining

- **L13.** Under NG1, killing an actor destroys its queued tasks. Therefore the pool
  MUST NOT kill an actor that has any in-flight pooled task.
- **L14.** Scale-down is a two-phase drain: mark the actor ineligible for selection,
  wait for its in-flight count to reach zero, then kill. The transition MUST be
  race-free with respect to concurrent `select()` calls.
- **L15.** Idle actors are reclaimed after `idle_timeout_s` (default e.g. 60s). Pools
  with zero actors and no pending work are garbage collected after
  `pool_idle_timeout_s`.
- **L16.** On driver exit, non-detached pools and their actors MUST be torn down.
  In-flight tasks fail with owner-death semantics, matching today.

### 9.6 Nested submission

- **L17.** A pooled task that calls `g.remote()` MUST work. The `PoolRegistry` inside
  a `_TaskWorker` resolves pools by name via `ray.get_actor` with the job namespace, so
  a nested submission joins the same pool rather than creating a per-actor one.
- **L18.** Ownership of nested results follows the actor, exactly as nested task
  results follow the parent worker today.
- **L19.** The deadlock hazard from T16 is real and is the subject of §12/Q6.

---

## 10. Observability

- **O1.** `name=` on each `__ray_execute__` call MUST be the user function name so
  `ray list tasks` and the dashboard show meaningful names. The `type` will read
  `ACTOR_TASK`; whether to add a "pooled task" distinction is §12/Q8.
- **O2.** The layer MUST export pool metrics: per-pool pending count, in-flight count,
  actor counts by state (pending / alive / draining / dead), tasks completed, tasks
  failed by cause, locality hit rate, and actor-churn count. Without these, the
  `PENDING_NODE_ASSIGNMENT` debugging signal (T26) is simply gone.
- **O3.** `ray.get_runtime_context()` inside a pooled task MUST report
  `get_task_name()` and `get_task_function_name()` as the *user function*, not
  `__ray_execute__`. `get_assigned_resources()` returns the actor's resources; this
  divergence MUST be documented.
- **O4.** Logs from many pooled tasks interleave in one long-lived actor log file.
  Task-id log prefixes MUST be preserved so `ray logs task <id>` remains usable.
- **O5.** `RAY_PDB`-style breakpoint-on-submit (T28) is not carried by
  `submit_actor_task`. v1 MUST document this as unsupported; adding a
  `debugger_breakpoint` field to the actor-task path is a small, separable core change.
- **O6.** If any pool state is exposed over a dashboard HTTP endpoint, `runtime_env`
  in the `PoolKey` MUST be redacted for browser-originated requests — `env_vars`
  routinely carries credentials.

---

## 11. Performance model

Expected relative to today's task path:

- **Better:** submission latency and throughput for small tasks — no
  `RequestWorkerLease` round trip to the raylet, no worker-pool startup on the critical
  path once warm. This is the main performance argument for the whole design.
- **Better:** warm-process reuse for functions with expensive imports or GPU context
  setup (at the cost of S18's isolation change).
- **Worse:** utilization under heterogeneous resource shapes — every distinct
  `PoolKey` reserves its own actors for their lifetime, versus per-task allocation
  today (§13/TR4).
- **Worse:** tail latency under skewed task durations — no work stealing (§5, Design A).
- **Neutral-to-worse:** memory — long-lived actors keep their heap; today's workers are
  recycled by `max_calls` and worker-pool pressure.

A benchmark suite MUST cover: (1) 100k trivial tasks submit-to-complete, (2) skewed
duration mix, (3) heterogeneous resource shapes, (4) large-argument fan-in,
(5) nested/recursive parallelism, (6) crash-under-load.

---

## 12. Open questions — decisions required before implementation

These are ordered by how much of the rest of the design they move.

### Q1. Binding time: Design A, B, or a hybrid? *(blocks everything)*

Recommendation: **A**. But this decision should be made explicitly, because choosing A
means accepting permanently: no work stealing, no re-dispatch of never-started tasks,
and a crash blast radius equal to the in-flight window. If any of those are
unacceptable, the answer is B (pay a hop + a copy) or C (change the core), and there is
no third option — the absence of a fulfillable-promise primitive in the object store is
load-bearing here.

### Q2. Is this a replacement for `ray.remote`, or a parallel API?

- (a) **Parallel opt-in namespace** first (e.g. `ray.pool_tasks.remote`), promote later.
- (b) A flag that reroutes `ray.remote` functions (`RAY_TASKS_ON_ACTORS=1`).
- (c) Direct replacement.

Recommendation: (a) for v1, (b) for validation at scale, (c) only after §13's
regressions are closed. The no-retry constraint alone makes (c) a breaking change for
every existing user, since `max_retries` defaults to 3 today.

### Q3. What happens when every actor's in-flight window is full?

- (a) **Ignore the window and keep pushing** — matches T4 (submission never blocks) but
  makes the window meaningless and unbounds the blast radius.
- (b) **Block `.remote()`** until a slot frees — breaks T1's non-blocking contract and
  can deadlock when the blocked submitter is itself a pooled task.
- (c) **Raise** (`PendingCallsLimitExceeded`-style) — explicit, but a new failure mode
  with no analogue in the task API.
- (d) **Soft window:** the window governs *selection preference*, and overflow is
  admitted to the least-loaded actor anyway, with a metric and a warning.

Recommendation: (d) by default with (b)/(c) available as an explicit
`backpressure="block"|"raise"` option. Note (b) is genuinely dangerous inside pooled
tasks for the same reason as Q6.

### Q4. What is the default in-flight window `W`, i.e. the default blast radius?

This is the sharpest consequence of the no-retry constraint (§8.3).

- `W = 1`: exact parity with today (one crash kills one task), but each actor idles
  for one submission round trip between tasks.
- `W = max_concurrency + 1`: one queued task keeps the actor busy across the gap;
  a crash kills up to 2 tasks.
- `W` large (Ray Data-style): best throughput, worst blast radius.

Recommendation: `W = max_concurrency + 1` as the default, with the durability
implication documented and `W` user-tunable. **Decide explicitly**, because raising the
default later is a silent durability regression.

### Q5. `ray.cancel(force=True)` — what does it mean?

- (a) **Raise `ValueError`** (what actor tasks do today). Honest, but breaks T24.
- (b) **Kill the actor.** Achieves the effect, but collaterally fails every other task
  in that actor's inbox — under NG1 those are gone. Only defensible when `W == 1`.
- (c) **Emulate in the execution wrapper:** run the user function on a worker thread
  and inject an async exception on cancel. Unreliable in C extensions and in blocking
  syscalls, and incompatible with `max_concurrency=1` semantics.

Recommendation: (a) by default; (b) as an explicit `pool_kill_on_force_cancel=True`
opt-in that is rejected unless `W == 1`. Also note `force=False` is weaker than today
(S24): it sets a flag rather than raising, so cooperative cancellation requires user
code to poll `is_canceled()`.

### Q6. Nested `ray.get` deadlock — how is T16 replaced? *(highest-risk correctness gap)*

Tasks release CPU when blocked in `ray.get`; **actors do not**
(`WorkerContext::ShouldReleaseResourcesOnBlockingCalls` returns false for actor calls).
A fixed-size pool where every actor blocks on a child task is the classic thread-pool
deadlock, and divide-and-conquer workloads (`doc/source/ray-core/tasks/nested-tasks.rst`)
hit it immediately.

Options:

- (a) **Elastic pool on blocking:** wrap `ray.get`/`ray.wait` inside `_TaskWorker` so a
  task notifies the pool before blocking; the pool grows past `max_size` while any
  actor is blocked and shrinks after. This re-implements the raylet's release-on-block
  at the Python layer. Requires intercepting `ray.get` in pooled tasks — intrusive but
  contained, and it is the only option that preserves today's programming model.
- (b) **Permit-based concurrency:** actors run with `max_concurrency = K` and a
  semaphore of K permits; a task releases its permit around a blocking `ray.get`.
  Closest analogue to the raylet's behavior, but forces multi-threaded execution
  (GIL contention, thread-unsafe user code, no per-task GPU assignment).
- (c) **Require async:** nested submissions must `await ref` on an async pool actor.
  Clean and deadlock-free, but changes the user-facing programming model.
- (d) **Document the hazard** and require `max_size >= recursion depth`. Cheapest,
  and a real regression versus today.

Recommendation: (d) for a v1 preview **with loud documentation and a deadlock
detector** (if all actors are blocked in `ray.get` on refs produced by this pool for
>N seconds, log an error naming the cycle), then (a) as the target. Do not ship the
API broadly without solving this — it is the single behavior most likely to turn
"tasks on actors" into a user-visible downgrade.

### Q7. Pool granularity: how aggressively do we share actors across functions?

Sharing amortizes startup; isolating preserves resource semantics. Sub-questions:

- Do two functions with identical `PoolKey` but very different runtimes share? (Yes by
  default — but a long function then delays a short one, and there is no work
  stealing.)
- Should `num_cpus=1` and `num_cpus=2` functions ever share a `num_cpus=2` actor
  (over-provisioning the small one)? Cheaper on fragmentation, wrong on accounting.
- Should the pool key include the function itself, opt-in via `pool=`?

Recommendation: exact-shape keying by default (P1), explicit `pool=` override (P3),
and a hard cap on distinct pools (P4). Revisit after measuring fragmentation.

### Q8. Do pooled tasks need their own task type in task events?

Reporting them as `ACTOR_TASK` keeps the core unchanged but makes
`ray summary tasks`, the dashboard, and every existing task dashboard read
differently. Adding a `POOLED_TASK` type (or a `pool_id` label on task events) is a
small core/protobuf change that buys back most of T26. Decide whether "no backend
changes" is absolute or whether observability is a permitted exception.

### Q9. Are pools per-job, per-driver, or detached/cluster-wide?

Per-job is the safe default (§9.1) and matches task semantics: a task's worker belongs
to a job. Cluster-wide detached pools would let unrelated jobs share warm actors —
attractive for short jobs, but it breaks job-scoped `runtime_env`, job-scoped logging,
and the ownership story for objects. Recommendation: per-job in v1; treat cross-job
pools as a separate proposal.

### Q10. What happens to `max_calls`' implicit GPU behavior (T18)?

Recommendation is S18 (do not inherit `max_calls=1`, warn once). The alternative —
honoring it — makes GPU pools recycle an actor per task, which costs more than today's
task path. Confirm that silently changing this default is acceptable, since it changes
the isolation guarantee for GPU code that never asked for it.

### Q11. Should `.bind()` / the DAG API be supported in v1?

`RemoteFunction.bind()` builds a `FunctionNode` that ultimately calls `_remote`.
Supporting it is mostly wiring, but DAG execution has its own scheduling assumptions.
Recommendation: raise `NotImplementedError` in v1 and scope it separately.

### Q12. How is at-most-once communicated to users?

Today's default is `max_retries=3`; a lot of production Ray code implicitly relies on
it. Options: reject `max_retries` outright (S20), require an explicit
`at_most_once=True` acknowledgement on the decorator, or emit a one-time warning per
job. Recommendation: S20 plus a prominent doc section, and make the error message name
the alternative (`retry in application code`).

---

## 13. Key tradeoffs

| # | Tradeoff | Choosing one way | Choosing the other |
|---|---|---|---|
| TR1 | **Binding time** (Q1) | *Push:* native refs, zero copies, trivial failure plumbing — but final placement, no stealing, no re-dispatch | *Broker:* deferred placement and safe re-dispatch — but +1 hop, +1 full copy of every result, new failure domain |
| TR2 | **In-flight window** (Q4) | *Small:* crash kills ~1 task; actor idles between tasks | *Large:* full pipelining; a crash destroys everything queued, unrecoverably under NG1 |
| TR3 | **Actor concurrency** | *1:* CPU-parity, per-actor GPU assignment, no GIL contention | *>1:* survives nested blocking, higher throughput for IO-bound work, but breaks resource accounting and thread-safety assumptions |
| TR4 | **Pool granularity** (Q7) | *Exact shape per pool:* faithful resource semantics | *Shared/coarse pools:* better warm reuse and less fragmentation, but resources are over- or under-accounted |
| TR5 | **Warm reuse** | *Reuse:* the entire point — amortized imports, CUDA contexts, model weights | *Recycle:* isolation parity with today's GPU tasks, but throws away the benefit |
| TR6 | **Autoscaler visibility** | *Eager actor creation for pending demand:* the autoscaler sees demand and adds nodes | *Lazy creation:* no wasted actors, but the queue is invisible and the cluster never grows |
| TR7 | **Scheduling in Python** | *Full control:* pool policies, custom locality, no core changes | *Loses:* raylet spillback, PG-aware bin packing, resource-based admission, backlog reporting, and the `PENDING_NODE_ASSIGNMENT` diagnostic |
| TR8 | **Latency vs. utilization** | Pool avoids the worker-lease round trip → much faster small tasks | Long-lived actors hold resources they are not currently using |
| TR9 | **No retries** (NG1) | Simpler, deterministic, no duplicate side effects, no lineage bookkeeping | Every crash is user-visible; workloads that relied on `max_retries=3` must add application-level retry |
| TR10 | **Cancellation fidelity** (Q5) | Honest weak cancel | Kill-the-actor strong cancel with unrecoverable collateral damage |

---

## 14. Delivery plan

1. **M0 — Skeleton.** `PooledFunction`, `PoolKey`, `PoolRegistry`, `_TaskWorker`,
   function export/resolution (§6.3), arg marshalling (§6.4). Single fixed-size pool,
   `num_returns=1` only. Proves S1–S8.
2. **M1 — Returns and errors.** `num_returns=N`/`dynamic`/`streaming`, error
   translation and traceback stripping (§8.1–8.2). Proves S9–S12, E1–E5.
3. **M2 — Resources and placement.** Full `PoolKey`, PG capture, scheduling
   strategies, `runtime_env`. Proves S13–S16.
4. **M3 — Lifecycle.** Autoscaling, draining, idle reclamation, circuit breakers.
   Proves L10–L16, E11–E13.
5. **M4 — Semantics gaps.** Cancellation policy (Q5), nested-submission deadlock
   detector (Q6), observability and metrics (§10).
6. **M5 — Hardening.** Benchmarks (§11), chaos tests, and a documented parity matrix
   listing every accepted divergence.

## 15. Test plan

Parity tests, driven off the §7 requirement numbers:

- **Semantics:** run a large slice of `python/ray/tests/test_basic*.py`-style task
  tests against the pooled implementation via a parametrized fixture, minus the
  retry/lineage tests, which must be replaced by explicit at-most-once assertions.
- **Errors:** for each row of §8.2's table, kill/crash/OOM an actor and assert the
  exact exception type, that the traceback contains no pool frames, that
  `except UserError:` still catches, and that the function did not run twice.
- **Blast radius:** with window `W`, submit `W` tasks to one actor, crash it mid-first
  task, and assert exactly `W` failures with the collateral tasks distinguishable
  (E8) and never re-executed.
- **Dependency HOL:** submit a task whose argument ref resolves after 10s, then submit
  100 fast tasks to the same actor; assert the fast tasks complete first (S7 /
  `allow_out_of_order_execution`).
- **Nested parallelism:** the quicksort example from
  `doc/source/ray-core/doc_code/pattern_nested_tasks.py` at a recursion depth greater
  than the pool size — currently expected to deadlock; the test asserts whichever
  behavior Q6 selects, and asserts the detector fires.
- **Autoscaling:** a burst of pending tasks must produce pending *actors* visible in
  cluster resource demand (L11), and idle actors must drain without failing tasks
  (L13–L14).
- **Cancellation:** queued cancel, running cancel, `force=True` per Q5, and recursive
  cancel of nested pooled tasks.
- **Determinism of at-most-once:** a task with a side effect (append to a detached
  counter actor) under injected worker crashes must never record two executions.
