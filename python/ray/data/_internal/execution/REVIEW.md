# Review instructions: execution

Checks for changes under `python/ray/data/_internal/execution/` (streaming
executor, physical operators, resource manager, backpressure, autoscaling),
in addition to `python/ray/data/REVIEW.md`.

## What Important means here

Important findings here are ones that can hang the executor, mis-account
resources, leak object store memory, or silently drop/duplicate bundles.
Operator-local refactors and naming are Nit at most.
Ensure that the streaming executor loop is not slow, as a slow loop reduces overall throughput by scheduling fewer tasks

## Always check

- The streaming executor control loop stays non-blocking: no `ray.get`,
  `ray.wait(timeout=None)` on unfinished work, sleeps, or other blocking calls
  on the scheduling path.
- `RefBundle` ownership is respected: `owns_blocks` is set correctly, and
  bundles are destroyed/freed exactly once. Flag paths where blocks can be
  neither consumed nor freed (object store leak) or freed twice.
- No reference outlives the bundle that owns it. `destroy_if_owned` only
  records the deallocation for memory tracing — blocks are reclaimed by Ray
  reference counting once no Python reference to the `ObjectRef` remains, so
  anything still holding one keeps the block in the object store after the
  executor has accounted it as freed. Check that an `ObjectRef` or `RefBundle`
  captured in a closure, a retry/backpressure buffer, a stats or metrics dict,
  or an operator's output queue is dropped (or the queue `clear()`ed) on the
  consume, cancel, and failure paths alike. Reference cycles are the same bug
  with a delay: a cycle through an operator, its bundles, or a captured `self`
  defers reclamation to the cyclic collector, so a leak shows up as memory
  that frees "eventually" under GC pressure rather than at consume time —
  prefer breaking the cycle over relying on `gc`.
- New/changed operators keep `OpRuntimeMetrics` and internal queue accounting
  consistent (inputs/outputs, object store bytes, pending task counts) —
  the resource manager and backpressure decisions are driven by these numbers.
- Backpressure policy and resource-budget changes are consistent with
  `resource_manager.py` budgets; a policy that can report "no progress is
  possible" needs a path that still schedules at least one task (no livelock).
- Shutdown, cancellation, and failure paths clean up: actors killed, pending
  tasks cancelled, iterators closed. Exceptions from tasks must propagate to
  the caller, not be swallowed.
- Thread safety: state shared between the executor thread and caller threads
  (progress, metrics, queues) is protected or documented as single-threaded.
- Autoscaler changes (`autoscaler/`, `../actor_autoscaler/`,
  `../cluster_autoscaler/`) include tests for both scale-up and scale-down
  decisions.
