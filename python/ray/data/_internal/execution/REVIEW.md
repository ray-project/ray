# Review instructions: execution

Checks for changes under `python/ray/data/_internal/execution/` (streaming
executor, physical operators, resource manager, backpressure, autoscaling),
in addition to `python/ray/data/REVIEW.md`.

## What Important means here

Important findings here are ones that can hang the executor, mis-account
resources, leak object store memory, or silently drop/duplicate bundles.
Operator-local refactors and naming are Nit at most.
Ensure that the streaming executor loop is not slow since that would result in a reduction of overall throughput since it will schedule fewer tasks

## Always check

- The streaming executor control loop stays non-blocking: no `ray.get`,
  `ray.wait(timeout=None)` on unfinished work, sleeps, or other blocking calls
  on the scheduling path.
- `RefBundle` ownership is respected: `owns_blocks` is set correctly, and
  bundles are destroyed/freed exactly once. Flag paths where blocks can be
  neither consumed nor freed (object store leak) or freed twice.
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
