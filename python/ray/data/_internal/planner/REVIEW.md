# Review instructions: physical planning

Checks for changes under `python/ray/data/_internal/planner/`
(logical-to-physical translation, exchange/shuffle planning, checkpointing),
in addition to `python/ray/data/REVIEW.md`.

## What Important means here

Important findings are translations that change semantics or lose plan
information (ray_remote_args, target block sizes, operator options), and
exchange plans that can lose or duplicate rows.

## Always check

- Every logical operator reachable from the public API has a physical
  translation; a new logical op without one must fail loudly at planning
  time, not at execution time.
- Translation preserves all operator options: `ray_remote_args`, concurrency,
  batch/block size targets, and `DataContext` overrides survive into the
  physical plan.
- Exchange/shuffle specs (`exchange/`) preserve total row count and schema
  across split/merge; partition boundary math handles empty and skewed
  partitions.
- Checkpoint planning (`checkpoint/`) keeps plan fingerprints/identities
  stable: an unrelated change must not silently invalidate existing
  checkpoints, and an incompatible change must version them explicitly.
- On restore, checkpointing should correctly eliminate already processed data to avoid repeated/wasted work.
