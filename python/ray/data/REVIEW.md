# Review instructions: Ray Data

Checks for changes under `python/ray/data/`. Subsystem-specific rules live in
`REVIEW.md` files next to the code (`_internal/execution/`, `_internal/logical/`,
`_internal/planner/`, `_internal/datasource/`, `_internal/datasource_v2/`,
`datasource/`).

## What Important means here

Reserve Important for findings that would:

- Lose, duplicate, or corrupt user data (dropped rows, wrong ordering where
  ordering is guaranteed, silent schema/type changes).
- Break the public API or change documented behavior without a deprecation
  cycle.
- Hang or deadlock a pipeline, or leak object store memory / actors / tasks.
- Regress performance on a per-row or per-block hot path (anything called once
  per block or more).
- Introduce a security issue (pickled object columns from external data,
  credentials in logs or error messages).
- Incur deterimental amounts of serialization, network cost that would impact overall throughput

Style, naming, and refactoring suggestions are Nit at most.

## Always check

- New datasources and formats are implemented in `_internal/datasource_v2/`.
  The v1 tree (`_internal/datasource/`) is deprecated and being ported to v2;
  a new datasource added under `_internal/datasource/` is Important.
- Public API changes (`dataset.py`, `read_api.py`, `grouped_data.py`,
  `iterator.py`, `expressions.py`, `aggregate.py`) carry the right annotation
  (`@PublicAPI`, `@DeveloperAPI`, `@Deprecated`) and a docstring with an
  example. New parameters must be optional with backward-compatible defaults.
- Renamed, moved, or removed public APIs add a redirect in
  `doc/redirects/current.yaml` (repo-wide rule; easy to miss from this
  directory).
- New knobs go through `DataContext` (`context.py`), not module-level
  environment variable reads or new global state.
- Changes keep execution lazy and streaming: no eager `ray.get` /
  materialization of all blocks where a streaming path exists.
- Arrow data is sliced zero-copy where possible; flag unnecessary
  `to_pandas()` / `to_pylist()` conversions or block copies on hot paths.
- Edge cases: empty blocks, empty datasets, missing columns, null values, and
  tensor/extension-type columns are handled, not just the happy path.
- New behavior has tests; tests reuse shared fixtures
  (e.g. `ray_start_regular_shared`) instead of starting a fresh cluster.

## Do not report

- Anything pre-commit/CI already enforces: formatting, import order, lint.
- Missing type annotations in `_internal/` code that predates the change.
