# Review instructions: datasources (v2)

Checks for changes under `python/ray/data/_internal/datasource_v2/` (listing,
readers, scanners, chunkers, format implementations), in addition to
`python/ray/data/REVIEW.md`.

**This is the current datasource architecture.** The v1 tree
(`_internal/datasource/`) is deprecated and being ported here; all new
datasources and formats must be implemented in this directory.

## What Important means here

Same as v1 datasources: data loss/corruption, remote code execution via
deserialized data, credential leaks, and per-file metadata overhead are
Important. Interface-conformance nits are Nit.

## Always check

- Any `pyarrow.Table` deserialized from external input calls
  `raise_on_pickle_object_columns(table)` before it is yielded or
  materialized — skipping it is a remote code execution bug.
- New formats implement the contracts in `interfaces/` rather than bypassing
  them; shared behavior belongs in `common/`, not copied into each format.
- A datasource ported from v1 preserves the v1 behavior users depend on
  (parameters, partitioning, metadata, error behavior) or calls out the
  intentional differences in the PR description.
- A port must not leave new functionality behind in the v1 tree: follow-up
  features for a ported datasource land here, not in
  `_internal/datasource/`.
- Listing (`listing/`) propagates file sizes and metadata downstream so
  readers/scanners don't re-stat files (no per-file HEAD requests on open).
- Readers and scanners stream blocks incrementally; chunkers (`chunkers/`)
  produce batches near the target size, including the last/partial chunk and
  empty-file cases.
- Partitioners (`partitioners/`) handle hive-style and unpartitioned layouts,
  and partition-column values don't silently collide with file columns.
- Credentials and connection details never appear in logs or exception
  messages; filesystems passed to tasks are serializable.
