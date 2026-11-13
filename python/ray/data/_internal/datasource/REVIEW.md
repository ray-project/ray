# Review instructions: datasources (v1 internal — DEPRECATED)

Checks for changes under `python/ray/data/_internal/datasource/` (legacy v1
read/write implementations), in addition to `python/ray/data/REVIEW.md`.

**This directory is deprecated.** We are porting all datasources to
`_internal/datasource_v2/`. Code here is in maintenance mode: bug fixes,
security fixes, and changes needed to keep existing formats working are fine;
new functionality is not.

## What Important means here

Important findings are data corruption or loss on read/write, remote code
execution via deserialized data, credential leaks, per-file overhead on
metadata-heavy paths (listing, size estimation) — and new development landing
in this deprecated tree (see below).

## Always check

- **No new datasources or formats here.** A PR adding a new datasource,
  format, or substantial new capability under this directory is Important:
  it belongs in `_internal/datasource_v2/`. Only bug fixes and maintenance of
  existing datasources are expected.
- A nontrivial feature addition to an existing v1 datasource deserves a
  comment asking whether the effort should go into porting that datasource to
  v2 instead of extending the deprecated implementation.
- Any `pyarrow.Table` deserialized from external input (Parquet, Arrow IPC, a
  service/library returning Arrow) calls `raise_on_pickle_object_columns(table)`
  before it is yielded or materialized — skipping it is a remote code
  execution bug.
- Credentials, connection strings, and auth tokens never appear in log lines,
  exception messages, or `repr`s (e.g. SQL connection params, cloud keys in
  URIs).
- Read tasks stream: yield blocks incrementally instead of materializing a
  whole file, and respect target block size where the format allows it.
- Metadata is honest: `estimate_inmemory_data_size`, row counts, and input
  file attribution are correct or `None`, never a guess presented as exact —
  downstream scheduling and `limit` pushdown rely on them.
- No redundant per-file remote calls (HEAD/stat/list) when the information is
  already available from listing; these dominate job startup on large
  directories.
- Filesystem objects passed into tasks are serializable (wrapped pyarrow
  filesystems), and retry logic retries only transient errors.
- Write paths are safe under task retries: filenames are unique per task
  attempt, partial output is not visible as committed data, and `on_write`
  callbacks handle failure/cleanup.
