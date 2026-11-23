# Review instructions: datasources (v2)

Checks for changes under `python/ray/data/_internal/datasource_v2/`, in
addition to `python/ray/data/REVIEW.md`. The layout is three directories —
`interfaces/` (abstract classes, imports nothing else here), `common/`
(format-independent implementations, imports only `interfaces/`) and
`formats/<name>/` (one folder per format, may import both). `README.md` in
this directory describes how a read works; read it before reviewing.

**This is the current datasource architecture.** The v1 tree
(`_internal/datasource/`) is deprecated and being ported here; all new
datasources and formats must be implemented in this directory.

## What Important means here

Important findings are ones that change what a read returns or make it
unusable at scale:

- Rows lost, duplicated, or silently reordered — most often a pushdown that
  skips data it cannot prove is irrelevant.
- A block whose schema differs from the schema the plan inferred.
- Remote code execution through deserialized data, or credentials in logs,
  exception messages, or `repr`s.
- Work that belongs in a task running on the driver, or per-file metadata
  requests that make job startup scale with file count.
- One bad file failing an entire read.

Interface-conformance nits and naming are Nit.

## Pushdowns

A scanner's pushdown mixins are a promise the optimizer believes, so the
reader has to keep it.

- **Pruning is conservative.** A file, row group, or stripe is skipped only
  when its metadata *proves* no row can match. Missing, partial, or unusable
  statistics mean the piece is read, never skipped.
- **A new pushdown never disables or weakens an existing one.** Adding
  `SupportsLimitPushdown` to a scanner that already prunes columns must not
  drop the column pruning, and `push_*` methods return a scanner that still
  carries everything previously pushed.
- **A scanner never reports a pushdown its reader doesn't enforce.** Indexing
  skips whole pieces; the reader is what makes the result correct. Each
  scanner's docstring states which of column pruning, filter, partition, and
  limit pushdown it implements and which it doesn't yet, and the format's file
  header says the same.
- **Push predicates as far down as possible, and prefer pruning during
  listing.** A predicate resolvable from path or log metadata should prune in
  the `FileIndexer` so the file is never opened, not be re-evaluated per row
  in the reader.
- **A read that needs no stored column still returns every row.** `count()`,
  or selecting only `__path` or partition columns, must not prune the column
  list down to nothing and yield zero rows or empty blocks.

## Listing, partitioning, and task sizing

- A path listed twice is read twice. Deduplicating listed paths changes the
  row count and is a behavior change, not a cleanup.
- An indexer that reads per-file metadata yields manifest rows as it goes
  rather than collecting them all, and stops once a pushed limit is
  satisfied.
- Listing propagates file sizes and metadata downstream so readers and
  scanners don't re-stat files — no per-file HEAD request on open.
- Read tasks are sized by **peak decoded memory**, not bytes on disk. A size
  that can't be known is `None`, not `0`; `0` reads as "free" to the
  partitioner and packs unbounded work into one task.
- `FilePartitioner` (how manifest rows are regrouped into read tasks) and
  partition *columns* (hive-style directory partitioning) are separate
  concerns — don't conflate them in one change. Hive-style and unpartitioned
  layouts are both handled, and synthesized partition-column values don't
  silently collide with columns stored in the file.
- Schedule as little as possible on the driver. Prefer doing the work in
  `ListFiles` or `ReadFiles` tasks; a driver-side open, stat, or log replay
  that scales with file or partition count is Important.

## Failure isolation and schema

- A bad file fails only its own read. One unreadable or malformed file must
  not fail the whole dataset where `ignore_missing_paths` or per-file error
  handling applies.
- Every block a reader yields matches the schema the plan inferred. A file
  that can't be cast to it fails its own read with an error naming the file
  **and** the column, not a bare pyarrow cast error.

## Structure and compatibility

- New formats implement the contracts in `interfaces/` rather than bypassing
  them; shared behavior belongs in `common/`, not copied into each format. A
  new format adds one folder under `formats/` and changes nothing in
  `interfaces/`.
- Shared interfaces use only types every format can supply. A signature that
  needs a Parquet footer, an Arrow schema, or a row-group index belongs on
  the format, not in `interfaces/`.
- A format moves to V2 behind `DataContext.use_datasource_v2`, not a new
  per-format flag. A temporary per-format flag needs a stated removal plan in
  the PR description.
- Code and tests work on every version of the underlying library Ray
  supports, not just the newest. Gate on capability (`inspect.signature`,
  feature probe) rather than a parsed version string where possible, and make
  sure the test actually exercises the installed version instead of
  monkeypatching a version number.
- A datasource ported from v1 preserves the v1 behavior users depend on
  (parameters, partitioning, metadata, error behavior) or calls out the
  intentional differences in the PR description.
- A port must not leave new functionality behind in the v1 tree: follow-up
  features for a ported datasource land here, not in
  `_internal/datasource/`.

## Security

- A read never unpickles untrusted bytes. Arrow data from external input can
  carry `ray.data.arrow_pickled_object` columns that execute code on access.
  Check against whatever guard is in `master`: today that's a
  `raise_on_pickle_object_columns(table)` call on each table before it is
  yielded or materialized; this is being centralized, so once that lands the
  per-call check is no longer the thing to look for. Either way, a read path
  that lets such a column reach a block is a remote code execution bug.
- Credentials and connection details never appear in logs or exception
  messages; filesystems and connection factories pickled into tasks are
  serializable and carry no secret in their `repr`.
