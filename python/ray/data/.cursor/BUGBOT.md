# Bugbot Rules

## Rule: Pickled-object columns in Ray Data readers
- Look at changes under `python/ray/data/_internal/datasource/` and `python/ray/data/_internal/datasource_v2/`.
- Apply this rule when a changed read path produces a `pa.Table` or `RecordBatch` from data outside the process: parquet, Arrow IPC, ORC, a service returning Arrow (ClickHouse, BigQuery, Databricks), or a third-party reader (lance, hudi, pyiceberg, HF datasets).
- `ray.data.arrow_pickled_object` columns hold pickle bytes and run `pickle.load` on access (`as_py`, `to_pylist`, `to_pandas`, `to_numpy`). `import ray.data` registers the type process-wide, so yielding such a column from an attacker's file is remote code execution (GHSA-2ch8-9c5v-84jf).
- If the read path does not call `raise_on_pickle_object_columns(table)` from `ray.data._internal.object_extensions.arrow` right after the read and before any `yield`, `to_pylist`, `to_pandas` or `to_numpy`, post:

> ⚠️ **This read path hands externally deserialized Arrow data to users without rejecting pickled-object columns.**
>
> `ray.data.arrow_pickled_object` columns unpickle on access, which executes arbitrary code from untrusted files (GHSA-2ch8-9c5v-84jf). Call `raise_on_pickle_object_columns(table)` right after the read and before any yield or materialization, and add a reject test that plants an `Exploit.__reduce__` marker payload and asserts the marker file is never created (pattern: `tests/datasource/test_parquet.py::test_read_parquet_rejects_pickle_object_columns`). See `python/ray/data/.claude/rules/datasource-pickle-gate.md`.

- Do not post this message for blocks built in-process from Python values (`DelegatingBlockBuilder`, `pyarrow_table_from_pydict`, `pa.Table.from_pylist`); those may legitimately contain object columns.
- If a library the datasource delegates to reads parquet itself (for example lerobot reading `meta/tasks.parquet` with pandas), expect the datasource to check those files' schemas first, as `_raise_on_pickle_object_meta_parquet` in `lerobot_datasource.py` does.
