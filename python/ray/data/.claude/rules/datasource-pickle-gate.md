---
paths:
  - "python/ray/data/_internal/datasource/**/*.py"
  - "python/ray/data/_internal/datasource_v2/**/*.py"
---
# Pickled-object columns in datasources (GHSA-2ch8-9c5v-84jf)

`ray.data.arrow_pickled_object` columns hold pickle bytes and unpickle on access
(`as_py`, `to_pylist`, `to_pandas`, `to_numpy`). Unpickling attacker bytes is remote
code execution, and `import ray.data` registers the type process-wide.

- Any read path that turns external bytes into a `pa.Table` (parquet, Arrow IPC, a
  service returning Arrow, a third-party reader such as lance/hudi/pyiceberg/HF
  datasets) must call `raise_on_pickle_object_columns(table)` from
  `ray.data._internal.object_extensions.arrow` right after the read and before any
  `yield`, `to_pylist`, `to_pandas` or `to_numpy`. Gating after materializing is
  too late.
- If a library the datasource delegates to reads parquet itself (lerobot reads
  `meta/tasks.parquet` with pandas), check those files' footers first; see
  `_raise_on_pickle_object_meta_parquet` in `lerobot_datasource.py`.
- Blocks built in-process from Python values (`DelegatingBlockBuilder`,
  `pyarrow_table_from_pydict`) may legitimately contain object columns. Do not gate
  those.
- Every gated datasource needs a reject test: plant a payload whose `__reduce__`
  runs `os.system("touch <marker>")`, wrap it with
  `ArrowPythonObjectArray.from_objects`, assert
  `pytest.raises(..., match="arrow_pickled_object")` and that the marker file was
  never created. Pattern:
  `tests/datasource/test_parquet.py::test_read_parquet_rejects_pickle_object_columns`.
- `tests/unit/datasource/test_pickle_gate_coverage.py` fails CI when a datasource
  module matches an Arrow-read pattern without referencing the gate.
