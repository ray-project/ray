# Datasource V2

The read path behind `ray.data.read_*` when
`DataContext.use_datasource_v2 = True`. A datasource says *where* the data is
and *how* to decode it; the framework does listing, planning, pushdown, task
grouping and execution. Parquet is the only format on it today and is the
reference implementation.

## Layout

```
interfaces/       abstract classes and the types in their signatures. Imports nothing else here.
common/           format-independent implementations. Imports only interfaces/.
formats/<name>/   one folder per format. May import both.
```

A new format adds one folder under `formats/` and changes nothing in
`interfaces/`. If you find yourself editing `interfaces/`, stop and ask
whether the hook is really missing or the format is doing the framework's job.

## How a read works

1. `read_api._read_datasource_v2` asks the datasource for a `FileIndexer`
   (`_get_file_indexer()`) and a `FilePartitioner`
   (`get_file_partitioner(hints=...)`).
2. `ListFiles` tasks run the indexer; it yields `FileManifest` blocks (paths,
   sizes, optional per-chunk metadata). Pruners drop files by extension or
   partition value before any file is opened.
3. The partitioner groups manifest rows into read units sized for one task.
4. The datasource's `Scanner` receives pushdowns (columns, filters, limit,
   partitions) from the optimizer and hands back a configured `Reader`.
5. `ReadFiles` tasks call `Reader.read(manifest)`, which yields
   `pyarrow.Table`s. `SynthesizedColumn`s (`path`, `row_hash`, ...) are
   appended here from a `ReadUnitPosition`.

## Adding a format

Pick a base class: `FileDataSourceV2` when the framework finds the files by
walking a filesystem, `DataSourceWithMetadata` when a catalog or table
metadata knows them. Then implement, in `formats/<name>/`:

| Piece | Extend | Must provide |
| --- | --- | --- |
| `<Name>DatasourceV2` | `FileDataSourceV2` | `paths`, `filesystem`, `_get_file_indexer`, `get_file_partitioner`, `schema_needs_file_sample`, `infer_schema`, `create_scanner` |
| `<Name>Scanner` | `common.ArrowFileScanner` | `read_schema`, `create_reader` (column/filter/limit pushdown come free; override `push_filters` to reject what the decoder cannot evaluate) |
| `<Name>FileReader` | `common.FileReader` | the per-file decode; partition columns and synthesized columns are appended for you |

Reuse before writing. `common.NonSamplingFileIndexer` lists any filesystem.
For a format without per-chunk metadata, the partitioner is two lines:

```python
def get_file_partitioner(self, *, hints=None):
    return RoundRobinPartitioner(
        SamplingInMemorySizeEstimator(self._make_reader()), hints=hints
    )
```

`SamplingInMemorySizeEstimator` reads one file per listing task to learn the
on-disk to in-memory ratio. Write a footer-style indexer or partitioner only
when the format has per-chunk metadata worth planning on (see
`formats/parquet/footer_file_indexer.py` and `common/online_bin_packer.py`).

Wire it up in `read_api.py` under the `use_datasource_v2` branch of the
matching `read_*` function.

## Tests and checks

Unit tests that need no Ray cluster go in `tests/unit/datasource_v2/`;
anything that calls `ray.data.read_*` goes in `tests/datasource/`. Every test
file ends with the `if __name__ == "__main__": sys.exit(pytest.main(...))`
block or the `pytest_format` lint fails. Before pushing, run `ruff`, `black`
and `pydoclint` at the versions pinned in `.pre-commit-config.yaml`, and
`pyrefly` via `ci/lint/pyrefly-check.sh`.

## Conventions reviewers hold you to

- Pass arguments by keyword at every hook call; an override may take
  `**kwargs`.
- Do not use "batch" in a name; in Ray Data it already means two other things.
- Name a value for exactly what it is (`unprocessed_rg_ids`, not
  `all_rg_indices`; `rows_before`, not `offset`).
- A `pa.Table` deserialized from external input must pass through
  `raise_on_pickle_object_columns` before it is yielded.
