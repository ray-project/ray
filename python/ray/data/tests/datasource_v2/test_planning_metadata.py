import os
from collections import Counter
from contextlib import nullcontext
from dataclasses import replace

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    _read_parquet_metadata,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_COLUMN_NAME,
    FILE_METADATA_TYPE,
    FileManifest,
)
from ray.data._internal.logical.operators import ListFiles
from ray.data.context import DataContext
from ray.data.tests.schema_planning_utils import (
    UnknownSizeRecordingLocalFileSystem,
    read_schema_records,
    recording_filesystem,
)


@pytest.fixture
def planning_files(tmp_path, monkeypatch, request):
    for suffix, value in dict(
        min_files=0,
        max_tasks=2,
        batch_size=2,
        io_concurrency=2,
        num_cpus=0.5,
        metadata_budget_bytes=96,
        task_metadata_budget_bytes=32,
    ).items():
        monkeypatch.setattr(
            inference, f"_FULL_SCHEMA_INFERENCE_{suffix.upper()}", value, raising=False
        )
    paths = [
        str(tmp_path / f"{i}.parquet") for i in range(getattr(request, "param", 4))
    ]
    for i, path in enumerate(paths):
        pq.write_table(pa.table({"id": pa.array(range(i), type=pa.int64())}), path)
        timestamp = 1_600_000_000 + i
        os.utime(path, (timestamp, timestamp))
    return paths


def test_schema_batch_preserves_per_file_metadata_budget(planning_files):
    paths = planning_files[1:]
    filesystem = pafs.LocalFileSystem()
    infos = filesystem.get_file_info(paths)

    result = inference._read_schema_batch(
        paths,
        [info.size for info in infos],
        filesystem,
        None,
        "Parquet",
        DataContext.get_current(),
        2,
        read_metadata=_read_parquet_metadata,
        row_records=2,
        mtimes=[info.mtime_ns for info in infos],
    )
    assert [position for position, _, _ in result.schemas] == [0]
    metadata = result.metadata
    assert metadata is not None
    assert metadata.to_pylist() == [
        {"num_rows": 1, "mtime_ns": infos[0].mtime_ns},
        {"num_rows": 2, "mtime_ns": infos[1].mtime_ns},
        None,
    ]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_prelisted_files_have_unknown_output_schema(
    planning_files, ray_start_regular_shared_2_cpus, datasource_v2
):
    dataset = ray.data.read_parquet(planning_files, merge_schema=True, num_cpus=1)
    op = next(
        op
        for op in dataset._logical_plan.dag.post_order_iter()
        if isinstance(op, ListFiles)
    )
    assert op.prelisted_file_infos
    assert FILE_METADATA_COLUMN_NAME in op.prelisted_file_infos[0].column_names
    assert op.infer_schema() is None
    assert replace(op, prelisted_file_infos=[]).infer_schema() is None


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize("planning_files", [6], indirect=True)
@pytest.mark.parametrize(
    "num_cpus,misses,distributed",
    [
        pytest.param(1, [2, 4, 5], True, id="worker_budget"),
        pytest.param(0, [0, 1, 2, 3, 4, 5], False, id="driver_uncached"),
    ],
)
def test_only_worker_schema_planning_reuses_counts(
    tmp_path,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
    num_cpus,
    misses,
    distributed,
):
    trace = tmp_path / "reads.jsonl"
    dataset = ray.data.read_parquet(
        planning_files,
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=num_cpus,
    )
    before = read_schema_records(trace)
    assert Counter(r["path"] for r in before) == Counter(planning_files)
    assert all((r["pid"] != os.getpid()) == distributed for r in before)
    assert dataset.count() == 15
    after = read_schema_records(trace)[len(before) :]
    assert Counter(r["path"] for r in after) == Counter(
        planning_files[i] for i in misses
    )


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_worker_cache_survives_lineage_roundtrip(
    tmp_path,
    monkeypatch,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
):
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", 256)
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", 256
    )
    trace = tmp_path / "lineage.jsonl"
    dataset = ray.data.read_parquet(
        str(tmp_path),
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=1,
    )
    before = read_schema_records(trace)
    assert Counter(r["path"] for r in before) == Counter(planning_files)
    assert all(r["pid"] != os.getpid() for r in before)
    serialized = dataset.serialize_lineage()
    pq.write_table(pa.table({"id": [99]}), tmp_path / "appended.parquet")
    restored = ray.data.Dataset.deserialize_lineage(serialized)
    assert restored.count() == 6
    assert read_schema_records(trace) == before
    assert sorted(row["id"] for row in restored.take_all()) == [0, 0, 0, 1, 1, 2]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "mtime_known,misses",
    [pytest.param(True, [], id="mtime"), pytest.param(False, [0, 1], id="no_identity")],
)
def test_worker_cache_handles_unknown_file_size(
    tmp_path,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
    mtime_known,
    misses,
):
    trace = tmp_path / "unknown-size.jsonl"
    fs = pafs.PyFileSystem(
        pafs.FSSpecHandler(
            UnknownSizeRecordingLocalFileSystem(
                trace_path=str(trace), mtime_known=mtime_known, skip_instance_cache=True
            )
        )
    )
    paths = planning_files[:2]
    dataset = ray.data.read_parquet(paths, merge_schema=True, filesystem=fs, num_cpus=1)
    before = read_schema_records(trace)
    assert Counter(r["path"] for r in before) == Counter(paths)
    assert all(r["pid"] != os.getpid() for r in before)
    assert dataset.count() == 1
    after = read_schema_records(trace)[len(before) :]
    assert Counter(r["path"] for r in after) == Counter(paths[i] for i in misses)


@pytest.mark.parametrize(
    "stat_error,size_known,mtime_delta,expected,outcome",
    [
        pytest.param(None, True, 0, 999, nullcontext(), id="hit"),
        pytest.param(None, True, 10**9, 3, nullcontext(), id="same_size_new_mtime"),
        pytest.param(PermissionError, True, 0, 3, nullcontext(), id="stat_denied"),
        pytest.param(
            NotImplementedError, True, 0, 3, nullcontext(), id="stat_unsupported"
        ),
        pytest.param(
            AssertionError, False, 0, 3, nullcontext(), id="no_identity_no_stat"
        ),
        pytest.param(
            FileNotFoundError,
            True,
            0,
            None,
            pytest.raises(FileNotFoundError, match="test stat"),
            id="stat_not_found",
        ),
        pytest.param(
            OSError,
            True,
            0,
            None,
            pytest.raises(OSError, match="test stat"),
            id="stat_io_error",
        ),
    ],
)
def test_count_cache_validates_optional_stat(
    tmp_path, stat_error, size_known, mtime_delta, expected, outcome
):
    class StatFileSystem(pafs.LocalFileSystem):
        def get_file_info(self, paths):
            if stat_error is not None:
                raise stat_error("test stat")
            return super().get_file_info(paths)

    path = str(tmp_path / "data.parquet")
    pq.write_table(pa.table({"id": [1, 2, 3]}), path)
    info = pafs.LocalFileSystem().get_file_info(path)
    mtime = info.mtime_ns
    assert mtime is not None
    manifest = FileManifest.construct_manifest(
        paths=[path],
        sizes=[info.size if size_known else None],
        chunk_metadatas=[None],
        extra_columns=pa.table(
            {
                FILE_METADATA_COLUMN_NAME: pa.array(
                    [
                        {
                            "num_rows": 999,
                            "mtime_ns": mtime - mtime_delta if size_known else None,
                        }
                    ],
                    type=FILE_METADATA_TYPE,
                )
            }
        ),
    )
    with outcome:
        assert [
            m.num_rows
            for m in ParquetFileReader(filesystem=StatFileSystem()).read_metadata(
                manifest
            )
        ] == [expected]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
