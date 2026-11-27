import os
from collections import Counter

import fsspec
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.logical.operators import ListFiles
from ray.data.expressions import col
from ray.data.tests.schema_planning_utils import (
    UnknownSizeRecordingLocalFileSystem,
    read_schema_records,
    recording_filesystem,
)


@pytest.fixture
def planning_files(tmp_path, monkeypatch):
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 2)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_REUSE_ROW_COUNT", 1)
    paths = [str(tmp_path / f"{i}.parquet") for i in range(4)]
    for i, path in enumerate(paths):
        pq.write_table(pa.table({"id": pa.array(range(i), type=pa.int64())}), path)
    return paths


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "num_cpus,enabled",
    [
        pytest.param(0, True, id="driver"),
        pytest.param(1, True, id="worker"),
        pytest.param(1, False, id="disabled"),
    ],
)
def test_planning_count_reuses_complete_total(
    tmp_path,
    monkeypatch,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
    num_cpus,
    enabled,
):
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_REUSE_ROW_COUNT", int(enabled)
    )
    trace = tmp_path / "reads.jsonl"
    dataset = ray.data.read_parquet(
        planning_files,
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=num_cpus,
    )
    listing = dataset._logical_plan.dag.input_dependencies[0]
    assert isinstance(listing, ListFiles)
    assert listing.prelisted_file_infos
    before = read_schema_records(trace)
    assert Counter(record["path"] for record in before) == Counter(planning_files)
    assert all((record["pid"] != os.getpid()) == (num_cpus > 0) for record in before)
    assert dataset.count() == 6
    after = read_schema_records(trace)[len(before) :]
    assert Counter(record["path"] for record in after) == (
        Counter() if enabled else Counter(planning_files)
    )


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_planning_count_survives_lineage_roundtrip(
    tmp_path, planning_files, ray_start_regular_shared_2_cpus, datasource_v2
):
    trace = tmp_path / "lineage.jsonl"
    dataset = ray.data.read_parquet(
        str(tmp_path),
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=1,
    )
    before = read_schema_records(trace)
    assert Counter(record["path"] for record in before) == Counter(planning_files)
    assert all(record["pid"] != os.getpid() for record in before)
    serialized = dataset.serialize_lineage()
    pq.write_table(pa.table({"id": [99]}), tmp_path / "appended.parquet")
    restored = ray.data.Dataset.deserialize_lineage(serialized)
    assert restored.count() == 6
    assert read_schema_records(trace) == before
    assert sorted(row["id"] for row in restored.take_all()) == [0, 0, 0, 1, 1, 2]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_planning_count_with_unknown_file_size(
    tmp_path, planning_files, ray_start_regular_shared_2_cpus, datasource_v2
):
    trace = tmp_path / "unknown-size.jsonl"
    fs = pafs.PyFileSystem(
        pafs.FSSpecHandler(
            UnknownSizeRecordingLocalFileSystem(
                trace_path=str(trace), skip_instance_cache=True
            )
        )
    )
    dataset = ray.data.read_parquet(
        planning_files, merge_schema=True, filesystem=fs, num_cpus=1
    )
    before = read_schema_records(trace)
    assert dataset.count() == 6
    assert read_schema_records(trace) == before


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "num_cpus", [pytest.param(0, id="driver"), pytest.param(1, id="worker")]
)
def test_planning_count_retries_footer_reads(
    tmp_path,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
    restore_data_context,
    num_cpus,
):
    class FailFirstOpen(pafs.FSSpecHandler):
        def __init__(self):
            super().__init__(fsspec.filesystem("file", skip_instance_cache=True))
            self.failed = False

        def open_input_file(self, path):
            if not self.failed:
                self.failed = True
                with open(tmp_path / "retry.txt", "a") as output:
                    output.write(f"{os.getpid()}\n")
                raise OSError("transient footer read")
            return super().open_input_file(path)

    restore_data_context.retried_io_errors.append("transient footer read")
    dataset = ray.data.read_parquet(
        planning_files,
        merge_schema=True,
        filesystem=pafs.PyFileSystem(FailFirstOpen()),
        num_cpus=num_cpus,
    )
    failures = (tmp_path / "retry.txt").read_text().splitlines()
    assert failures
    assert all((int(pid) != os.getpid()) == (num_cpus > 0) for pid in failures)
    assert dataset.count() == 6


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "transform,expected",
    [
        pytest.param(lambda ds: ds.filter(expr=col("id") == 0), 3, id="filter"),
        pytest.param(lambda ds: ds.limit(2), 2, id="limit"),
        pytest.param(lambda ds: ds.select_columns(["id"]), 6, id="projection"),
    ],
)
def test_planning_count_declines_non_plain_reads(
    tmp_path,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
    transform,
    expected,
):
    trace = tmp_path / "non-plain.jsonl"
    dataset = ray.data.read_parquet(
        planning_files,
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=1,
    )
    before = read_schema_records(trace)
    assert transform(dataset).count() == expected
    assert len(read_schema_records(trace)) > len(before)


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_planning_count_declines_read_projection(
    tmp_path, planning_files, ray_start_regular_shared_2_cpus, datasource_v2
):
    trace = tmp_path / "projected.jsonl"
    dataset = ray.data.read_parquet(
        planning_files,
        columns=["id"],
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=1,
    )
    before = read_schema_records(trace)
    assert dataset.count() == 6
    assert len(read_schema_records(trace)) > len(before)


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_planning_count_declines_checkpoint(
    tmp_path,
    planning_files,
    ray_start_regular_shared_2_cpus,
    datasource_v2,
    restore_data_context,
):
    from ray.data.checkpoint import CheckpointConfig

    restore_data_context.checkpoint_config = CheckpointConfig(
        id_column="id", checkpoint_path=str(tmp_path / "checkpoint")
    )
    trace = tmp_path / "checkpoint.jsonl"
    dataset = ray.data.read_parquet(
        [planning_files[1]],
        merge_schema=True,
        filesystem=recording_filesystem(trace),
        num_cpus=1,
    )
    before = read_schema_records(trace)
    assert dataset.count() == 1
    assert len(read_schema_records(trace)) > len(before)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
