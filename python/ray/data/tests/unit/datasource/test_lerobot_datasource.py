import os
import sys

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource.lerobot_datasource import (
    _LeRobotRoot,
    _read_lerobot_segment,
)
from ray.data._internal.object_extensions.arrow import (
    AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR,
    ArrowPythonObjectArray,
)
from ray.data._internal.untrusted_unpickling import guard_iterator

N_ROWS = 3


def _exploit_column(marker, n):
    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    return ArrowPythonObjectArray.from_objects([Exploit()] * n)


def _shard_columns(n=N_ROWS):
    return {
        "index": pa.array(range(n), pa.int64()),
        "episode_index": pa.array([0] * n, pa.int64()),
        "frame_index": pa.array(range(n), pa.int64()),
        "timestamp": pa.array([i / 10 for i in range(n)], pa.float64()),
        "task_index": pa.array([0] * n, pa.int64()),
    }


def _make_root(tmp_path) -> _LeRobotRoot:
    """A no-camera, no-delta root; the segment reader only touches a few fields."""
    return _LeRobotRoot(
        root=str(tmp_path),
        fs=fsspec.filesystem("file"),
        fs_root=str(tmp_path),
        data_path="data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        video_path=None,
        video_keys=[],
        image_keys=[],
        tasks_dict={0: "test_task"},
        schema=pa.schema([]),
        row_size_bytes=64,
        total_frames=N_ROWS,
        fps=10,
        storage_options={},
        stats_json="{}",
        frame_tolerance_s=0.05,
        delta_steps={},
    )


def _read_segment(root, shard):
    # ``ep_slice`` is only consulted for video or delta reads. Run the read the
    # way the read operator does: under the untrusted-unpickling guard.
    return list(
        guard_iterator(
            lambda: _read_lerobot_segment(
                root, 0, N_ROWS, 0, [str(shard)], pa.table({}), 1 << 20
            )
        )
    )


def test_read_lerobot_segment_allows_plain_columns(tmp_path):
    shard = tmp_path / "shard.parquet"
    pq.write_table(pa.table(_shard_columns()), shard)

    tables = _read_segment(_make_root(tmp_path), shard)

    assert len(tables) == 1
    assert tables[0].num_rows == N_ROWS
    assert tables[0].column("task").to_pylist() == ["test_task"] * N_ROWS


@pytest.mark.parametrize("evil_col", ["evil", "task_index"])
def test_read_lerobot_segment_rejects_pickle_object_columns(tmp_path, evil_col):
    """A tampered data shard must be rejected before anything is unpickled, even
    when the evil column shadows ``task_index``, which the reader materializes."""
    marker = tmp_path / "exploit_marker"
    shard = tmp_path / "shard.parquet"
    columns = {**_shard_columns(), evil_col: _exploit_column(marker, N_ROWS)}
    pq.write_table(pa.table(columns), shard)

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        _read_segment(_make_root(tmp_path), shard)

    assert not marker.exists(), "pickle.load executed attacker code"


def test_read_lerobot_segment_allows_pickle_object_columns_with_env_var(
    tmp_path, monkeypatch
):
    monkeypatch.setenv(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, "1")
    shard = tmp_path / "shard.parquet"
    columns = {
        **_shard_columns(),
        "obj": ArrowPythonObjectArray.from_objects([{"key": "value"}] * N_ROWS),
    }
    pq.write_table(pa.table(columns), shard)

    tables = _read_segment(_make_root(tmp_path), shard)

    assert tables[0].column("obj").to_pylist() == [{"key": "value"}] * N_ROWS


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
