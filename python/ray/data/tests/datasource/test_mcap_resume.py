"""Resuming ``read_mcap(samples=...)`` from a checkpoint keyed on a sample column.

A checkpoint filter needs actors of its own, so these tests run on a cluster
with more CPUs than ``test_mcap.py``'s.
"""

import importlib.util
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.unit.datasource_v2.mcap_testing import write_recording
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

SECOND = 1_000_000_000
ANCHORS = [1 * SECOND, 3 * SECOND // 2, 2 * SECOND, 5 * SECOND // 2]


def samples_of(path, anchors):
    """One sample of ``path`` per anchor, clipped to [0.5 s, 3 s)."""
    return [
        {
            "sample_id": f"s{i}",
            "files": [path],
            "clip_start": SECOND // 2,
            "clip_end": 3 * SECOND,
            "anchor": anchor,
        }
        for i, anchor in enumerate(anchors)
    ]


def write_twice(tmp_path, first, second, source):
    """Write a sample read of ``first``, then of ``second``, to one sink with a
    checkpoint on ``sample_id`` kept after each run, as a crashed run keeps it.
    Returns the ``sample_id`` of every row in the sink.

    ``source`` is where each run's samples come from: ``"items"``,
    ``"parquet"``, or ``"episodes"``, a Parquet listing fanned out into samples
    by a ``flat_map`` as a labeling job builds them from a list of episodes.
    The listing's rows have no ``sample_id``.
    """
    from ray.data.checkpoint import CheckpointConfig
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    out = str(tmp_path / "out")
    for run, rows in enumerate((first, second)):
        ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
            id_column="sample_id",
            checkpoint_path=str(tmp_path / "ckpt"),
            delete_checkpoint_on_success=False,
        )
        listing = str(tmp_path / f"samples{run}.parquet")
        if source == "episodes":
            pq.write_table(pa.Table.from_pylist([{"samples": rows}]), listing)
            samples = ray.data.read_parquet(listing).flat_map(lambda r: r["samples"])
        elif source == "parquet":
            pq.write_table(pa.Table.from_pylist(rows), listing)
            samples = ray.data.read_parquet(listing)
        else:
            samples = ray.data.from_items(rows)
        ds = ray.data.read_mcap(
            samples=samples, topics={"/imu": TopicSpec(steps.last(2))}
        )
        # A map keeps the read's topic columns, so every sample's file is read.
        ds = ds.map_batches(lambda b: {"sample_id": b["sample_id"]})
        ds.write_parquet(out)
    return sorted(pq.read_table(out)["sample_id"].to_pylist())


@pytest.fixture
def recording(tmp_path, restore_data_context):
    """A five-second camera and IMU recording, read through the V2 datasource."""
    ray.data.DataContext.get_current().use_datasource_v2 = True
    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path)
    return path


@pytest.mark.parametrize("source", ["items", "parquet", "episodes"])
def test_resume_skips_finished_samples_before_reading(
    ray_start_10_cpus_shared, recording, tmp_path, source
):
    """A rerun drops finished samples before their files are opened: in the
    second run they name a file that does not exist, and the read still
    succeeds. Each sample lands in the sink once."""
    rows = samples_of(recording, ANCHORS)
    # The first run finishes half the samples. The second asks for all of
    # them, with the finished half pointing at a missing file.
    missing = [dict(r, files=[str(tmp_path / "missing.mcap")]) for r in rows[:2]]

    written = write_twice(tmp_path, rows[:2], missing + rows[2:], source)

    assert written == ["s0", "s1", "s2", "s3"]


@pytest.mark.parametrize("source", ["items", "parquet", "episodes"])
def test_resume_with_every_sample_finished(
    ray_start_10_cpus_shared, recording, tmp_path, source
):
    """A rerun whose samples are all finished writes nothing, rather than
    failing on the empty manifest blocks the checkpoint leaves."""
    rows = samples_of(recording, ANCHORS[:2])

    written = write_twice(tmp_path, rows, rows, source)

    assert written == ["s0", "s1"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
