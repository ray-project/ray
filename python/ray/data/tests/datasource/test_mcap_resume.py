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
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.operators import Write
from ray.data._internal.logical.optimizers import get_execution_plan
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


def sample_read(tmp_path, rows, source, run=0):
    """A sample read of ``rows`` under a checkpoint on ``sample_id`` that is
    kept after each run, as a crashed run keeps it.

    ``source`` is where the samples come from: ``"items"``, or
    ``"episodes"``, a Parquet listing fanned out into samples by a
    ``flat_map`` as a labeling job builds them from a list of episodes. The
    listing's rows have no ``sample_id``. ``run`` names the run's listing.
    """
    from ray.data.checkpoint import CheckpointConfig
    from ray.data.datasource.mcap import TopicSpec, steps

    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="sample_id",
        checkpoint_path=str(tmp_path / "ckpt"),
        delete_checkpoint_on_success=False,
    )
    listing = str(tmp_path / f"samples{run}.parquet")
    if source == "episodes":
        pq.write_table(pa.Table.from_pylist([{"samples": rows}]), listing)
        samples = ray.data.read_parquet(listing).flat_map(lambda r: r["samples"])
    else:
        samples = ray.data.from_items(rows)
    ds = ray.data.read_mcap(samples=samples, topics={"/imu": TopicSpec(steps.last(2))})
    # A map keeps the read's topic columns, so every sample's file is read.
    return ds.map_batches(lambda b: {"sample_id": b["sample_id"]})


def write_twice(tmp_path, first, second, source):
    """Write a sample read of ``first``, then of ``second``, to one sink with a
    checkpoint on ``sample_id`` (``sample_read``). Returns the ``sample_id`` of
    every row in the sink.
    """
    out = str(tmp_path / "out")
    for run, rows in enumerate((first, second)):
        sample_read(tmp_path, rows, source, run).write_parquet(out)
    return sorted(pq.read_table(out)["sample_id"].to_pylist())


def op_names_from_sink(physical_plan):
    """The names of a linear physical plan's operators, from the sink up."""
    names = []
    op = physical_plan.dag
    while op is not None:
        names.append(op.name)
        op = op.input_dependencies[0] if op.input_dependencies else None
    return names


@pytest.fixture
def recording(tmp_path, restore_data_context):
    """A five-second camera and IMU recording, read through the V2 datasource."""
    ray.data.DataContext.get_current().use_datasource_v2 = True
    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path)
    return path


def test_resume_skips_finished_samples_before_reading(
    ray_start_10_cpus_shared, recording, tmp_path
):
    """A rerun drops finished samples before their files are opened: in the
    second run they name a file that does not exist, and the read still
    succeeds. Each sample lands in the sink once.

    The samples come from episodes, whose listing has no ``sample_id``, so the
    checkpoint must filter the sample read's input, not the listing.
    """
    rows = samples_of(recording, ANCHORS)
    # The first run finishes half the samples. The second asks for all of
    # them, with the finished half pointing at a missing file.
    missing = [dict(r, files=[str(tmp_path / "missing.mcap")]) for r in rows[:2]]

    written = write_twice(tmp_path, rows[:2], missing + rows[2:], "episodes")

    assert written == ["s0", "s1", "s2", "s3"]


def test_resume_filters_samples_before_cutting_tasks(
    ray_start_10_cpus_shared, recording, tmp_path
):
    """A rerun's checkpoint filter drops finished samples before they are cut
    into blocks of N samples, so each read task holds N unfinished samples."""
    rows = samples_of(recording, ANCHORS)
    out = str(tmp_path / "out")
    sample_read(tmp_path, rows[:2], "items").write_parquet(out)
    rerun = sample_read(tmp_path, rows, "items", run=1)
    write_op = Write(ParquetDatasink(out), input_dependencies=[rerun._logical_plan.dag])

    physical_plan, _ = get_execution_plan(
        LogicalPlan(write_op, ray.data.DataContext.get_current())
    )

    names = op_names_from_sink(physical_plan)
    position = {
        step: next(i for i, name in enumerate(names) if step in name)
        for step in ("ReadFilesMCAP", "StreamingRepartition", "CheckpointFilter")
    }
    assert (
        position["ReadFilesMCAP"]
        < position["StreamingRepartition"]
        < position["CheckpointFilter"]
    ), names


def test_resume_with_every_sample_finished(
    ray_start_10_cpus_shared, recording, tmp_path
):
    """A rerun whose samples are all finished writes nothing, rather than
    failing on the empty manifest blocks the checkpoint leaves."""
    rows = samples_of(recording, ANCHORS[:2])

    written = write_twice(tmp_path, rows, rows, "episodes")

    assert written == ["s0", "s1"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
