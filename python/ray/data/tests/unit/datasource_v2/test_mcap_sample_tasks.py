"""Unit tests for sizing sample read tasks by bytes (``mcap_sample_tasks``)."""

import importlib.util
import os

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_sample_rows import SamplePaths
from ray.data._internal.datasource_v2.formats.mcap.mcap_sample_tasks import (
    DEFAULT_SAMPLES_PER_TASK,
    MAX_SAMPLES_PER_TASK,
    sample_bytes,
    samples_per_task,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    SampleTimes,
    TopicPlan,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data.datasource.mcap import steps
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    SECOND,
    summary_of,
    write_recording,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

# A sample of the ``recording`` fixture: clip [1 s, 4 s), anchor at 2 s.
SAMPLE = SampleTimes(1 * SECOND, 4 * SECOND, 2 * SECOND)


def plan(topic, spec):
    return TopicPlan.from_spec(topic, spec)


def chunk_bytes(summary, topic, low, high):
    """The decompressed bytes of the chunks holding ``topic`` in ``[low, high]``,
    by offset."""
    channels = {cid for cid, c in summary.channels.items() if c.topic == topic}
    return {
        c.chunk_start_offset: c.uncompressed_size
        for c in summary.chunk_indexes
        if c.message_end_time >= low
        and c.message_start_time <= high
        and not channels.isdisjoint(c.message_index_offsets)
    }


def test_sample_bytes_counts_the_chunks_its_steps_reach(recording):
    """A sample weighs the chunks each topic's steps reach; a chunk two topics
    share counts once."""
    summary = summary_of(recording)
    cam = plan("/cam", TopicSpec(steps.before(0.5, fps=10)))
    imu = plan("/imu", TopicSpec(steps.after(0.2, fps=50)))
    cam_chunks = chunk_bytes(summary, "/cam", *cam.reach(SAMPLE))
    imu_chunks = chunk_bytes(summary, "/imu", *imu.reach(SAMPLE))

    weight = sample_bytes(summary, SAMPLE, [cam, imu])

    assert weight == sum({**cam_chunks, **imu_chunks}.values())


def test_a_file_without_a_chunk_index_weighs_its_whole_size():
    """Without a chunk index nothing can be told apart, so the estimate is the
    file's size."""
    assert sample_bytes(12_345, SAMPLE, []) == 12_345


@pytest.mark.parametrize(
    "budget_factor, expected",
    [
        # Twice one sample's weight fits two samples.
        (2, 2),
        # Less than one sample's weight still takes one sample.
        (0.5, 1),
        # A huge budget stops at the cap.
        (10**9, MAX_SAMPLES_PER_TASK),
    ],
)
def test_samples_per_task_divides_the_budget_by_the_median(
    recording, budget_factor, expected
):
    """A task takes the budget over the median sample's weight, at least one
    sample and at most the cap."""
    plans = [plan("/cam", TopicSpec(steps.before(0.5, fps=10)))]
    weight = sample_bytes(summary_of(recording), SAMPLE, plans)
    probe = pa.Table.from_pylist(
        [
            {
                "files": [recording],
                "clip_start": SAMPLE.clip_start,
                "clip_end": SAMPLE.clip_end,
                "anchor": SAMPLE.anchor,
            }
        ]
        * 3
    )

    count = samples_per_task(
        probe, plans, SamplePaths(None), int(weight * budget_factor)
    )

    assert count == expected


def test_samples_per_task_reads_each_summary_once(tmp_path, monkeypatch):
    """The estimate reads the summary of each file the probe names once, however
    many samples name it."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_sample_tasks

    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path)
    reads = []
    real = mcap_sample_tasks.read_summary
    monkeypatch.setattr(
        mcap_sample_tasks,
        "read_summary",
        lambda fs, p: reads.append(p) or real(fs, p),
    )
    probe = pa.Table.from_pylist(
        [
            {
                "files": [path],
                "clip_start": 0,
                "clip_end": 4 * SECOND,
                "anchor": a * SECOND,
            }
            for a in (1, 2, 3)
        ]
    )

    samples_per_task(
        probe, [plan("/imu", TopicSpec(steps.last(2)))], SamplePaths(None), 1 << 20
    )

    assert len(reads) == 1


def test_files_that_cannot_be_opened_are_left_out(recording, tmp_path):
    """The probe may name files a checkpoint will skip, or that are gone: they
    do not count, and with nothing left the default count is used."""
    plans = [plan("/cam", TopicSpec(steps.before(0.5, fps=10)))]
    weight = sample_bytes(summary_of(recording), SAMPLE, plans)
    missing = str(tmp_path / "missing.mcap")

    def probe_of(*paths):
        return pa.Table.from_pylist(
            [
                {
                    "files": [p],
                    "clip_start": SAMPLE.clip_start,
                    "clip_end": SAMPLE.clip_end,
                    "anchor": SAMPLE.anchor,
                }
                for p in paths
            ]
        )

    mixed = samples_per_task(
        probe_of(missing, recording), plans, SamplePaths(None), weight * 2
    )
    gone = samples_per_task(probe_of(missing), plans, SamplePaths(None), weight * 2)

    assert (mixed, gone) == (2, DEFAULT_SAMPLES_PER_TASK)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
