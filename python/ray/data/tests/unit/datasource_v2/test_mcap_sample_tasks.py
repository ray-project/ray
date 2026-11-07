"""Unit tests for sizing sample read tasks by bytes (``mcap_sample_tasks``)."""

import copy
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


def probe_of(*paths):
    """A probe with one ``SAMPLE`` per path."""
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


@pytest.mark.parametrize(
    "counted, timed",
    [
        # /cam logs 10 frames a second, so its last 20 frames span 2 s.
        (steps.last(20), steps.before(frames=20, fps=10)),
        (steps.next(15), steps.after(frames=15, fps=10)),
    ],
)
def test_last_and_next_weigh_the_chunks_their_messages_reach(recording, counted, timed):
    """``steps.last`` and ``steps.next`` weigh about the chunks of the time
    their messages take to log, as time steps over that time do, not only the
    chunk at the anchor."""
    summary = summary_of(recording)

    weight = sample_bytes(summary, SAMPLE, [plan("/cam", TopicSpec(counted))])

    assert weight == sample_bytes(summary, SAMPLE, [plan("/cam", TopicSpec(timed))])


def test_a_summary_without_channels_counts_every_chunk_in_reach(recording):
    """A summary that lists no channel cannot tell which chunks hold a topic, so
    a sample weighs every chunk its steps' span overlaps, as the read reads."""
    summary = copy.copy(summary_of(recording))
    summary.channels = {}
    cam = plan("/cam", TopicSpec(steps.before(0.5, fps=10)))
    low, high = cam.reach(SAMPLE)
    in_reach = [
        c.uncompressed_size
        for c in summary.chunk_indexes
        if c.message_end_time >= low and c.message_start_time <= high
    ]

    weight = sample_bytes(summary, SAMPLE, [cam])

    assert in_reach and weight == sum(in_reach)


def test_a_file_without_a_chunk_index_weighs_its_whole_size(tmp_path):
    """Without a chunk index nothing can be told apart, so a sample of an
    unindexed file weighs the whole file."""
    path = os.path.join(tmp_path, "unindexed.mcap")
    write_recording(path, index=False)
    size = os.path.getsize(path)
    probe = pa.Table.from_pylist(
        [
            {
                "files": [path],
                "clip_start": SAMPLE.clip_start,
                "clip_end": SAMPLE.clip_end,
                "anchor": SAMPLE.anchor,
            }
        ]
    )
    plans = [plan("/cam", TopicSpec(steps.before(0.5, fps=10)))]

    count = samples_per_task(probe, plans, SamplePaths(None), 3 * size)

    # Three times the file's size fits three samples.
    assert count == 3


@pytest.mark.parametrize(
    "budget_factor, expected",
    [
        # Four times the median weight fits four samples. Over the mean it
        # would fit two, and over the max one.
        (4, 4),
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
    # Steps across the clip reach more chunks in a longer clip, so clips of
    # 0.5 s, 1 s and 4 s weigh more and more.
    plans = [plan("/cam", TopicSpec(steps.across(4)))]
    ends = [SECOND // 2, SECOND, 4 * SECOND]
    median = sample_bytes(summary_of(recording), SampleTimes(0, ends[1], 0), plans)
    probe = pa.Table.from_pylist(
        [{"files": [recording], "clip_start": 0, "clip_end": end} for end in ends]
    )

    count = samples_per_task(
        probe, plans, SamplePaths(None), int(median * budget_factor)
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

    mixed = samples_per_task(
        probe_of(missing, recording), plans, SamplePaths(None), weight * 2
    )
    gone = samples_per_task(probe_of(missing), plans, SamplePaths(None), weight * 2)

    assert (mixed, gone) == (2, DEFAULT_SAMPLES_PER_TASK)


@pytest.mark.parametrize("corruption", ["cut_short", "not_mcap"])
def test_files_that_do_not_parse_are_left_out(recording, tmp_path, corruption):
    """A truncated recording, or a file that is not MCAP, is left out of the
    estimate like a missing file, so the read reports it rather than planning."""
    plans = [plan("/cam", TopicSpec(steps.before(0.5, fps=10)))]
    weight = sample_bytes(summary_of(recording), SAMPLE, plans)
    corrupt = str(tmp_path / "corrupt.mcap")
    with open(recording, "rb") as f:
        data = f.read()
    with open(corrupt, "wb") as f:
        # A run that crashed mid-recording, or a file that is not MCAP at all.
        f.write(data[: len(data) // 2] if corruption == "cut_short" else b"x" * 100)

    mixed = samples_per_task(
        probe_of(corrupt, recording), plans, SamplePaths(None), weight * 2
    )
    alone = samples_per_task(probe_of(corrupt), plans, SamplePaths(None), weight * 2)

    assert (mixed, alone) == (2, DEFAULT_SAMPLES_PER_TASK)


def test_unexpected_errors_reading_a_summary_raise(recording, monkeypatch):
    """An error that is neither I/O nor corrupt data is a bug, so the estimate
    raises it rather than leaving the file out."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_sample_tasks

    def broken(filesystem, path):
        raise TypeError("a bug")

    monkeypatch.setattr(mcap_sample_tasks, "read_summary", broken)
    plans = [plan("/cam", TopicSpec(steps.before(0.5, fps=10)))]

    with pytest.raises(TypeError, match="a bug"):
        samples_per_task(probe_of(recording), plans, SamplePaths(None), 1 << 20)


@pytest.mark.parametrize(
    "bad",
    [{"clip_start": None}, {"clip_end": None}, {"files": [None]}, {"files": []}],
    ids=["no_clip_start", "no_clip_end", "null_file", "no_file"],
)
def test_samples_without_a_file_or_clip_are_left_out(recording, bad):
    """A sample with a null file or clip bound is left out of the estimate, so
    the read, not planning, reports it with the sample validation error."""
    plans = [plan("/cam", TopicSpec(steps.before(0.5, fps=10)))]
    weight = sample_bytes(summary_of(recording), SAMPLE, plans)
    good = {
        "files": [recording],
        "clip_start": SAMPLE.clip_start,
        "clip_end": SAMPLE.clip_end,
        "anchor": SAMPLE.anchor,
    }
    probe = pa.Table.from_pylist([good, {**good, "anchor": None, **bad}])

    count = samples_per_task(probe, plans, SamplePaths(None), weight * 2)

    assert count == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
