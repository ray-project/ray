"""Unit tests for sample rows: reading each topic's steps around a sample's anchor.

The tests drive the scanner and reader as a read task of
``read_mcap(samples=...)`` does, with no Ray cluster. Most read the
``recording`` fixture: a 10 fps camera whose payloads end with their frame
number, and a 50 Hz IMU whose payloads are their sample number.
"""

import importlib.util
import json
import os

import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data.datasource.mcap import steps
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    SECOND,
    frame_time,
    pack,
    read_all,
    read_samples,
    record_parsed_chunks,
    summary_of,
    write_mcap,
    write_recording,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

TENTH = SECOND // 10
CLIP_START, CLIP_END = 1 * SECOND, 4 * SECOND
IMU_NS = SECOND // 50  # the IMU's sample period


def sample(path, anchor, sample_id="s", **columns):
    """A sample of ``path`` with the default clip, anchored at ``anchor``."""
    return {
        "sample_id": sample_id,
        "files": [path],
        "clip_start": CLIP_START,
        "clip_end": CLIP_END,
        "anchor": anchor,
        **columns,
    }


def frames(row, topic="/cam"):
    """The camera frame each step holds, ``None`` for an empty step."""
    return [int.from_bytes(d[-4:], "big") if d else None for d in row[f"data:{topic}"]]


def imu_samples(row):
    """The IMU sample each step holds, ``None`` for an empty step."""
    return [int.from_bytes(d, "big") if d else None for d in row["data:/imu"]]


def json_values(row, topic):
    """The ``t`` value of each step's JSON payload, ``None`` for an empty step."""
    return [json.loads(d)["t"] if d else None for d in row[f"data:{topic}"]]


@pytest.fixture
def gap_file(tmp_path):
    """Two JSON topics, one message per chunk, ``/a`` with a gap::

        seconds  1.0  1.1  1.2  1.3 ... 2.9  3.0
        /a       a    a    a                 a
        /b       b    b    b    b   ... b    b

    Each message's ``t`` is its log time in tenths of a second.
    """
    path = os.path.join(tmp_path, "gap.mcap")
    messages = []
    for k in range(10, 31):
        if k <= 12 or k == 30:
            messages.append({"topic": "/a", "log_time": k * TENTH, "data": {"t": k}})
        messages.append({"topic": "/b", "log_time": k * TENTH, "data": {"t": k}})
    write_mcap(path, messages)
    return path


def chunks_of(path, topic):
    """The offsets of the chunks that hold ``topic``, one message per chunk."""
    summary = summary_of(path)
    ids = {cid for cid, c in summary.channels.items() if c.topic == topic}
    return {
        c.chunk_start_offset
        for c in summary.chunk_indexes
        if not ids.isdisjoint(c.message_index_offsets)
    }


def test_rows_hold_each_topics_steps(recording):
    """Each row keeps its sample's columns, and per topic the payload, log time and
    validity of every step: the camera's frames at 10 fps up to the anchor, and
    the IMU's next two samples after it."""
    topics = {
        "/cam": TopicSpec(steps.before(0.3, fps=10)),
        "/imu": TopicSpec(steps.next(2)),
    }
    anchors = [frame_time(15), frame_time(25) + 5_000_000]

    rows = read_samples(
        [sample(recording, a, f"s{i}", label=i) for i, a in enumerate(anchors)], topics
    )

    assert [(row["sample_id"], row["label"]) for row in rows] == [("s0", 0), ("s1", 1)]
    assert [frames(row) for row in rows] == [[13, 14, 15], [23, 24, 25]]
    assert rows[0]["times:/cam"] == [frame_time(k) for k in (13, 14, 15)]
    assert rows[0]["valid:/cam"] == [True, True, True]
    # The IMU logs every 20 ms, so the samples after 1.5 s are 76 and 77.
    assert [imu_samples(row) for row in rows] == [[76, 77], [126, 127]]
    assert rows[1]["times:/imu"] == [126 * IMU_NS, 127 * IMU_NS]


def test_files_in_manifest_order_samples_in_anchor_order(tmp_path):
    """A task reads its files in the order the manifest first names them, and
    each file's samples in anchor order."""
    a, b = (os.path.join(tmp_path, f"{name}.mcap") for name in "ab")
    write_recording(a)
    write_recording(b)
    samples = [
        sample(b, 3 * SECOND, "b3"),
        sample(a, 2 * SECOND, "a2"),
        sample(b, frame_time(15), "b1.5"),
    ]

    rows = read_samples(samples, {"/cam": TopicSpec(steps.before(frames=1, fps=10))})

    assert [row["sample_id"] for row in rows] == ["b1.5", "b3", "a2"]


@pytest.mark.parametrize(
    "pad, expected, valid",
    [
        # Steps at 0.7, 0.8 and 0.9 s lie before the clip, which starts at 1.0 s.
        ("edge", [10, 10, 10, 10, 11], [False, False, False, True, True]),
        ("zeros", [None, None, None, 10, 11], [False, False, False, True, True]),
    ],
)
def test_steps_before_the_clip_are_padded(recording, pad, expected, valid):
    """Steps past the clip's edge are padded: ``edge`` repeats the first real
    frame, and ``zeros`` leaves empty payloads at the steps' own times."""
    topics = {"/cam": TopicSpec(steps.before(0.5, fps=10), pad=pad)}

    (row,) = read_samples([sample(recording, frame_time(11))], topics)

    assert frames(row) == expected
    assert row["valid:/cam"] == valid
    if pad == "zeros":
        assert row["times:/cam"][:3] == [frame_time(k) for k in (7, 8, 9)]


def test_raise_names_the_sample_and_its_file(recording):
    """``pad="raise"`` fails the read on a sample whose window reaches past its
    clip, naming the sample and its file."""
    topics = {"/cam": TopicSpec(steps.before(0.5, fps=10), pad="raise")}

    with pytest.raises(ValueError, match=r"Sample 'early' of .*run\.mcap.*3 of 5"):
        read_samples([sample(recording, frame_time(11), "early")], topics)


def test_nearby_samples_share_their_chunks(recording, monkeypatch):
    """Samples whose windows meet are read in one pass: no chunk is parsed
    twice, and none outside their windows is parsed."""
    parsed = record_parsed_chunks(monkeypatch)
    anchors = [frame_time(k) for k in range(15, 26)]
    topics = {
        "/cam": TopicSpec(steps.before(0.5, fps=10)),
        "/imu": TopicSpec(steps.every(before_s=0.2)),
    }

    rows = read_samples([sample(recording, a) for a in anchors], topics)

    assert [frames(row)[-1] for row in rows] == list(range(15, 26))
    assert len(parsed) == len(set(parsed))
    # The first window starts 0.4 s before the first anchor, the last ends at
    # the last anchor.
    low, high = anchors[0] - 4 * TENTH, anchors[-1]
    near = {
        c.chunk_start_offset
        for c in summary_of(recording).chunk_indexes
        if c.message_end_time >= low and c.message_start_time <= high
    }
    assert set(parsed) <= near


def test_a_step_in_a_gap_reads_back_to_the_last_message(gap_file, monkeypatch):
    """A step in a gap takes the topic's last message before the gap, read
    back chunk by chunk through the chunks that hold the topic alone."""
    parsed = record_parsed_chunks(monkeypatch)

    (row,) = read_samples(
        [sample(gap_file, 25 * TENTH)],
        {"/a": TopicSpec(steps.before(frames=2, fps=10))},
    )

    assert json_values(row, "/a") == [12, 12]
    assert row["valid:/a"] == [True, True]
    # Only the chunks of /a before the gap are read, not those of /b.
    assert set(parsed) <= chunks_of(gap_file, "/a")


def test_frame_count_without_fps_reads_as_far_as_it_needs(recording):
    """``before_frames`` without ``fps`` takes that many recorded frames up to
    the anchor, however many chunks back they lie."""
    (row,) = read_samples(
        [sample(recording, frame_time(25))], {"/cam": TopicSpec(steps.last(12))}
    )

    assert frames(row) == list(range(14, 26))


def test_file_without_a_chunk_index(tmp_path):
    """A file without a chunk index is read whole and gives the same rows."""
    indexed, unindexed = (os.path.join(tmp_path, f"{n}.mcap") for n in ("i", "u"))
    write_recording(indexed)
    write_recording(unindexed, index=False)
    topics = {
        "/cam": TopicSpec(steps.before(0.3, fps=10)),
        "/imu": TopicSpec(steps.every(after_s=0.1)),
    }

    rows = {
        path: read_samples([sample(path, frame_time(20) + 1)], topics)[0]
        for path in (indexed, unindexed)
    }

    for column in ("data:/cam", "times:/cam", "data:/imu", "valid:/imu"):
        assert rows[indexed][column] == rows[unindexed][column]


def test_topic_missing_from_the_file(recording):
    """A topic the file lacks leaves every step empty and invalid, at the clip's
    edge for a frame count without ``fps``."""
    (row,) = read_samples(
        [sample(recording, 2 * SECOND)], {"/nope": TopicSpec(steps.next(2))}
    )

    assert row["data:/nope"] == [b"", b""]
    assert row["valid:/nope"] == [False, False]
    assert row["times:/nope"] == [CLIP_END - 1, CLIP_END - 1]


def test_projection_reads_only_the_wanted_topics(gap_file, monkeypatch):
    """A projection that keeps one topic's columns reads only that topic's
    chunks and builds only its kept columns."""
    parsed = record_parsed_chunks(monkeypatch)
    topics = {
        "/a": TopicSpec(steps.before(frames=1, fps=10)),
        "/b": TopicSpec(steps.before(frames=1, fps=10)),
    }

    rows = read_samples(
        [sample(gap_file, 11 * TENTH)], topics, columns=["valid:/b", "sample_id"]
    )

    assert rows == [{"valid:/b": [True], "sample_id": "s"}]
    assert set(parsed) <= chunks_of(gap_file, "/b")


def test_projection_to_the_samples_columns_reads_no_file(tmp_path):
    """A projection that keeps only the samples' own columns, as ``count()``
    does, needs no message: the files are never opened."""
    missing = os.path.join(tmp_path, "never-written.mcap")
    samples = [sample(missing, 2 * SECOND, f"s{i}") for i in range(3)]
    topics = {"/cam": TopicSpec(steps.last(1))}

    rows = read_samples(samples, topics, columns=["sample_id"])
    counted = read_samples(samples, topics, columns=[])

    assert [row["sample_id"] for row in rows] == ["s0", "s1", "s2"]
    assert len(counted) == 3


def test_limit_stops_after_its_rows(recording):
    """A pushed-down limit stops the task after that many rows."""
    samples = [sample(recording, frame_time(k)) for k in range(12, 20)]

    rows = read_samples(samples, {"/cam": TopicSpec(steps.last(1))}, limit=3)

    assert [frames(row) for row in rows] == [[12], [13], [14]]


def test_row_over_the_row_limit_fails(recording, monkeypatch):
    """A row whose payloads pass ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails the read."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", "100")

    with pytest.raises(ValueError, match="RAY_DATA_MCAP_MAX_ROW_BYTES"):
        read_samples(
            [sample(recording, 3 * SECOND)],
            {"/imu": TopicSpec(steps.every(before_s=1.0))},
        )


def test_file_uris_resolve_in_the_task(recording):
    """A sample may name its file by URI: the task resolves its filesystem."""
    (row,) = read_samples(
        [sample(f"file://{recording}", frame_time(20))],
        {"/cam": TopicSpec(steps.before(frames=2, fps=10))},
    )

    assert frames(row) == [19, 20]


@pytest.mark.parametrize("window_ns", [SECOND // 2, 3 * SECOND // 10])
def test_tiled_windows_hold_the_message_rows(recording, window_ns):
    """Samples that tile the recording into back-to-back windows, each taking
    every message of its window, hold exactly the message rows of the same file
    grouped by window, in log-time order.

    This is the samples table a fixed-window read amounts to.
    """
    topics = ["/cam", "/imu"]
    statistics = summary_of(recording).statistics
    assert statistics is not None
    start, end = statistics.message_start_time, statistics.message_end_time + 1
    # The anchor sits on a window's last nanosecond, so ``steps.every`` over the
    # window's length covers it exactly.
    windows = [(t, min(t + window_ns, end)) for t in range(start, end, window_ns)]
    samples = [
        {"files": [recording], "clip_start": a, "clip_end": b, "anchor": b - 1}
        for a, b in windows
    ]
    datasource = MCAPDatasourceV2([recording], topics=topics, include_metadata=False)

    rows = read_samples(
        samples,
        {t: TopicSpec(steps.every(before_s=window_ns / SECOND)) for t in topics},
    )
    messages = read_all(datasource, *pack(datasource)).to_pylist()

    assert len(rows) == len(windows)
    for (a, b), row in zip(windows, sorted(rows, key=lambda r: r["clip_start"])):
        for t in topics:
            expected = [
                m for m in messages if m["topic"] == t and a <= m["log_time"] < b
            ]
            assert row[f"times:{t}"] == [m["log_time"] for m in expected]
            assert row[f"data:{t}"] == [m["data"] for m in expected]
            assert all(row[f"valid:{t}"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
