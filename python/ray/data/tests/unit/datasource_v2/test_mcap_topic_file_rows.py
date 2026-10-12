"""Unit tests for MCAP topic and file rows: a whole topic or file in one row.

Most tests read the ``recording`` fixture, whose docstring draws its timeline.
No Ray cluster: the indexer, scanner and reader are driven directly.
"""

import dataclasses
import importlib.util
import os

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_layout import (
    coarse_row_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    TimeRange,
    WindowSpec,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    CAM_FPS,
    CAM_FRAMES,
    CAM_SCHEMA,
    DURATION_S,
    H264_KEYFRAME,
    H264_PFRAME,
    IMU_HZ,
    IMU_SAMPLES,
    LEAD_IN_FRAMES,
    MID_GOP_NS,
    SECOND,
    frame_time,
    list_manifests,
    read_rows,
    scanner_for,
    write_recording,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)


def test_topic_rows_one_task_per_topic_starting_at_a_keyframe(tmp_path):
    """Topic granularity lists one task per topic, and the camera's row starts at
    its first keyframe."""
    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path, first_keyframe=2)  # frames 0 and 1 are not decodable
    datasource = MCAPDatasourceV2([path], read_granularity="topic", include_row_id=True)

    manifests = list_manifests(datasource)
    assert [m.file_chunk_metadatas[0]["topic"] for m in manifests] == ["/cam", "/imu"]
    assert datasource.get_file_partitioner() is None
    rows = read_rows(datasource, manifests)

    assert [row["topic"] for row in rows] == ["/cam", "/imu"]
    cam, imu = rows
    assert cam["num_messages"] == CAM_FRAMES - 2
    assert cam["start_time"] == frame_time(2)
    assert cam["num_lead_in"] == 0
    assert cam["data"][0].startswith(H264_KEYFRAME)
    assert cam["row_id"] == f"{path}#/cam@{datasource.selection.digest()}"
    assert imu["num_messages"] == IMU_SAMPLES
    assert imu["channel_id"] == [imu["channels"][0]["channel_id"]] * imu["num_messages"]
    assert imu["log_time"] == sorted(imu["log_time"])


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_topic_rows_look_back_for_a_keyframe_before_the_range(
    recording, tmp_path, index
):
    """When a time range starts mid-GOP, the topic row prepends the frames from the
    keyframe before the range as lead-in. With no keyframe in the range or the
    look-back, the row is dropped."""
    if not index:
        recording = os.path.join(tmp_path, "noindex.mcap")
        write_recording(recording, index=False)
    datasource = MCAPDatasourceV2(
        [recording],
        topics=["/cam"],
        time_range=TimeRange(start_time=SECOND + MID_GOP_NS, end_time=3 * SECOND),
        read_granularity="topic",
    )

    (row,) = read_rows(datasource, list_manifests(datasource))

    assert row["num_lead_in"] == LEAD_IN_FRAMES
    assert row["log_time"][:LEAD_IN_FRAMES] == [
        SECOND + frame_time(k) for k in range(LEAD_IN_FRAMES)
    ]
    assert row["data"][0].startswith(H264_KEYFRAME)
    assert row["start_time"] == SECOND + frame_time(6)  # 1.6 s, first frame in range
    assert row["end_time"] == 2 * SECOND + frame_time(9)
    assert row["num_messages"] == LEAD_IN_FRAMES + 14

    # A range so short that no keyframe lies in it or in the look-back: the row
    # is dropped rather than emitted undecodable.
    monkey = MCAPDatasourceV2(
        [recording],
        topics=["/cam"],
        time_range=TimeRange(
            start_time=SECOND + 150_000_000, end_time=SECOND + 450_000_000
        ),
        read_granularity="topic",
    )
    scanner = scanner_for(monkey)
    scanner = dataclasses.replace(scanner, max_lead_in_ns=50_000_000)
    rows = [
        r
        for m in list_manifests(monkey)
        for t in scanner.create_reader().read(m)
        for r in t.to_pylist()
    ]
    assert rows == []


@pytest.mark.parametrize("first_channel", ["video", "not video"])
def test_topic_row_starts_each_video_channel_on_a_keyframe(tmp_path, first_channel):
    """Two channels share ``/cam`` and the range starts at frame 12. Each video
    channel leads in from its own last keyframe: the second's at frame 5, the
    first's, when it is video, at frame 10."""
    from mcap.writer import CompressionType, Writer

    step = SECOND // CAM_FPS
    path = os.path.join(tmp_path, "shared_topic.mcap")
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        video = writer.register_schema(name=CAM_SCHEMA, encoding="ros2msg", data=b"")
        other = writer.register_schema(name="custom/Blob", encoding="ros2msg", data=b"")
        a = writer.register_channel(
            schema_id=video if first_channel == "video" else other,
            topic="/cam",
            message_encoding="cdr",
        )
        b = writer.register_channel(
            schema_id=video, topic="/cam", message_encoding="cdr"
        )
        for i in range(30):
            for channel_id, keyframe_at in ((a, 0), (b, 5)):
                log_time = i * step + (channel_id == b)
                payload = H264_KEYFRAME if (i - keyframe_at) % 10 == 0 else H264_PFRAME
                writer.add_message(
                    channel_id=channel_id,
                    log_time=log_time,
                    publish_time=log_time,
                    data=payload,
                    sequence=i,
                )
        writer.finish()
    datasource = MCAPDatasourceV2(
        [path], time_range=TimeRange(12 * step, 30 * step), read_granularity="topic"
    )

    (row,) = read_rows(datasource, list_manifests(datasource))

    firsts = {}
    for channel_id, log_time in zip(row["channel_id"], row["log_time"]):
        firsts.setdefault(channel_id, log_time // step)
    assert firsts == {a: 10 if first_channel == "video" else 12, b: 5}
    assert row["num_lead_in"] == (9 if first_channel == "video" else 7)


def test_topic_rows_honor_topics_and_excluded_units(recording):
    """Topic rows list only the selected topics, and an excluded topic unit is
    not listed."""
    datasource = MCAPDatasourceV2(
        [recording], topics=["/imu"], read_granularity="topic"
    )
    manifests = list_manifests(datasource)
    assert len(manifests) == 1

    from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
        topic_unit_id,
    )

    assert (
        list_manifests(
            datasource, excluded_read_unit_ids={topic_unit_id(recording, "/imu")}
        )
        == []
    )


@pytest.mark.parametrize(
    "time_range",
    [None, TimeRange(start_time=MID_GOP_NS, end_time=DURATION_S * SECOND)],
    ids=["whole", "mid_gop"],
)
def test_topic_rows_keep_out_a_channel_the_summary_omits(
    recording, monkeypatch, time_range
):
    """A channel that only the chunks declare stays out of another topic's row.

    The summary omits ``/imu``, whose messages share chunks with ``/cam``. With
    ``time_range`` the ``/cam`` row also reads back for its lead-in.
    """
    from mcap.reader import SeekingReader

    real_get_summary = SeekingReader.get_summary

    def without_imu(self):
        summary = real_get_summary(self)
        assert summary is not None
        summary.channels = {
            cid: c for cid, c in summary.channels.items() if c.topic != "/imu"
        }
        return summary

    monkeypatch.setattr(SeekingReader, "get_summary", without_imu)
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="topic", time_range=time_range
    )

    (row,) = read_rows(datasource, list_manifests(datasource))

    topics = {c["channel_id"]: c["topic"] for c in row["channels"]}
    assert row["topic"] == "/cam"
    assert {topics[cid] for cid in row["channel_id"]} == {"/cam"}
    assert row["num_messages"] == CAM_FRAMES


def test_file_rows(recording):
    """File granularity reads the whole recording into one row, in log-time
    order."""
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="file", include_row_id=True
    )

    manifests = list_manifests(datasource)
    assert len(manifests) == 1 and manifests[0].file_chunk_metadatas[0] is None
    assert datasource.get_file_partitioner() is None
    (row,) = read_rows(datasource, manifests)

    assert row["path"] == recording
    assert row["num_messages"] == CAM_FRAMES + IMU_SAMPLES
    assert row["start_time"] == 0
    assert row["end_time"] == (IMU_SAMPLES - 1) * SECOND // IMU_HZ
    assert set(row["topic"]) == {"/cam", "/imu"}
    assert len(row["channels"]) == 2
    assert row["row_id"] == f"{recording}@{datasource.selection.digest()}"
    assert row["log_time"] == sorted(row["log_time"])


def test_oversized_row_is_refused(recording, tmp_path, monkeypatch):
    """A file or topic row over ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails while its
    payloads are collected, before the whole row is held in memory."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", "100")
    datasource = MCAPDatasourceV2([recording], read_granularity="file")
    with pytest.raises(ValueError, match="at least [0-9]+ bytes.*over the 100-byte"):
        read_rows(datasource, list_manifests(datasource))
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="topic", topics=["/cam"]
    )
    with pytest.raises(ValueError, match="topic '/cam'.*at least [0-9]+ bytes"):
        read_rows(datasource, list_manifests(datasource))
    unindexed = os.path.join(tmp_path, "noindex.mcap")
    write_recording(unindexed, index=False)
    datasource = MCAPDatasourceV2([unindexed], read_granularity="topic")
    with pytest.raises(ValueError, match="topic '/(cam|imu)'.*at least [0-9]+ bytes"):
        read_rows(datasource, list_manifests(datasource))


@pytest.mark.parametrize("granularity", ["window", "topic", "file"])
def test_coarse_rows_without_metadata(recording, granularity):
    """Without metadata, coarse rows drop ``channels`` and keep ``path`` and
    ``data``."""
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity=granularity,
        window=WindowSpec(length_s=1.0) if granularity == "window" else None,
        include_metadata=False,
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert rows
    assert "channels" not in rows[0]
    assert "path" in rows[0] and "data" in rows[0]


@pytest.mark.parametrize("granularity", ["window", "topic", "file"])
def test_coarse_row_payloads_use_64_bit_offsets(recording, granularity):
    """Coarse row payloads use 64-bit offsets, so one row is not capped at 2 GiB."""
    large = pa.large_list(pa.large_binary())
    for include_metadata in (False, True):
        schema = coarse_row_schema(
            granularity, include_metadata=include_metadata, include_row_id=False
        )
        assert schema.field("data").type == large
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity=granularity,
        window=WindowSpec(length_s=1.0) if granularity == "window" else None,
    )
    scanner = scanner_for(datasource)
    assert scanner.read_schema().field("data").type == large

    tables = [
        table
        for manifest in list_manifests(datasource)
        for table in scanner.create_reader().read(manifest)
    ]

    assert tables
    assert all(table.schema.field("data").type == large for table in tables)


@pytest.mark.parametrize("granularity", ["window", "topic", "file"])
def test_pruned_coarse_reads_do_not_build_the_payloads(
    recording, granularity, monkeypatch
):
    """A coarse read builds only the projected columns, so a ``count()`` does not
    copy the payloads into ``data``."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_coarse_layout

    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity=granularity,
        window=WindowSpec(length_s=1.0) if granularity == "window" else None,
    )
    scanner = scanner_for(datasource)
    manifests = list_manifests(datasource)

    def num_rows(scanner):
        reader = scanner.create_reader()
        return sum(t.num_rows for m in manifests for t in reader.read(m))

    expected = num_rows(scanner)
    assert expected > 0

    def no_payloads(self):
        raise AssertionError("the data column was built")

    monkeypatch.setattr(mcap_coarse_layout.CoarseRowBatch, "_data_column", no_payloads)
    assert num_rows(scanner.prune_columns([])) == expected
    assert num_rows(scanner.prune_columns(["path", "num_messages"])) == expected
    with pytest.raises(AssertionError, match="data column was built"):
        num_rows(scanner)


def test_granularity_option_validation(recording):
    """``read_granularity`` must be known, ``window`` granularity needs a
    ``WindowSpec``, and a ``WindowSpec`` needs ``window`` granularity."""
    with pytest.raises(ValueError, match="read_granularity must be one of"):
        MCAPDatasourceV2([recording], read_granularity="clip")
    with pytest.raises(ValueError, match="needs a WindowSpec"):
        MCAPDatasourceV2([recording], read_granularity="window")
    with pytest.raises(ValueError, match="window applies to"):
        MCAPDatasourceV2([recording], window=WindowSpec(length_s=1))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
