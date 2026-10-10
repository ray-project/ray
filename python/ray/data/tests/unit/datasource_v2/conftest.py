"""Fixtures shared by the MCAP datasource unit tests."""

import os

import pytest

from ray.data.tests.unit.datasource_v2.mcap_testing import (
    CHUNKED_FILE_MESSAGES,
    H264_FILE_FRAMES,
    encode_h264,
    round_robin_messages,
    summary_of,
    write_mcap,
    write_payloads,
    write_recording,
)


@pytest.fixture
def chunked_file(tmp_path):
    """Nine JSON messages on ``/a``, ``/b`` and ``/c`` in turn, one per chunk.

        chunk / seq  0   1   2   3   4   5   6   7   8
        topic        /a  /b  /c  /a  /b  /c  /a  /b  /c

    Message ``seq`` is ``{"seq": seq}``, logged at ``BASE_TIME + seq * STEP``.
    """
    path = os.path.join(tmp_path, "chunked.mcap")
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    return path


@pytest.fixture
def recording(tmp_path):
    """Five seconds of a 10 fps camera and a 50 Hz IMU, in ~600-byte chunks.

        seconds  0.0  0.1  0.2  0.3  0.4  0.5  0.6  ...  1.0  1.1  ...  4.9
        /cam     K    P    P    P    P    P    P    ...  K    P    ...  P
        /imu     a sample every 20 ms, 0.00 to 4.98 s
                 |-------- lead-in ---------| ^ MID_GOP_NS (0.55 s)

    ``/cam`` frame ``k`` is logged at ``frame_time(k)``: a keyframe (K) every
    ``GOP`` frames, so at each whole second, and an inter frame (P) otherwise.
    A window opening at ``MID_GOP_NS`` leads in with the ``LEAD_IN_FRAMES``
    frames from 0.0 to 0.5 s. More than 8 chunks let a read split across tasks.
    """
    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path)
    summary = summary_of(path)
    assert len(summary.chunk_indexes) > 8, len(summary.chunk_indexes)
    return path


@pytest.fixture
def h264_file(tmp_path):
    """30 H.264 access units in ~400-byte chunks, so chunk boundaries fall mid-GOP.

        frame   0 | 1 .. 7 | 8 .. 14 | 15 .. 20 | 21 .. 27 | 28 29
        chunk   0 |   1    |    2    |    3     |    4     |   5

    Keyframes are frames 0, 10 and 20. Frame ``i`` is logged at
    ``i * FRAME_NS`` with sequence ``i``.
    """
    path = os.path.join(tmp_path, "h264.mcap")
    write_payloads(
        path,
        encode_h264(H264_FILE_FRAMES),
        schema_name="foxglove.CompressedVideo",
        chunk_size=400,
    )
    summary = summary_of(path)
    assert len(summary.chunk_indexes) >= 4
    return path
