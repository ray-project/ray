"""Unit tests for placing MCAP windows and assigning them to chunks."""

import importlib.util

import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import WindowSpec
from ray.data._internal.datasource_v2.formats.mcap.mcap_windows import (
    owner_offsets,
    place_windows,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import SECOND, summary_of

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)


def test_place_windows_from_file_start():
    """One-second windows tile the span from its start, a span shorter than one
    stride is one window, and an empty span has none."""
    spec = WindowSpec(length_s=1.0)

    assert place_windows(spec, 0, 5 * SECOND - 1) == [
        (k * SECOND, (k + 1) * SECOND) for k in range(5)
    ]
    assert place_windows(spec, 7 * SECOND, 7 * SECOND + 10) == [
        (7 * SECOND, 8 * SECOND)
    ]
    assert place_windows(spec, 10, 5) == []


def test_place_windows_drop_partial_and_stride():
    """``drop_partial`` drops a last window that runs past the span, and
    ``stride_s`` makes windows overlap."""
    spec = WindowSpec(length_s=1.0, drop_partial=True)
    # The last message is at 4.5 s, so the window [4 s, 5 s) runs past it.
    assert place_windows(spec, 0, 4 * SECOND + SECOND // 2) == [
        (k * SECOND, (k + 1) * SECOND) for k in range(4)
    ]

    overlapping = WindowSpec(length_s=1.0, stride_s=0.5)
    starts = [start for start, _ in place_windows(overlapping, 0, SECOND)]
    assert starts == [0, SECOND // 2, SECOND]


def test_place_windows_epoch_and_absolute_anchor():
    """An ``epoch`` anchor aligns windows to whole seconds, and an absolute anchor
    aligns them on both sides of it."""
    epoch = WindowSpec(length_s=1.0, anchor="epoch")
    # A file starting at 2.3 s gets windows aligned to whole seconds.
    assert place_windows(epoch, 2 * SECOND + 3 * SECOND // 10, 3 * SECOND) == [
        (2 * SECOND, 3 * SECOND),
        (3 * SECOND, 4 * SECOND),
    ]

    absolute = WindowSpec(length_s=1.0, anchor=10 * SECOND)
    assert place_windows(absolute, 8 * SECOND + SECOND // 2, 11 * SECOND) == [
        (8 * SECOND, 9 * SECOND),
        (9 * SECOND, 10 * SECOND),
        (10 * SECOND, 11 * SECOND),
        (11 * SECOND, 12 * SECOND),
    ]


def test_window_spec_validation():
    """``WindowSpec`` rejects a non-positive length or stride, an unknown anchor,
    and a length or stride under one nanosecond."""
    with pytest.raises(ValueError, match="length_s"):
        WindowSpec(length_s=0)
    with pytest.raises(ValueError, match="stride_s"):
        WindowSpec(length_s=1, stride_s=-1)
    with pytest.raises(ValueError, match="anchor"):
        WindowSpec(length_s=1, anchor="middle")  # pyrefly: ignore[bad-argument-type]
    with pytest.raises(ValueError, match="one nanosecond"):
        WindowSpec(length_s=1e-10)
    with pytest.raises(ValueError, match="one nanosecond"):
        WindowSpec(length_s=1, stride_s=1e-10)
    assert WindowSpec(length_s=0.5).stride_ns == SECOND // 2


def test_owner_offsets(recording):
    """A window belongs to the last chunk that starts at or before it, or to the
    first chunk if it starts before every chunk."""
    summary = summary_of(recording)
    chunks = sorted(summary.chunk_indexes, key=lambda c: c.message_start_time)
    starts = [c.message_start_time for c in chunks]

    assert owner_offsets(chunks, [starts[3] + 1]) == [chunks[3].chunk_start_offset]
    assert owner_offsets(chunks, [starts[3]]) == [chunks[3].chunk_start_offset]
    assert owner_offsets(chunks, [-5]) == [chunks[0].chunk_start_offset]
    assert owner_offsets(chunks, [starts[-1] + 10**12]) == [
        chunks[-1].chunk_start_offset
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
