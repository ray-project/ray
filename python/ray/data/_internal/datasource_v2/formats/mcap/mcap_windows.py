"""Window placement and window ownership.

Windows are ``length`` long and open one ``stride`` apart. With
``anchor="file_start"`` the first opens at the file's first message, or at the
time range's start if that is later. With ``"epoch"`` or a time, windows open
at whole strides from it, so they line up across files. A window belongs to
the last chunk that starts at or before the window's start. The task that owns
that chunk emits the window and reads on into later chunks to fill it. Every
task derives this from the same summary, so each window is emitted once::

    length 4 s, stride 2 s, anchor "file_start", last message at 7 s

    t (s)       0 1 2 3 4 5 6 7 8 9 10
    chunks      |c0 . . . |c1 |
    w0 [0, 4)   [-------)              c0
    w1 [2, 6)       [-------)          c0, reads into c1
    w2 [4, 8)           [-------)      c0, reads into c1
    w3 [6, 10)              [-------)  c1
"""

import bisect
from typing import TYPE_CHECKING, Iterable, List, Sequence, Tuple

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import WindowSpec

if TYPE_CHECKING:
    from mcap.records import ChunkIndex

# A window as ``[start, end)`` in nanoseconds.
Window = Tuple[int, int]


def place_windows(spec: WindowSpec, span_start: int, span_end: int) -> List[Window]:
    """The windows of ``spec`` that meet the closed span ``[span_start, span_end]``.

    ``span_start`` and ``span_end`` are a file's first and last log times,
    clipped to the time range. With ``anchor="file_start"``, the first window
    starts at ``span_start``. With ``"epoch"`` or an absolute anchor, windows
    start at the anchor plus a whole number of strides, so they line up across
    files. ``drop_partial`` drops the windows that end after the last message.
    """
    if span_end < span_start:
        return []
    length, stride = spec.length_ns, spec.stride_ns
    if spec.anchor == "file_start":
        base, first_k = span_start, 0
    else:
        base = 0 if spec.anchor == "epoch" else int(spec.anchor)
        # The first window that still reaches span_start:
        # base + k * stride > span_start - length.
        first_k = -((span_start - length + 1 - base) // -stride)
    windows: List[Window] = []
    k = first_k
    while True:
        start = base + k * stride
        if start > span_end:
            break
        end = start + length
        if spec.drop_partial and end > span_end + 1:
            break
        windows.append((start, end))
        k += 1
    return windows


def owner_offsets(
    chunk_indexes: Sequence["ChunkIndex"], window_starts: Iterable[int]
) -> List[int]:
    """The byte offset of the chunk that owns each window.

    A window belongs to the last chunk, by message start time, that starts at
    or before the window. A window before every chunk belongs to the first.
    """
    ordered = sorted(
        chunk_indexes, key=lambda c: (c.message_start_time, c.chunk_start_offset)
    )
    starts = [c.message_start_time for c in ordered]
    owners = []
    for window_start in window_starts:
        index = max(bisect.bisect_right(starts, window_start) - 1, 0)
        owners.append(ordered[index].chunk_start_offset)
    return owners
