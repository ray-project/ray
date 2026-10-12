"""The lead-in: the frames before a row's own that a video decoder needs.

A video row decodes on its own only if it starts on a keyframe. So a window row
also carries, per video channel with a frame in the window, the frames from the
last keyframe before the window. The search starts at the window's start, or at
the time range's start if that is later. It goes back at most the look-back cap,
``RAY_DATA_MCAP_MAX_LEAD_IN_S`` (K is a keyframe, P another picture)::

    t                 0  1  2  3  4  5  6  7  8  9  10 11 12
    /cam (video)      K  P  P  K  P  P  P  P  P  K  P  P  P
    /imu              i  i  i  i  i  i  i  i  i  i  i  i  i      not video
    window                                 [-----------------)
    look-back cap           [--------------)
    lead-in of /cam            [-----------)

    first /cam frame in the window is a K  -> no lead-in
    no K in the look-back cap              -> no lead-in
    codec not recognised                   -> the whole look-back cap

A topic row starts each of its video channels on a keyframe too. If a channel's
first frame is not one, its frames from its last keyframe in the look-back
before the time range are prepended. Without such a keyframe the channel starts
at its first keyframe. A channel whose codec is not recognised keeps all its
messages, and one with no keyframe at all is dropped with a warning.
"""

import bisect
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Set, Tuple

from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_layout import _Entry
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    channel_codec,
    is_keyframe,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import Channel, Schema

logger = logging.getLogger(__name__)


@dataclass
class ChannelMessages:
    """One video channel's messages in a read range, for lead-in lookups."""

    channel: "Channel"
    schema: Optional["Schema"]
    times: List[int]
    entries: List[_Entry]
    codec: Optional[VideoCodec]
    # Cached ``is_keyframe`` per entry. Windows overlap, so each is asked often.
    keyframe: List[Optional[bool]]

    def is_keyframe_at(self, index: int) -> bool:
        cached = self.keyframe[index]
        if cached is None:
            assert self.codec is not None
            cached = is_keyframe(self.entries[index][2].data, self.codec)
            self.keyframe[index] = cached
        return cached


def video_channels(
    entries: List[_Entry],
    is_video: Callable[["Channel", Optional["Schema"]], bool],
    max_lead_in_ns: int,
) -> Dict[int, ChannelMessages]:
    """Index the entries of every video channel for lead-in lookups."""
    grouped: Dict[int, List[_Entry]] = {}
    for entry in entries:
        grouped.setdefault(entry[1].id, []).append(entry)
    by_channel: Dict[int, ChannelMessages] = {}
    for channel_id, channel_entries in grouped.items():
        schema, channel, _ = channel_entries[0]
        if not is_video(channel, schema):
            continue
        codec = _channel_codec(
            channel, schema, (e[2].data for e in channel_entries), max_lead_in_ns
        )
        by_channel[channel_id] = ChannelMessages(
            channel,
            schema,
            [e[2].log_time for e in channel_entries],
            channel_entries,
            codec,
            [None] * len(channel_entries),
        )
    return by_channel


def window_lead_in(
    channels: Dict[int, ChannelMessages],
    start: int,
    in_window: List[_Entry],
    *,
    range_start: Optional[int],
    max_lead_in_ns: int,
) -> List[_Entry]:
    """The lead-in of the video channels in ``in_window``, a window from ``start``.

    A video channel with no message in the window needs no lead-in. A
    window that opens before the time range holds frames from the range
    on, so its lead-in must reach the keyframe before those.
    """
    anchor = start if range_start is None else max(start, range_start)
    channels_in_window = {entry[1].id for entry in in_window}
    lead_in: List[_Entry] = []
    for channel_id, channel_messages in channels.items():
        if channel_id in channels_in_window:
            lead_in.extend(_lead_in(channel_messages, anchor, max_lead_in_ns))
    lead_in.sort(key=lambda e: e[2].log_time)
    return lead_in


def _lead_in(
    messages: ChannelMessages, anchor: int, max_lead_in_ns: int
) -> List[_Entry]:
    """The frames a decoder needs before ``anchor`` on one video channel.

    ``anchor`` is the first log time whose frames the row carries. The
    lead-in runs from the last keyframe before ``anchor``, searched back at
    most the look-back cap. It is empty if no keyframe is found there. A
    channel whose codec cannot be parsed gets the whole look-back span. A
    decoder can start on it if the stream's keyframe interval fits the cap.
    """
    end = bisect.bisect_left(messages.times, anchor)
    start = bisect.bisect_left(messages.times, anchor - max_lead_in_ns)
    if messages.codec is None:
        return messages.entries[start:end]
    if end < len(messages.entries) and messages.is_keyframe_at(end):
        # The window opens on a keyframe: nothing before it is needed.
        return []
    for index in range(end - 1, start - 1, -1):
        if messages.is_keyframe_at(index):
            return messages.entries[index:end]
    return []


@dataclass(frozen=True)
class _StreamStart:
    """Where one video channel's stream starts in a topic row.

    Each field counts the channel's messages the row skips: of the look-back,
    before its lead-in, and of its own, before the first one kept.
    """

    lead_skipped: int
    skipped: int


def topic_from_keyframe(
    path: str,
    topic: str,
    entries: List[_Entry],
    lead: List[_Entry],
    video_ids: Set[int],
    max_lead_in_ns: int,
) -> Tuple[List[_Entry], int]:
    """A topic's messages with each video channel from a keyframe on, and how
    many are lead-in.

    A topic row must decode on its own, and each channel in ``video_ids`` is a
    stream of its own. ``lead`` is the look-back before the time range. The
    other channels are kept as they are.
    """
    starts = {
        channel_id: _stream_start(
            path,
            topic,
            _of_channel(entries, channel_id),
            _of_channel(lead, channel_id),
            max_lead_in_ns,
        )
        for channel_id in video_ids
    }
    lead_in = _skip_first(lead, {c: s.lead_skipped for c, s in starts.items()})
    kept = _skip_first(entries, {c: s.skipped for c, s in starts.items()})
    return lead_in + kept, len(lead_in)


def _stream_start(
    path: str,
    topic: str,
    entries: List[_Entry],
    lead: List[_Entry],
    max_lead_in_ns: int,
) -> _StreamStart:
    """Where one video channel's stream starts, so that it decodes on its own.

    If ``entries`` do not open on a keyframe, the lead-in runs from the last
    keyframe of ``lead``. Without one, the frames before the first keyframe of
    ``entries`` are dropped. Without either, the channel is dropped with a
    warning.
    """
    schema, channel, message = entries[0]
    # A range holding only VP9 or AV1 inter frames names no codec in its
    # bytes, but a keyframe in its look-back does.
    codec = _channel_codec(
        channel, schema, (m.data for _, _, m in entries + lead), max_lead_in_ns
    )
    no_lead_in = len(lead)
    if (
        codec is None
        or codec.every_frame_is_a_keyframe
        or is_keyframe(message.data, codec)
    ):
        return _StreamStart(no_lead_in, 0)
    for index in range(len(lead) - 1, -1, -1):
        if is_keyframe(lead[index][2].data, codec):
            return _StreamStart(index, 0)
    for index, (_, _, candidate) in enumerate(entries):
        if is_keyframe(candidate.data, codec):
            return _StreamStart(no_lead_in, index)
    if log_once(f"mcap_topic_no_keyframe:{topic}"):
        logger.warning(
            "Video topic %r of %r (channel %d) has no keyframe in the selected "
            "span nor in the %.2f s before it, so its frames cannot be decoded "
            "and are dropped from the topic row. Widen time_range, or raise "
            "RAY_DATA_MCAP_MAX_LEAD_IN_S.",
            topic,
            path,
            channel.id,
            max_lead_in_ns / 1e9,
        )
    return _StreamStart(no_lead_in, len(entries))


def _of_channel(entries: List[_Entry], channel_id: int) -> List[_Entry]:
    return [entry for entry in entries if entry[1].id == channel_id]


def _skip_first(entries: List[_Entry], counts: Dict[int, int]) -> List[_Entry]:
    """``entries`` in order, less each channel's first ``counts[channel]`` ones."""
    seen: Dict[int, int] = {}
    kept: List[_Entry] = []
    for entry in entries:
        channel_id = entry[1].id
        seen[channel_id] = seen.get(channel_id, 0) + 1
        if seen[channel_id] > counts.get(channel_id, 0):
            kept.append(entry)
    return kept


def _channel_codec(
    channel: "Channel",
    schema: Optional["Schema"],
    payloads: Iterable[bytes],
    max_lead_in_ns: int,
) -> Optional[VideoCodec]:
    """A video channel's codec (:func:`channel_codec`); warns if none is found.

    When the bytes decide, the first payloads may name no codec (VP9 or AV1
    inter frames), so all of them are searched.
    """
    codec = channel_codec(
        schema.name if schema else None, channel.message_encoding, payloads
    )
    if codec is None:
        _warn_capped_lead_in(channel.topic, max_lead_in_ns)
    return codec


def _warn_capped_lead_in(topic: str, max_lead_in_ns: int) -> None:
    if log_once(f"mcap_capped_lead_in:{topic}"):
        logger.warning(
            "Video topic %r uses a codec that neither its format field nor its "
            "bytes name (JPEG, PNG, H.264, H.265, VP9 and AV1 are recognised). "
            "Its window rows carry the whole %.2f s look-back as lead-in rather "
            "than the frames since the last keyframe, and its topic rows start "
            "at the first message. RAY_DATA_MCAP_MAX_LEAD_IN_S sets the span.",
            topic,
            max_lead_in_ns / 1e9,
        )
