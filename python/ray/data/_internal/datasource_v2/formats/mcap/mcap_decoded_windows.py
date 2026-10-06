"""Decoded window rows: each window's frames per video topic, decoded in the task.

``CoarseRows`` places the windows a task owns and reads their entries. Each
planned video topic is decoded once for the task, so overlapping windows share
its frames. Its ``_WindowChannel`` is primed from the lead-in before the first
window, with the cold-channel rules of message rows. Each frame goes to every
window that holds its log time, in ``frames:<topic>`` and ``frame_times:<topic>``,
and the other topics fill the message lists. A window is emitted as soon as it
is complete, so a task holds the frames of only a few windows::

    windows this task owns, and their entries          (CoarseRows.placed_windows)
          |                                  |
          | planned video topics             | other topics
          v                                  |
    _WindowChannel per topic:                |
    prime from the lead-in,                  |
    decode each frame once                   |
          | frames                           | entries
          v                                  v
    _PendingWindows: add each to every window holding it, and emit a window
    once the entries reach its end and every decoded channel is past it
          |
          v
    CoarseRow -> table                                           (CoarseRowBatch)
"""

import bisect
import logging
from dataclasses import dataclass, field as dataclasses_field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_layout import (
    FRAMES_PREFIX,
    CoarseRow,
    _Entry,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_rows import (
    CoarseRows,
    FinishTable,
    RowSettings,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    FrameThinner,
    warn_cold_channel,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_lead_in import (
    ChannelMessages,
    last_keyframe,
    video_channels,
    window_lead_in,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    carries_picture,
    channel_codec,
    is_keyframe,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video_source import FpsSeed
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import Message
    from mcap.summary import Summary

    from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import _Assignment

logger = logging.getLogger(__name__)


@dataclass
class _PendingWindow:
    """An owned window being filled while its task's entries stream by."""

    start: int
    end: int
    # The messages inside it that go to the list columns, in log-time order.
    messages: List[_Entry] = dataclasses_field(default_factory=list)
    # Per decoded topic: the kept frames' log times and the frames themselves.
    frames: Dict[str, Tuple[List[int], List[Any]]] = dataclasses_field(
        default_factory=dict
    )
    decoded_bytes: int = 0


@dataclass
class _WindowChannel:
    """One decoded video channel of a window task.

    It follows the message path's rules. The channel is cold until a keyframe
    is in reach, and parameter sets are fed but never counted. With
    ``frames:`` pruned there is no decoder, and only the frame times a decoder
    would keep are produced.
    """

    topic: str
    codec: VideoCodec
    thinner: FrameThinner
    decoder: Optional[FrameDecoder] = None
    # Entries at or after the anchor still to feed. The stream is flushed when
    # this reaches zero.
    remaining: int = 0
    decodable: bool = False
    # Log time of the last picture the decoder took, and of the last frame out
    # of it, kept or thinned.
    last_picture: Optional[int] = None
    released: Optional[int] = None
    done: bool = False
    # For the cold-channel warning: the file and the look-back cap.
    path: str = ""
    cap_ns: int = 0

    def prime(self, lead: List[_Entry], before: Optional[int] = None) -> None:
        """Feed the lead-in before the anchor and drop its frames.

        Decoding then starts as in a whole-file read. A lead-in picture takes
        its ``fps`` interval, unless the decoder is fed it and rejects it.
        """
        keyframe_at = last_keyframe(lead, self.codec)
        self.decodable = keyframe_at is not None or self.codec.every_frame_is_a_keyframe
        if not self.decodable and any(
            carries_picture(m.data, self.codec) for _, _, m in lead
        ):
            warn_cold_channel(
                self.topic,
                self.path,
                self.cap_ns,
                lead[-1][2].log_time if before is None else before,
            )
        fed: Set[int] = set()
        if self.decoder is not None:
            fed = self._feed_lead_in(lead, keyframe_at)
        for index, (_, _, message) in enumerate(lead):
            if index not in fed and carries_picture(message.data, self.codec):
                self.thinner.observe(message.log_time)
        if self.remaining == 0:
            self.finish()

    def _feed_lead_in(self, lead: List[_Entry], keyframe_at: Optional[int]) -> Set[int]:
        """Decode the lead-in from its last keyframe on and drop the frames.

        Parameter sets written on their own are fed too, since the keyframe
        needs them. Returns the indexes fed. The decoder takes the ``fps``
        interval of each fed picture that decodes, as in a whole-file read.
        """
        assert self.decoder is not None
        fed = {
            index
            for index, (_, _, message) in enumerate(lead)
            if (keyframe_at is not None and index >= keyframe_at)
            or not carries_picture(message.data, self.codec)
        }
        for index in sorted(fed):
            for _ in self.decoder.decode(lead[index][2]):
                pass
        return fed

    def feed(self, message: "Message") -> List[Tuple[int, Any]]:
        """Feed one entry at or after the anchor and return the frames it releases."""
        self.remaining -= 1
        has_picture = carries_picture(message.data, self.codec)
        if has_picture and not self.decodable:
            if not is_keyframe(message.data, self.codec):
                # Cold: nothing to decode against until the next keyframe. The
                # frame still takes its ``fps`` interval, as in a whole-file read.
                self.thinner.observe(message.log_time)
                return []
            self.decodable = True
        released: List[Tuple[int, Any]] = []
        if self.decoder is not None:
            released.extend(self.decoder.decode(message))
            # A thinned frame shows how far the decoder got as well as a kept
            # one, so ``fps`` does not hold windows back.
            self.released = self.decoder.last_output
            # A rejected picture has no frame to wait for.
            if has_picture and not self.decoder.rejected_last:
                self.last_picture = message.log_time
        elif has_picture and self.thinner.keep(message.log_time):
            released.append((message.log_time, None))
            self.released = message.log_time
        return released

    def finish(self) -> List[Tuple[int, Any]]:
        """Drain the decoder at the end of the stream."""
        released: List[Tuple[int, Any]] = []
        if self.decoder is not None:
            for log_time, frame in self.decoder.flush():
                # An unstamped frame belongs to the last picture fed.
                stamp = log_time if log_time is not None else self.last_picture
                if stamp is not None:
                    released.append((stamp, frame))
        self.done = True
        return released

    def past(self, end: int) -> bool:
        """Whether every frame before ``end`` is out.

        It is asked once the entries have reached ``end``, so the channel gets
        no more pictures before it. Frames leave a B-frame-free decoder in
        log-time order, kept or thinned, so it is enough that the last picture
        the decoder took, or a frame at or past ``end``, is out. A channel that
        has fed no picture, or has no decoder, holds none.
        """
        if self.done or self.decoder is None or self.last_picture is None:
            return True
        due = min(end, self.last_picture)
        return self.released is not None and self.released >= due


@dataclass(frozen=True)
class _WindowStreams:
    """A decoded window task's channels, sorted by where their messages go."""

    # Per planned video channel, its entries in log-time order.
    video: Dict[int, List[_Entry]]
    # The other channels, whose messages fill the list columns.
    plain: Set[int]
    # The video channels among ``plain``: planning did not decode them, so they
    # stay encoded, with the lead-in a decoder needs.
    unplanned: Set[int]


class _PendingWindows:
    """The windows a decoded window task owns, filled as its entries stream by.

    A window is complete once the entries have reached its end and every
    decoded channel is past it. Its row is then handed out, and the window
    lets go of its contents.
    """

    def __init__(
        self,
        windows: Sequence[Tuple[int, int]],
        channels: Sequence[_WindowChannel],
        *,
        path: str,
        digest: str,
        length_ns: int,
        anchor: int,
        selection: MCAPSelection,
        max_row_bytes: int,
        lead_in: Optional[Callable[[int, List[_Entry]], List[_Entry]]] = None,
    ):
        self._windows = [_PendingWindow(start, end) for start, end in windows]
        self._starts = [start for start, _ in windows]
        # The first window not handed out yet.
        self._next = 0
        self._channels = channels
        self._path = path
        self._digest = digest
        self._length_ns = length_ns
        self._anchor = anchor
        self._selection = selection
        self._max_row_bytes = max_row_bytes
        # The lead-in of the encoded video topics in a window's messages, for
        # a window opening at the given start.
        self._lead_in = lead_in

    def add_message(self, entry: _Entry) -> None:
        """Add a list-column message to the windows holding it, if in the time range."""
        log_time = entry[2].log_time
        if self._selection.in_time_range(log_time):
            for index in self._holding(log_time):
                self._windows[index].messages.append(entry)

    def add_frames(self, topic: str, released: List[Tuple[int, Any]]) -> None:
        """Add a channel's released frames to the windows holding them.

        Frames before the anchor or outside the time range are dropped. A
        ``None`` frame (``frames:`` pruned) adds only its log time. A window
        whose frames pass the row limit fails the read.
        """
        for log_time, frame in released:
            if log_time < self._anchor or not self._selection.in_time_range(log_time):
                continue
            for index in self._holding(log_time):
                window = self._windows[index]
                frame_times, frames = window.frames.setdefault(topic, ([], []))
                frame_times.append(log_time)
                if frame is None:
                    continue
                frames.append(frame)
                window.decoded_bytes += frame.nbytes
                if window.decoded_bytes > self._max_row_bytes:
                    raise ValueError(
                        f"The decoded frames of window [{window.start}, {window.end}) "
                        f"of {self._path!r} would exceed {self._max_row_bytes} bytes "
                        "(RAY_DATA_MCAP_MAX_ROW_BYTES): thin them with "
                        "VideoOptions(fps=...), shrink them with "
                        "resize=(height, width), or read at "
                        "read_granularity='message'."
                    )

    def ready_rows(self, up_to: Optional[int]) -> Iterator[CoarseRow]:
        """Yield the rows of the windows that are complete, in window order.

        ``up_to`` is the log time the entries have reached. ``None`` means every
        entry is in, so every remaining window is complete. A window with
        neither messages nor frames gives no row.
        """
        while self._next < len(self._windows):
            window = self._windows[self._next]
            if up_to is not None and (
                window.end > up_to
                or not all(s.past(window.end) for s in self._channels)
            ):
                break
            self._next += 1
            if not window.messages and not any(
                frame_times for frame_times, _ in window.frames.values()
            ):
                continue
            lead = (
                self._lead_in(window.start, window.messages)
                if self._lead_in is not None
                else []
            )
            row = CoarseRow(
                path=self._path,
                row_id=f"{self._path}#[{window.start},{window.end})@{self._digest}",
                messages=lead + window.messages,
                window=(window.start, window.end),
                num_lead_in=len(lead),
                frames=window.frames,
                decoded_bytes=window.decoded_bytes,
            )
            # The row owns those containers now. The window lets go of them,
            # so the task holds only its in-flight windows in memory.
            window.messages = []
            window.frames = {}
            window.decoded_bytes = 0
            yield row

    def _holding(self, log_time: int) -> range:
        """The windows not handed out yet whose span holds ``log_time``."""
        # Windows share one length, so those holding ``log_time`` are the
        # ones starting in ``(log_time - length, log_time]``.
        low = bisect.bisect_right(self._starts, log_time - self._length_ns)
        high = bisect.bisect_right(self._starts, log_time)
        return range(max(low, self._next), high)


class DecodedWindowRows:
    """Builds the decoded window rows of one read task."""

    def __init__(
        self, settings: RowSettings, coarse_rows: CoarseRows, finish: FinishTable
    ):
        self._settings = settings
        # Places the windows and builds the batches of decoded window rows.
        self._coarse_rows = coarse_rows
        self._finish = finish

    def tables(
        self, f: Any, assignment: "_Assignment", summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit the window rows this task owns for one file, decoding video topics."""
        placed = self._coarse_rows.placed_windows(f, assignment, summary)
        if placed is not None:
            yield from self._decoded_window_rows(
                assignment, placed.entries, placed.windows, FpsSeed(f, summary)
            )

    def _decoded_window_rows(
        self,
        assignment: "_Assignment",
        entries: List[_Entry],
        windows: Sequence[Tuple[int, int]],
        fps_seed: FpsSeed,
    ) -> Iterator[pa.Table]:
        """Cut ``entries`` (log-time ordered) into windows, decoding the video topics.

        Each planned video topic's stream is decoded once for the task. It is
        primed from the last keyframe before the first owned window, with the
        cold-channel rules of message rows. Its ``fps`` thinning is seeded as
        theirs, so a frame comes out the same whichever task's window holds
        it. Each kept frame goes to every owned window that holds its log
        time. Frames before the first window or outside the time range are
        dropped. The other topics fill the list columns. A window is emitted
        once every decoded channel is past its end, so a task holds the frames
        of only a few windows. A window whose frames would pass the row limit
        fails before it is built.
        """
        assert self._settings.video is not None and self._settings.window is not None
        if not windows:
            return
        video = self._settings.video
        # One frame-shape memory per task, shared by its batches, so a window
        # without frames still gets a tensor of the topic's shape.
        frame_shape: Dict[str, Tuple[int, int]] = (
            {t: video.resize for t in self._settings.decoded_topics}
            if video.resize is not None
            else {}
        )
        batch = self._coarse_rows.new_batch(frame_shape)
        for row in self._decoded_windows(assignment.path, entries, windows, fps_seed):
            batch.add(row)
            if (
                self._settings.target_block_size is not None
                and batch.payload_bytes >= self._settings.target_block_size
            ):
                yield self._finish(batch.build(), assignment, 0)
                batch = self._coarse_rows.new_batch(frame_shape)
        if len(batch) > 0:
            yield self._finish(batch.build(), assignment, 0)

    def _decoded_windows(
        self,
        path: str,
        entries: List[_Entry],
        windows: Sequence[Tuple[int, int]],
        fps_seed: FpsSeed,
    ) -> Iterator[CoarseRow]:
        """Decode the video streams and yield the window rows as they complete."""
        range_start = self._settings.selection.start_time
        first_start = windows[0][0]
        anchor = first_start if range_start is None else max(first_start, range_start)
        streams = self._split_window_streams(entries, path)
        encoded_video = video_channels(
            [e for e in entries if e[1].id in streams.unplanned],
            self._settings.is_video,
            self._settings.max_lead_in_ns,
        )
        channels: Dict[int, _WindowChannel] = {}
        try:
            self._set_up_channels(channels, streams.video, path, anchor, fps_seed)
            pending = self._pending_windows(
                windows, channels, path, anchor, encoded_video
            )
            yield from _stream_entries(
                entries, first_start, anchor, streams, channels, pending
            )
            _finish_channels(channels, pending)
        finally:
            # A read that fails or stops early must still release its decoders.
            _close_decoders(channels)
        yield from pending.ready_rows(None)

    def _split_window_streams(self, entries: List[_Entry], path: str) -> _WindowStreams:
        """Split the channels into planned video streams and list-column channels."""
        planned = set(self._settings.decoded_topics)
        video: Dict[int, List[_Entry]] = {}
        plain: Set[int] = set()
        unplanned: Set[int] = set()
        for entry in entries:
            schema, channel, _ = entry
            if channel.id in video:
                video[channel.id].append(entry)
            elif channel.id in plain:
                continue
            elif channel.topic in planned:
                video[channel.id] = [entry]
            else:
                if self._settings.is_video(channel, schema):
                    unplanned.add(channel.id)
                if channel.id in unplanned and log_once(
                    f"mcap_unplanned_video:{channel.topic}"
                ):
                    logger.warning(
                        "Video topic %r in %r was not in the files planning "
                        "sampled, so its frames stay encoded in the message lists "
                        "of the window rows; list it in video_topics=[...] to "
                        "decode it.",
                        channel.topic,
                        path,
                    )
                plain.add(channel.id)
        return _WindowStreams(video, plain, unplanned)

    def _set_up_channels(
        self,
        channels: Dict[int, _WindowChannel],
        video: Dict[int, List[_Entry]],
        path: str,
        anchor: int,
        fps_seed: FpsSeed,
    ) -> None:
        """Build each video stream's state, add it to ``channels`` and prime it.

        A stream whose codec no payload names gets no state.
        """
        wanted = (
            set(self._settings.columns) if self._settings.columns is not None else None
        )
        for channel_id, stream in video.items():
            state = self._window_channel(stream, path, wanted)
            if state is None:
                continue
            # Kept before priming, so the caller closes its decoder even if
            # priming fails.
            channels[channel_id] = state
            self._prime_window_channel(state, stream, anchor, fps_seed)

    def _window_channel(
        self,
        stream: List[_Entry],
        path: str,
        wanted: Optional[Set[str]],
    ) -> Optional[_WindowChannel]:
        """Build the decoding state of one video stream, not yet primed.

        Returns ``None`` when no payload of the stream names its codec.
        """
        assert self._settings.video is not None
        video = self._settings.video
        codec = _stream_codec(stream, path)
        if codec is None:
            return None
        topic = stream[0][1].topic
        state = _WindowChannel(
            topic=topic,
            codec=codec,
            thinner=FrameThinner(video.fps_interval_ns),
            path=path,
            cap_ns=self._settings.max_lead_in_ns,
        )
        # Without ``frames:`` there is no decoder: the channel produces only
        # the frame times a decoder would keep. It runs even without
        # ``frame_times:``, so a count or a projection to the window bounds
        # still sees every window that holds a frame.
        if wanted is None or f"{FRAMES_PREFIX}{topic}" in wanted:
            state.decoder = FrameDecoder(
                codec, resize=video.resize, thinner=state.thinner
            )
        return state

    def _prime_window_channel(
        self,
        state: _WindowChannel,
        stream: List[_Entry],
        anchor: int,
        fps_seed: FpsSeed,
    ) -> None:
        """Feed the stream's lead-in before ``anchor`` and seed its ``fps`` thinning.

        The task reads the stream back to the look-back cap before ``anchor``.
        """
        cap = self._settings.max_lead_in_ns
        times = [e[2].log_time for e in stream]
        first = bisect.bisect_left(times, anchor)
        state.remaining = len(stream) - first
        state.prime(
            stream[bisect.bisect_left(times, anchor - cap) : first], before=anchor
        )
        if first < len(stream):
            channel_id = stream[0][1].id
            fps_seed.apply(state.thinner, channel_id, times[first], (0, anchor - cap))

    def _pending_windows(
        self,
        windows: Sequence[Tuple[int, int]],
        channels: Dict[int, _WindowChannel],
        path: str,
        anchor: int,
        encoded_video: Dict[int, ChannelMessages],
    ) -> _PendingWindows:
        """The task's windows, to fill as the entries stream by.

        A window's messages carry the lead-in of ``encoded_video``, the video
        channels that stay encoded.
        """
        assert self._settings.window is not None
        lead_in = (
            partial(
                window_lead_in,
                encoded_video,
                range_start=self._settings.selection.start_time,
                max_lead_in_ns=self._settings.max_lead_in_ns,
            )
            if encoded_video
            else None
        )
        return _PendingWindows(
            windows,
            list(channels.values()),
            path=path,
            digest=self._settings.row_digest,
            length_ns=self._settings.window.length_ns,
            anchor=anchor,
            selection=self._settings.selection,
            max_row_bytes=self._settings.max_row_bytes,
            lead_in=lead_in,
        )


def _stream_codec(stream: List[_Entry], path: str) -> Optional[VideoCodec]:
    """The codec of a video stream, or ``None``, with a warning, if nothing names it.

    Without a ``format`` naming the codec, the bytes decide. A VP9 or AV1 inter
    frame names none: look on until a payload (the next keyframe) does.
    """
    schema, channel, _ = stream[0]
    codec = channel_codec(
        schema.name if schema else None,
        channel.message_encoding,
        (e[2].data for e in stream),
    )
    if codec is None and log_once(f"mcap_codec_pending:{channel.topic}"):
        logger.warning(
            "The codec of video topic %r in %r cannot be told from any "
            "of its payloads in this task; its frames are skipped.",
            channel.topic,
            path,
        )
    return codec


def _stream_entries(
    entries: List[_Entry],
    first_start: int,
    anchor: int,
    streams: _WindowStreams,
    channels: Dict[int, _WindowChannel],
    pending: _PendingWindows,
) -> Iterator[CoarseRow]:
    """Stream the entries from ``first_start`` on into ``pending``.

    A decoded channel is fed its entries from ``anchor`` on, and the plain
    channels' entries go to the list columns. Yields each window as it
    completes.
    """
    first_entry = bisect.bisect_left([e[2].log_time for e in entries], first_start)
    for entry in entries[first_entry:]:
        _, channel, message = entry
        state = channels.get(channel.id)
        if state is not None and message.log_time >= anchor and not state.done:
            pending.add_frames(state.topic, state.feed(message))
            if state.remaining == 0:
                pending.add_frames(state.topic, state.finish())
        elif channel.id in streams.plain:
            pending.add_message(entry)
        # A planned video channel whose codec no payload names is in
        # neither: its messages are neither frames nor list entries.
        yield from pending.ready_rows(message.log_time)


def _finish_channels(
    channels: Dict[int, _WindowChannel], pending: _PendingWindows
) -> None:
    """Drain the decoders still running and add the frames they release."""
    for state in channels.values():
        if not state.done:
            pending.add_frames(state.topic, state.finish())


def _close_decoders(channels: Dict[int, _WindowChannel]) -> None:
    """Release every decoder, also when the task fails or stops early."""
    for state in channels.values():
        if state.decoder is not None:
            state.decoder.close()
