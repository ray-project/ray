"""Decoded sample rows: a camera topic's steps as RGB frames.

With ``video``, a sample read decodes its camera topics in the task, which
planning settles from the files it samples. ``SampleRows`` resolves the steps
of a group of samples whose steps meet first (``mcap_sample_rows``), so each
camera is decoded once per group: from the last keyframe at or before the
first frame a step takes, through the last one. Only the frames that steps
take become RGB::

    camera messages        K  P  P  P  P  K  P  P  P  P
    frames steps take            x     x        x
    fed to the decoder     K  P  P  P  P  K  P  P
    converted to RGB             x     x        x

The keyframe is searched for back from the first frame, chunk by chunk, at
most ``RAY_DATA_MCAP_MAX_LEAD_IN_S`` back. A frame that does not decode, such
as one with no keyframe in reach, leaves its step empty, and the topic's pad
fills it (``FrameColumns``).

A camera's decoder keeps running from one group to the next of the same file
when no keyframe lies between them, as for back-to-back clips. Each frame
from the keyframe on is then fed once::

    camera messages        K  P  P  P  P  P  P  P  K  P
    frames steps take            x  x     x  x
    fed to the decoder     K  P  P  P  P  P  P
"""

import logging
from dataclasses import dataclass, replace
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

import numpy as np
import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    warn_cold_channel,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    FRAMES_PREFIX,
    TIMES_PREFIX,
    VALID_PREFIX,
    Step,
    TopicPlan,
    TopicTimes,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    CodecSearch,
    NalFormat,
    VideoCodec,
    carries_picture,
    is_keyframe,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video_source import (
    StreamStart,
)
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorArray,
    ArrowVariableShapedTensorArray,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.summary import Summary

logger = logging.getLogger(__name__)

# A message as a row holds it: schema, channel, record.
_Entry = Tuple[Any, Any, Any]
# How a decoded frame is found again: its channel id and log time.
FrameKey = Tuple[int, int]

# How many messages the keyframe search looks at first. It looks at four
# times as many each time it finds none, up to the look-back cap.
_FIRST_LOOK_BACK = 16


@dataclass(frozen=True)
class _Running:
    """A camera's decoder kept running between groups of samples.

    ``last_fed`` is the log time of the last message fed, and ``keyframe``
    that of the latest keyframe fed.
    """

    decoder: FrameDecoder
    last_fed: int
    keyframe: int


def frame_key(entry: _Entry) -> FrameKey:
    return entry[1].id, entry[2].log_time


@dataclass(frozen=True)
class _Stream:
    """One camera channel's stream in a run: its codec and how to read it."""

    channel_id: int
    topic: str
    codec: VideoCodec
    nal_format: NalFormat

    def as_annex_b(self, message: Any) -> Any:
        """``message`` with its payload as an Annex-B access unit."""
        data = self.nal_format.annex_b(message.data)
        return message if data is message.data else replace(message, data=data)


class RunDecoder:
    """Decodes the frames a group of samples' steps take, for one open file.

    A channel's codec is found once per file, from the payloads of the frames
    steps take or, if those name none, of the messages between and before them
    (``_stream``). A
    channel's decoder keeps running into the next group when that decodes the
    same frames as a fresh one for less; ``close`` ends the running ones.
    """

    def __init__(
        self,
        f: Any,
        path: str,
        summary: Optional["Summary"],
        *,
        resize: Optional[Tuple[int, int]],
        max_lead_in_ns: int,
    ):
        self._path = path
        self._resize = resize
        self._cap = max_lead_in_ns
        self._stream_start = StreamStart(f, path, summary)
        self._streams: Dict[int, _Stream] = {}
        self._running: Dict[int, _Running] = {}

    def frames(
        self, times: TopicTimes, taken: Iterable[_Entry]
    ) -> Dict[FrameKey, np.ndarray]:
        """The RGB frame of each taken message that decodes, by ``frame_key``.

        ``times`` is the topic's timeline, which the keyframe search and the
        frames between the taken ones are read from.
        """
        by_channel: Dict[int, Dict[FrameKey, _Entry]] = {}
        for entry in taken:
            by_channel.setdefault(entry[1].id, {})[frame_key(entry)] = entry
        frames: Dict[FrameKey, np.ndarray] = {}
        for channel_id, entries in by_channel.items():
            first = min(entries.values(), key=lambda e: e[2].log_time)
            stream = self._stream(channel_id, first, times, entries)
            if stream is not None:
                frames.update(self._decode(stream, times, entries))
        return frames

    def _stream(
        self,
        channel_id: int,
        first: _Entry,
        times: TopicTimes,
        entries: Dict[FrameKey, _Entry],
    ) -> Optional[_Stream]:
        """The channel's stream, its codec found from its payloads, or ``None``
        when none names one.

        The search reads the taken frames, the messages between them, and then
        the look-back before the first: a VP9 frame names its codec only when
        it is a keyframe. A channel with no codec yet is searched again for the
        next group.
        """
        if channel_id in self._streams:
            return self._streams[channel_id]
        schema, channel, _ = first
        search = CodecSearch(schema.name if schema else None, channel.message_encoding)
        for entry in entries.values():
            if search.found:
                break
            search.feed(entry[2].data)
        if not search.found:
            last = max(e[2].log_time for e in entries.values())
            for _, entry in times.all_in(first[2].log_time, last):
                if entry[1].id == channel_id and not search.found:
                    search.feed(entry[2].data)
        if not search.found:
            self._search_look_back(search, channel_id, times, first[2].log_time)
        codec = search.result()
        if codec is None:
            if log_once(f"mcap_sample_codec_unknown:{channel.topic}"):
                logger.warning(
                    "No payload of video topic %r in %r names its codec, so its "
                    "frames are left empty and padded.",
                    channel.topic,
                    self._path,
                )
            return None
        stream = _Stream(channel_id, channel.topic, codec, NalFormat(codec))
        self._streams[channel_id] = stream
        return stream

    def _search_look_back(
        self, search: CodecSearch, channel_id: int, times: TopicTimes, first: int
    ) -> None:
        """Feed ``search`` the channel's messages before ``first``, nearest
        first, until one names the codec or the look-back cap is reached."""
        count = _FIRST_LOOK_BACK
        low = max(first - self._cap, 0)
        fed = 0
        while True:
            timed = times.last_in(low, first - 1, count)
            # A larger count adds only messages before those already fed.
            for _, entry in reversed(timed[: len(timed) - fed]):
                if entry[1].id == channel_id:
                    search.feed(entry[2].data)
                    if search.found:
                        return
            if len(timed) < count:
                return
            fed = len(timed)
            count *= 4

    def close(self) -> None:
        """End the decoders kept running, as a read must when it stops."""
        running, self._running = self._running, {}
        for kept in running.values():
            kept.decoder.close()

    def _decode(
        self, stream: _Stream, times: TopicTimes, entries: Dict[FrameKey, _Entry]
    ) -> Dict[FrameKey, np.ndarray]:
        """Decode the stream through the last taken frame: on from the
        channel's running decoder, or else from the keyframe at or before the
        first taken frame."""
        wanted = {log_time for _, log_time in entries}
        running = self._running.pop(stream.channel_id, None)
        feed = None
        if running is not None:
            feed = self._fed_on(stream, times, entries, running)
            if feed is None:
                running.decoder.close()
                running = None
        if running is not None and feed is not None:
            decoder = running.decoder
        else:
            decoder = FrameDecoder(
                stream.codec,
                resize=self._resize,
                read_parameter_sets=partial(
                    self._stream_start.read, stream.channel_id, stream.topic
                ),
            )
            feed = self._feed(stream, times, entries)
        frames: Dict[FrameKey, np.ndarray] = {}
        kept: Optional[_Running] = None
        try:
            for message in feed:
                for log_time, frame in decoder.decode(message, wanted.__contains__):
                    frames[(stream.channel_id, log_time)] = frame
            if all((stream.channel_id, t) in frames for t in wanted):
                kept = self._still_running(stream, decoder, feed, running)
            if kept is None:
                for log_time, frame in decoder.flush():
                    if log_time in wanted:
                        frames[(stream.channel_id, log_time)] = frame
        finally:
            if kept is None:
                decoder.close()
        if kept is not None:
            self._running[stream.channel_id] = kept
        return frames

    def _fed_on(
        self,
        stream: _Stream,
        times: TopicTimes,
        entries: Dict[FrameKey, _Entry],
        running: _Running,
    ) -> Optional[List[Any]]:
        """The messages that carry ``running`` on through the last taken frame,
        or ``None`` when a fresh decoder should take over.

        A fresh decoder starts at the last keyframe at or before the first taken
        frame. Going on decodes the same frames for less only when that keyframe
        is the running decoder's: none lies between its last message and the
        first taken frame, and the fresh decoder's look-back would reach it.
        """
        if stream.codec.every_frame_is_a_keyframe:
            return None
        first = min(log_time for _, log_time in entries)
        last = max(log_time for _, log_time in entries)
        if running.last_fed >= first or first - running.keyframe > self._cap:
            return None
        feed = []
        for _, entry in times.all_in(running.last_fed + 1, last):
            if entry[1].id != stream.channel_id:
                continue
            message = stream.as_annex_b(entry[2])
            if message.log_time <= first and is_keyframe(message.data, stream.codec):
                return None
            feed.append(message)
        return feed

    def _still_running(
        self,
        stream: _Stream,
        decoder: FrameDecoder,
        feed: List[Any],
        running: Optional[_Running],
    ) -> Optional[_Running]:
        """``decoder`` kept running after ``feed``, or ``None`` when it must be
        flushed: it holds a frame back, or has not been fed a keyframe.

        The latest keyframe fed is in ``feed``, or else it is the one of
        ``running``, the decoder that ``feed`` went on from.
        """
        if stream.codec.every_frame_is_a_keyframe:
            return None
        pictures = [m for m in feed if carries_picture(m.data, stream.codec)]
        if not pictures or decoder.last_output != pictures[-1].log_time:
            return None
        keyframe = next(
            (m.log_time for m in reversed(feed) if is_keyframe(m.data, stream.codec)),
            None if running is None else running.keyframe,
        )
        if keyframe is None:
            return None
        return _Running(decoder, feed[-1].log_time, keyframe)

    def _feed(
        self, stream: _Stream, times: TopicTimes, entries: Dict[FrameKey, _Entry]
    ) -> List[Any]:
        """The messages to feed, as Annex-B access units.

        Stills decode on their own, so only the taken ones are fed. A stream
        with inter frames is fed from the last keyframe at or before the
        first taken frame, after the parameter sets written in messages of
        their own before it. Without a keyframe in reach, its pictures are
        skipped until the next keyframe.
        """
        taken = sorted(entries.values(), key=lambda e: e[2].log_time)
        if stream.codec.every_frame_is_a_keyframe:
            return [stream.as_annex_b(e[2]) for e in taken]
        first, last = taken[0][2].log_time, taken[-1][2].log_time
        lead = self._look_back(stream, times, first)
        keyframe = next(
            (
                i
                for i in range(len(lead) - 1, -1, -1)
                if is_keyframe(lead[i].data, stream.codec)
            ),
            None,
        )
        start = len(lead) if keyframe is None else keyframe
        feed = [m for m in lead[:start] if not carries_picture(m.data, stream.codec)]
        feed += lead[start:]
        cold = keyframe is None
        if cold and any(
            carries_picture(m.data, stream.codec) and m.log_time < first for m in lead
        ):
            warn_cold_channel(stream.topic, self._path, self._cap, first)
        for _, entry in times.all_in(first + 1, last):
            if entry[1].id != stream.channel_id:
                continue
            message = stream.as_annex_b(entry[2])
            if cold and carries_picture(message.data, stream.codec):
                if not is_keyframe(message.data, stream.codec):
                    continue
                cold = False
            feed.append(message)
        return feed

    def _look_back(self, stream: _Stream, times: TopicTimes, first: int) -> List[Any]:
        """The channel's messages back from ``first`` to its last keyframe at or
        before it, in log-time order, as Annex-B access units.

        It looks back at most the look-back cap, and returns all of that when no
        keyframe is in reach. Chunks are read nearest first, only as far as the
        search needs.
        """
        count = _FIRST_LOOK_BACK
        low = max(first - self._cap, 0)
        while True:
            timed = times.last_in(low, first, count)
            lead = [
                stream.as_annex_b(entry[2])
                for _, entry in timed
                if entry[1].id == stream.channel_id
            ]
            if (
                any(is_keyframe(m.data, stream.codec) for m in lead)
                or len(timed) < count
            ):
                return lead
            count *= 4


class FrameColumns:
    """A decoded topic's columns while rows are added: frames, times, validity.

    A step whose frame does not decode loses its validity, and its pad fills
    it: ``"edge"`` repeats the nearest step with a frame, ``"zeros"`` gives a
    black frame, and ``"raise"`` fails the read. A black frame takes the size
    of the row's other frames, or ``resize``, or the last size the topic had.
    """

    def __init__(self, plan: TopicPlan, resize: Optional[Tuple[int, int]]):
        self._plan = plan
        self._resize = resize
        self._size: Optional[Tuple[int, int]] = resize
        self._rows: List[List[Optional[np.ndarray]]] = []
        self.offsets: List[int] = [0]
        self.step_times: List[int] = []
        self.step_valid: List[bool] = []

    def add(
        self,
        steps: List[Step[_Entry]],
        frames: Optional[Dict[FrameKey, np.ndarray]],
        where: str,
    ) -> int:
        """Add one row's steps with their frames; return the bytes of the frames.

        ``frames`` is ``None`` when the read builds no ``frames:`` column: the
        steps keep their validity, and no frame is looked for.
        """
        row: List[Optional[np.ndarray]] = []
        valid = [step.valid for step in steps]
        if frames is not None:
            row = [
                None if s.message is None else frames.get(frame_key(s.message))
                for s in steps
            ]
            self._check_decoded(steps, row, where)
            valid = [v and frame is not None for v, frame in zip(valid, row)]
            row = self._padded(row)
        self._rows.append(row)
        self.step_times.extend(step.time for step in steps)
        self.step_valid.extend(valid)
        self.offsets.append(self.offsets[-1] + len(steps))
        return sum(frame.nbytes for frame in row if frame is not None)

    def _check_decoded(
        self, steps: List[Step[_Entry]], row: List[Optional[np.ndarray]], where: str
    ) -> None:
        """Fail on a step whose message does not decode, if its pad is ``raise``."""
        lost = [s for s, f in zip(steps, row) if s.message is not None and f is None]
        if lost and self._plan.pad == "raise":
            raise ValueError(
                f"{where}: the frame of topic {self._plan.topic!r} logged at "
                f"{lost[0].time} does not decode, and its pad is 'raise'. A frame "
                "with no keyframe within RAY_DATA_MCAP_MAX_LEAD_IN_S before it does "
                "not decode."
            )

    def _padded(self, row: List[Optional[np.ndarray]]) -> List[Optional[np.ndarray]]:
        """``row`` with each missing frame repeated from the nearest one, the
        earlier on a tie, under ``"edge"``; other pads leave it missing."""
        decoded = [i for i, frame in enumerate(row) if frame is not None]
        if self._plan.pad != "edge" or not decoded:
            return row
        nearest = [min(decoded, key=lambda j: (abs(j - i), j)) for i in range(len(row))]
        return [row[j] for j in nearest]

    def finish(self, wants: Callable[[str], bool]) -> List[Tuple[str, pa.Array]]:
        """The wanted columns of the rows added so far; the columns start over."""
        offsets = pa.array(self.offsets, pa.int32())
        columns = []
        if wants(FRAMES_PREFIX):
            columns.append((FRAMES_PREFIX, self._frames()))
        for prefix, values in (
            (TIMES_PREFIX, pa.array(self.step_times, pa.int64())),
            (VALID_PREFIX, pa.array(self.step_valid, pa.bool_())),
        ):
            if wants(prefix):
                columns.append(
                    (
                        prefix,
                        pa.ListArray.from_arrays(
                            offsets, values, type=pa.list_(values.type)
                        ),
                    )
                )
        self._rows = []
        self.offsets, self.step_times, self.step_valid = [0], [], []
        return columns

    def _frames(self) -> pa.Array:
        """One ``(steps, height, width, 3)`` tensor per row.

        With fixed steps and ``resize``, every row has one shape, so the rows
        fill one array.
        """
        rows = self._rows
        if self._plan.num_steps is None or self._resize is None:
            return ArrowVariableShapedTensorArray.from_numpy(
                [self._stack(row) for row in rows]
            )
        assert self._resize is not None and self._plan.num_steps is not None
        height, width = self._resize
        out = np.zeros((len(rows), self._plan.num_steps, height, width, 3), np.uint8)
        for r, row in enumerate(rows):
            for i, frame in enumerate(row):
                if frame is not None:
                    out[r, i] = frame
        return ArrowTensorArray.from_numpy(out)

    def _stack(self, row: List[Optional[np.ndarray]]) -> np.ndarray:
        """A row's frames in one array, black where a step has none."""
        sizes = {frame.shape[:2] for frame in row if frame is not None}
        if len(sizes) > 1:
            raise ValueError(
                f"Topic {self._plan.topic!r} changes frame size within one sample "
                f"row ({sorted(sizes)}); pass VideoOptions(resize=(height, width)) "
                "to decode it to one size."
            )
        if sizes:
            (self._size,) = sizes
        height, width = self._size or (0, 0)
        stack = np.zeros((len(row), height, width, 3), dtype=np.uint8)
        for i, frame in enumerate(row):
            if frame is not None:
                stack[i] = frame
        return stack
