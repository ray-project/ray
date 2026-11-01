"""Where a video stream starts: its parameter sets, for a decoder that lacks them.

An H.264 or H.265 decoder needs the stream's parameter sets before its first
picture. Most encoders repeat them on every keyframe. Some write them only
before the first one, so a decoder whose first keyframe comes without them reads
them where the stream starts (``StreamStart``): one more read, for such a stream
only (K* carries the parameter sets)::

    frame       0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19
    /camera     K* p  p  p  p  K  p  p  p  p  K  p  p  p  p  K  p  p  p  p
    chunks      [ first ][        other samples       ][   these samples   ]
    reads       [ start ]                  [ look-back][    their frames   ]

That read lies in a chunk the task may not need otherwise, so it plans with the
file's whole summary. A sample read reads back to each camera's keyframe itself
(``mcap_decoded_samples``).
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, Iterator, List, Optional, Set

from ray.data._internal.datasource_v2.formats.mcap.mcap_io import buffered, read_chunk
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    NalFormat,
    VideoCodec,
    is_keyframe,
    parameter_set_kinds,
    parameter_set_types,
    parameter_sets,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import ChunkIndex
    from mcap.summary import Summary

logger = logging.getLogger(__name__)


def stream_start_chunks(summary: "Summary", channel_id: int) -> List["ChunkIndex"]:
    """The chunks where the channel's stream may start, in file order: those
    written without message indexes, which may hold any channel, up to the first
    whose index names the channel."""
    chunks = []
    for chunk in sorted(summary.chunk_indexes, key=lambda c: c.chunk_start_offset):
        if not chunk.message_index_offsets:
            chunks.append(chunk)
        elif channel_id in chunk.message_index_offsets:
            chunks.append(chunk)
            break
    return chunks


@dataclass(frozen=True)
class StreamStart:
    """Reads a video stream's parameter sets where the stream starts.

    A decoder whose first keyframe comes without them asks once
    (``FrameDecoder``). They are taken from the channel's messages up to its
    first keyframe, in the first chunk that holds the channel
    (``stream_start_chunks``), or from the start of a file without a chunk index.
    Such a file is read whole before its frames are decoded, so reading its start
    again leaves the task's own read alone.
    """

    f: Any
    path: str
    summary: Optional["Summary"]

    def read(self, channel_id: int, topic: str, codec: VideoCodec) -> bytes:
        """The parameter sets at the start of the channel's stream, with start codes.

        Warns, naming the file and the topic, when a kind is missing there or
        the records there do not parse. The frames then cannot be decoded until
        a keyframe that carries them.
        """
        try:
            sets = _first_parameter_sets(
                self._stream_head(channel_id), channel_id, codec
            )
        except OSError:
            raise  # an I/O failure: the task's read is retried
        except Exception as e:  # noqa: BLE001 - corrupt records: decode without them
            logger.debug("Cannot read the start of %r in %r: %s", topic, self.path, e)
            sets = b""
        if parameter_set_kinds(sets, codec) != parameter_set_types(codec):
            _warn_missing_parameter_sets(topic, self.path, codec)
        return sets

    def _stream_head(self, channel_id: int) -> Iterable[Any]:
        """The records where the channel's stream starts, in file order: those
        of the first chunk with a message on the channel."""
        from mcap.records import Message
        from mcap.stream_reader import breakup_chunk

        if self.summary is None or not self.summary.chunk_indexes:
            return _records_from_start(self.f)
        for chunk in stream_start_chunks(self.summary, channel_id):
            records = list(breakup_chunk(read_chunk(self.f, chunk)))
            if any(
                isinstance(r, Message) and r.channel_id == channel_id for r in records
            ):
                return records
        return []


def _first_parameter_sets(
    records: Iterable[Any], channel_id: int, codec: VideoCodec
) -> bytes:
    """The parameter sets in the channel's messages up to its first keyframe.

    Each kind comes from the first message that holds it. Length-prefixed NAL
    units are read as such (``NalFormat``).
    """
    from mcap.records import Message

    needed = parameter_set_types(codec)
    nal_format = NalFormat(codec)
    found: List[bytes] = []
    kinds: Set[int] = set()
    for record in records:
        if not isinstance(record, Message) or record.channel_id != channel_id:
            continue
        payload = nal_format.annex_b(record.data)
        sets = parameter_sets(payload, codec, skip=kinds)
        found.append(sets)
        kinds |= parameter_set_kinds(sets, codec)
        if kinds >= needed or is_keyframe(payload, codec):
            break
    return b"".join(found)


def _records_from_start(f: Any) -> Iterator[Any]:
    """The records of ``f`` from its start, read through a buffer of
    ``DataContext.streaming_read_buffer_size`` bytes."""
    from mcap.stream_reader import StreamReader

    from ray.data.context import DataContext

    f.seek(0)
    stream = buffered(f, DataContext.get_current().streaming_read_buffer_size)
    yield from StreamReader(stream).records


def _warn_missing_parameter_sets(topic: str, path: str, codec: VideoCodec) -> None:
    """Warn once per topic of a file that its frames lack parameter sets."""
    if log_once(f"mcap_missing_parameter_sets:{path}:{topic}"):
        logger.warning(
            "Video topic %r in %r has a keyframe without the stream's %s parameter "
            "sets, and they cannot be read from the start of the stream either. "
            "Its frames are skipped until a keyframe that carries them.",
            topic,
            path,
            codec.name,
        )
