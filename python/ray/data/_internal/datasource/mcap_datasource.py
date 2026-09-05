"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.
"""

import json
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from mcap.reader import Channel, Message, Schema

logger = logging.getLogger(__name__)

# Payload magic numbers. MCAP is codec-agnostic and the schema often does not say
# what a video payload is, so the encoding is sniffed from the bytes.
_JPEG_SOI = b"\xff\xd8\xff"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_ANNEXB_START_4 = b"\x00\x00\x00\x01"
_ANNEXB_START_3 = b"\x00\x00\x01"

# H.264 NAL unit types that can begin a decode: IDR slice, sequence parameter
# set, picture parameter set. See ITU-T H.264 table 7-1.
_ANNEXB_KEYFRAME_NAL_TYPES = frozenset({5, 7, 8})

# How far into a payload to look for a JPEG start-of-image marker. A ROS 2
# sensor_msgs/CompressedImage puts a header in front of the JPEG.
_EMBEDDED_JPEG_SEARCH_WINDOW = 256

_STILL_IMAGE_KINDS = frozenset({"jpeg", "png", "jpeg_embedded"})


@dataclass
class TimeRange:
    """Time range for filtering MCAP messages.

    Attributes:
        start_time: Start time in nanoseconds (inclusive).
        end_time: End time in nanoseconds (exclusive).
    """

    start_time: int
    end_time: int

    def __post_init__(self):
        """Validate time range after initialization."""
        if self.start_time >= self.end_time:
            raise ValueError(
                f"start_time ({self.start_time}) must be less than "
                f"end_time ({self.end_time})"
            )
        if self.start_time < 0 or self.end_time < 0:
            raise ValueError(
                f"time values must be non-negative, got start_time={self.start_time}, "
                f"end_time={self.end_time}"
            )


@DeveloperAPI
class MCAPDatasource(FileBasedDatasource):
    """MCAP (Message Capture) datasource for Ray Data.

    This datasource provides reading of MCAP files with predicate pushdown
    optimization for filtering by topics, time ranges, and message types.

    MCAP is a standardized format for storing timestamped messages from robotics and
    autonomous systems, commonly used for sensor data, control commands, and other
    time-series data.

    Examples:
        Basic usage:

        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.read_mcap("/path/to/data.mcap")  # doctest: +SKIP

        With topic filtering and time range:

        >>> from ray.data.datasource import TimeRange  # doctest: +SKIP
        >>> ds = ray.data.read_mcap(  # doctest: +SKIP
        ...     "/path/to/data.mcap",
        ...     topics={"/camera/image_raw", "/lidar/points"},
        ...     time_range=TimeRange(start_time=1000000000, end_time=2000000000)
        ... )  # doctest: +SKIP

        With multiple files and metadata:

        >>> ds = ray.data.read_mcap(  # doctest: +SKIP
        ...     ["file1.mcap", "file2.mcap"],
        ...     topics={"/camera/image_raw", "/lidar/points"},
        ...     message_types={"sensor_msgs/Image", "sensor_msgs/PointCloud2"},
        ...     include_metadata=True
        ... )  # doctest: +SKIP
    """

    _FILE_EXTENSIONS = ["mcap"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        topics: Optional[Union[List[str], Set[str]]] = None,
        time_range: Optional[TimeRange] = None,
        message_types: Optional[Union[List[str], Set[str]]] = None,
        include_metadata: bool = True,
        decode_video: bool = False,
        fps: Optional[int] = None,
        resize: Optional[Tuple[int, int]] = None,
        **file_based_datasource_kwargs,
    ):
        """Initialize MCAP datasource.

        Args:
            paths: Path or list of paths to MCAP files.
            topics: Optional list/set of topic names to include. If specified,
                only messages from these topics will be read.
            time_range: Optional TimeRange for filtering messages by timestamp.
                TimeRange contains start_time and end_time in nanoseconds, where
                both values must be non-negative and start_time < end_time.
            message_types: Optional list/set of message type names (schema names)
                to include. Only messages with matching schema names will be read.
            include_metadata: Whether to include MCAP metadata fields in the output.
                Defaults to True. When True, includes schema, channel, and message
                metadata.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="mcap", package="mcap")

        # Convert to sets for faster lookup
        self._topics = set(topics) if topics else None
        self._message_types = set(message_types) if message_types else None
        self._time_range = time_range
        self._include_metadata = include_metadata
        self._decode_video = decode_video
        self._fps = fps
        self._resize = resize

        if decode_video:
            _check_import(self, module="av", package="av")
            _check_import(self, module="PIL", package="Pillow")
        if fps is not None and (not isinstance(fps, int) or fps <= 0):
            raise ValueError(f"Expected `fps` to be a positive integer, got {fps}.")
        if resize is not None and (
            len(resize) != 2 or any(not isinstance(v, int) or v <= 0 for v in resize)
        ):
            raise ValueError(
                f"Expected `resize` to be a (height, width) pair of positive "
                f"integers, got {resize}."
            )
        if (fps is not None or resize is not None) and not decode_video:
            raise ValueError("`fps` and `resize` only apply when `decode_video=True`.")

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read MCAP file and yield blocks of message data.

        This method implements efficient MCAP reading with predicate pushdown.
        It uses MCAP's built-in filtering capabilities for optimal performance
        and applies additional filters when needed.

        Args:
            f: File-like object to read from. Must be seekable for MCAP reading.
            path: Path to the MCAP file being processed.

        Yields:
            Block: Blocks of MCAP message data as pyarrow Tables.

        Raises:
            ValueError: If the MCAP file cannot be read or has invalid format.
        """
        from mcap.reader import make_reader

        reader = make_reader(f)
        if self._decode_video:
            yield from self._read_video_stream(reader, path)
            return

        # Note: MCAP summaries are optional and iter_messages works without them
        # We don't need to validate the summary since it's not required

        # Use MCAP's built-in filtering for topics and time range
        messages = reader.iter_messages(
            topics=list(self._topics) if self._topics else None,
            start_time=self._time_range.start_time if self._time_range else None,
            end_time=self._time_range.end_time if self._time_range else None,
            log_time_order=True,
            reverse=False,
        )

        builder = DelegatingBlockBuilder()

        for schema, channel, message in messages:
            # Apply filters that couldn't be pushed down to MCAP level
            if not self._should_include_message(schema, channel, message):
                continue

            # Convert message to dictionary format
            message_data = self._message_to_dict(schema, channel, message, path)
            builder.add(message_data)

        # Yield the block if we have any messages
        if builder.num_rows() > 0:
            yield builder.build()

    def _read_video_stream(self, reader: Any, path: str) -> Iterator[Block]:
        """Decode video topics inside the read task and yield frames as rows.

        `read_videos` establishes this contract for container formats: decoding
        happens where the decoder can hold state for a whole file, so the rows
        that reach a block are self-contained frames. MCAP needs the same, for
        the same reason -- an H.264 access unit is meaningless without the group
        of pictures it belongs to, and Ray Data splits rows into blocks at
        positions that have nothing to do with those groups.

        A `time_range` is deliberately *not* pushed down to the reader here. The
        first access unit at or after `start_time` is usually not a keyframe, so
        decoding has to begin at the last keyframe before it; those earlier
        frames are decoded as context and never emitted. The scan before the
        window costs I/O but no decoding.
        """
        start_time = self._time_range.start_time if self._time_range else None
        end_time = self._time_range.end_time if self._time_range else None

        decoders: Dict[int, _ChannelVideoDecoder] = {}
        builder = DelegatingBlockBuilder()

        for schema, channel, message in reader.iter_messages(
            topics=list(self._topics) if self._topics else None,
            start_time=None,
            end_time=end_time,
            log_time_order=True,
            reverse=False,
        ):
            if not self._should_include_message(schema, channel, message):
                continue

            decoder = decoders.get(message.channel_id)
            if decoder is None:
                decoder = _ChannelVideoDecoder(
                    kind=_extract_kind(
                        message.data,
                        channel.message_encoding,
                        schema.name if schema else "",
                    ),
                    schema=schema,
                    channel=channel,
                    fps=self._fps,
                    resize=self._resize,
                    start_time=start_time,
                )
                decoders[message.channel_id] = decoder

            for source, frame in decoder.feed(message):
                builder.add(
                    self._frame_to_dict(
                        decoder.schema, decoder.channel, source, frame, path
                    )
                )

        for decoder in decoders.values():
            for source, frame in decoder.flush():
                builder.add(
                    self._frame_to_dict(
                        decoder.schema, decoder.channel, source, frame, path
                    )
                )

        if builder.num_rows() > 0:
            yield builder.build()

    def _frame_to_dict(
        self,
        schema: "Schema",
        channel: "Channel",
        message: "Message",
        frame: Any,
        path: str,
    ) -> Dict[str, Any]:
        """Build a row for a decoded frame.

        The same columns a message row carries, with the undecoded `data`
        payload replaced by the decoded `frame`. Keeping both would double the
        memory for no benefit: `data` is unusable once decoded, and a decoded
        frame is orders of magnitude larger than the payload it came from.
        """
        row = self._message_to_dict(schema, channel, message, path)
        row.pop("data", None)
        row["frame"] = frame
        return row

    def _should_include_message(
        self, schema: "Schema", channel: "Channel", message: "Message"
    ) -> bool:
        """Check if a message should be included based on filters.

        This method applies Python-level filtering that cannot be pushed down
        to the MCAP library level. Topic filters are already handled by the
        MCAP reader, so only message_types filtering is needed here.

        Args:
            schema: MCAP schema object containing message type information.
            channel: MCAP channel object containing topic and metadata.
            message: MCAP message object containing the actual data.

        Returns:
            True if the message should be included, False otherwise.
        """
        # Message type filter (cannot be pushed down to MCAP reader)
        if self._message_types and schema and schema.name not in self._message_types:
            return False

        return True

    def _message_to_dict(
        self, schema: "Schema", channel: "Channel", message: "Message", path: str
    ) -> Dict[str, Any]:
        """Convert MCAP message to dictionary format.

        This method converts MCAP message objects into a standardized dictionary
        format suitable for Ray Data processing.

        Args:
            schema: MCAP schema object containing message type and encoding info.
            channel: MCAP channel object containing topic and channel metadata.
            message: MCAP message object containing the actual message data.
            path: Path to the source file (for include_paths functionality).

        Returns:
            Dictionary containing message data in Ray Data format.
        """
        # Decode message data based on encoding
        decoded_data = message.data
        if channel.message_encoding == "json" and isinstance(message.data, bytes):
            try:
                decoded_data = json.loads(message.data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Keep raw bytes if decoding fails
                decoded_data = message.data

        # Core message data
        message_data = {
            "data": decoded_data,
            "topic": channel.topic,
            "log_time": message.log_time,
            "publish_time": message.publish_time,
            "sequence": message.sequence,
        }

        # Add metadata if requested
        if self._include_metadata:
            message_data.update(
                {
                    "channel_id": message.channel_id,
                    "message_encoding": channel.message_encoding,
                    "schema_name": schema.name if schema else None,
                    "schema_encoding": schema.encoding if schema else None,
                    "schema_data": schema.data if schema else None,
                }
            )

        # Add file path if include_paths is enabled (from FileBasedDatasource)
        if getattr(self, "include_paths", False):
            message_data["path"] = path

        return message_data

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "MCAP"

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads.

        MCAP files can be read in parallel across multiple files.
        """
        return True


def _extract_kind(payload: bytes, message_encoding: str, schema_name: str) -> str:
    """Classify a video payload by its leading bytes.

    Sniffing beats trusting metadata here. MCAP is codec-agnostic, rigs log video
    under several schemas, and the schema is frequently absent or unhelpful, so
    the bytes are the only reliable signal.
    """
    if payload.startswith(_JPEG_SOI):
        return "jpeg"
    if payload.startswith(_PNG_SIGNATURE):
        return "png"
    if payload.startswith(_ANNEXB_START_4) or payload.startswith(_ANNEXB_START_3):
        return "h264_annexb"
    # A ROS 2 sensor_msgs/CompressedImage wraps the JPEG behind a header, so the
    # marker appears a little way in rather than at offset 0.
    if payload.find(_JPEG_SOI, 0, _EMBEDDED_JPEG_SEARCH_WINDOW) > 0:
        return "jpeg_embedded"
    raise ValueError(
        "Cannot decode this payload as video: "
        f"message_encoding={message_encoding!r}, schema={schema_name!r}, "
        f"first bytes {payload[:16].hex()}. `decode_video=True` only supports "
        "JPEG, PNG and H.264 Annex-B payloads. Pass `topics=[...]` to select "
        "only the video topics of this recording."
    )


def _annexb_nal_types(payload: bytes) -> List[int]:
    """Return the H.264 NAL unit types present in an Annex-B buffer."""
    types: List[int] = []
    index, size = 0, len(payload)
    while index < size - 3:
        if payload[index] == 0 and payload[index + 1] == 0:
            if payload[index + 2] == 1:
                header = index + 3
            elif (
                payload[index + 2] == 0 and index + 3 < size and payload[index + 3] == 1
            ):
                header = index + 4
            else:
                index += 1
                continue
            if header < size:
                types.append(payload[header] & 0x1F)
            index = header + 1
            continue
        index += 1
    return types


def _is_annexb_keyframe(payload: bytes) -> bool:
    """Whether this access unit can begin a decode.

    True when it carries an IDR slice or the parameter sets a decoder needs to
    interpret what follows. An H.264 stream that starts anywhere else cannot be
    decoded, which is why `_ChannelVideoDecoder` rewinds to one of these before
    a `time_range`.
    """
    return any(t in _ANNEXB_KEYFRAME_NAL_TYPES for t in _annexb_nal_types(payload))


class _ChannelVideoDecoder:
    """Decodes one channel's payloads into frames.

    One per channel, because topics interleave in log-time order: the decoder for
    a camera topic has to survive IMU messages arriving between two of its own
    frames, and inter-frame video cannot be decoded without that continuity.

    Holding the decoder here -- inside the read task -- is the point of the whole
    feature. Blocks then carry decoded frames, which are self-contained, so no
    downstream block boundary or re-batching can produce an undecodable unit.
    """

    def __init__(
        self,
        kind: str,
        schema: "Schema",
        channel: "Channel",
        fps: Optional[int],
        resize: Optional[Tuple[int, int]],
        start_time: Optional[int],
    ):
        self.kind = kind
        self.schema = schema
        self.channel = channel
        self._resize = resize
        self._start_time = start_time
        self._min_interval_ns = int(1e9 / fps) if fps else None
        self._last_emit_ns: Optional[int] = None
        self._codec = None
        # Access units since the last keyframe, held only until the window opens.
        self._gop: List["Message"] = []
        self._primed = False

    # -- public ------------------------------------------------------------

    def feed(self, message: "Message") -> Iterator[Tuple["Message", Any]]:
        """Yield ``(message, frame)`` for every frame this payload completes."""
        if self.kind in _STILL_IMAGE_KINDS:
            if self._before_window(message.log_time) or not self._due(message.log_time):
                return
            self._last_emit_ns = message.log_time
            yield message, self._decode_still(message.data)
            return

        if not self._primed:
            # Before the window opens, keep only the current group of pictures.
            # Those are exactly the packets the decoder needs to produce the
            # first frame inside the window, and no more.
            if _is_annexb_keyframe(message.data):
                self._gop = [message]
            else:
                self._gop.append(message)
            if self._start_time is not None and message.log_time < self._start_time:
                return
            self._primed = True
            backlog, self._gop = self._gop, []
            for buffered in backlog:
                yield from self._decode_packet(buffered)
            return

        yield from self._decode_packet(message)

    def flush(self) -> Iterator[Tuple["Message", Any]]:
        """Drain frames the decoder is still holding."""
        if self._codec is None:
            return
        for frame in self._codec.decode(None):
            yield from self._emit(self._last_packet, frame)

    # -- internals ---------------------------------------------------------

    def _before_window(self, log_time: int) -> bool:
        return self._start_time is not None and log_time < self._start_time

    def _due(self, log_time: int) -> bool:
        """Whether enough time has passed since the last emitted frame.

        `read_videos` strides by frame index, which assumes a constant frame
        rate. An MCAP topic has no such guarantee -- messages are timestamped and
        can be irregular -- so `fps` is honoured against log time instead.
        """
        if self._min_interval_ns is None or self._last_emit_ns is None:
            return True
        return log_time - self._last_emit_ns >= self._min_interval_ns

    def _decode_packet(self, message: "Message") -> Iterator[Tuple["Message", Any]]:
        import av

        if self._codec is None:
            self._codec = av.CodecContext.create("h264", "r")
        self._last_packet = message
        packet = av.packet.Packet(message.data)
        packet.pts = message.log_time
        try:
            frames = self._codec.decode(packet)
        except av.error.InvalidDataError:
            # A truncated or mid-GOP access unit. Skipping keeps the rest of the
            # topic readable; failing here would lose the whole file.
            logger.debug(
                f"Undecodable access unit on {self.channel.topic!r} at "
                f"log_time {message.log_time}.",
            )
            return
        for frame in frames:
            yield from self._emit(message, frame)

    def _emit(self, message: "Message", frame) -> Iterator[Tuple["Message", Any]]:
        log_time = message.log_time
        if self._before_window(log_time):
            # Decoded only as context for the first frame inside the window.
            return
        if not self._due(log_time):
            return
        self._last_emit_ns = log_time
        yield message, self._to_array(frame)

    def _to_array(self, frame):
        if self._resize is not None:
            height, width = self._resize
            # One libswscale pass does the colour conversion and the scale
            # together; converting first and scaling after costs more for the
            # same output.
            frame = frame.reformat(width=width, height=height, format="rgb24")
            return frame.to_ndarray()
        return frame.to_ndarray(format="rgb24")

    def _decode_still(self, payload: bytes):
        import io

        import numpy as np
        from PIL import Image

        if self.kind == "jpeg_embedded":
            payload = payload[payload.find(_JPEG_SOI) :]
        image = Image.open(io.BytesIO(payload)).convert("RGB")
        if self._resize is not None:
            height, width = self._resize
            if image.size != (width, height):
                # An already-coded still gives libswscale nothing to fuse with,
                # so this path stays on PIL.
                image = image.resize((width, height), Image.BILINEAR)
        return np.asarray(image)
