"""Optional, metadata-only PyORC access and bounded bootstrap byte reuse."""

import importlib
import io
import logging
import operator
from dataclasses import dataclass
from typing import Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.util import call_with_retry
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrcStripeMetadata:
    index: int
    num_rows: int
    row_offset: int
    size_bytes: int
    bytes_offset: int


@dataclass(frozen=True)
class OrcFileMetadata:
    num_rows: int
    stripes: Tuple[OrcStripeMetadata, ...]


@dataclass(frozen=True)
class OrcLayoutSeed:
    path: str
    size: int
    layout: OrcFileMetadata


def pyorc_available() -> bool:
    """Only an absent optional package permits whole-file fallback."""
    try:
        importlib.import_module("pyorc")
    except ModuleNotFoundError as error:
        if error.name == "pyorc":
            return False
        raise
    return True


def orc_read_unit_id(path: str, stripe_index: int) -> str:
    return f"{path}#stripe{stripe_index}"


class _RangeCache:
    """Read-through immutable ranges, scoped to one owned input file.

    Borrowed views have independent cursors and never close the owned source.
    Partial hits read only uncovered gaps. Failed/short reads are not cached.
    The cap bounds retained bytes, not native allocations or large read-throughs.
    """

    def __init__(self, source, capacity: int = 8 * 1024 * 1024):
        self.source = source
        self.size = source.size()
        self.capacity = capacity
        self.retained_bytes = 0
        self.uncached_bytes = 0
        self.ranges = []

    def view(self):
        return _CachedInput(self)

    def read(self, start: int, length: int) -> bytes:
        end = min(self.size, start + length)
        if start >= end:
            return b""
        parts = []
        cursor = start
        for offset, data in list(self.ranges):
            if offset + len(data) <= cursor:
                continue
            if offset >= end:
                break
            if cursor < offset:
                parts.append(self._read_gap(cursor, offset))
                cursor = offset
            stop = min(end, offset + len(data))
            parts.append(data[cursor - offset : stop - offset])
            cursor = stop
            if cursor == end:
                break
        if cursor < end:
            parts.append(self._read_gap(cursor, end))
        return b"".join(parts)

    def _read_gap(self, start: int, end: int) -> bytes:
        self.source.seek(start)
        data = self.source.read(end - start)
        if len(data) != end - start:
            raise OSError(
                f"Unexpected short ORC read at {start}: {len(data)}/{end-start}"
            )
        if self.retained_bytes + len(data) <= self.capacity:
            self.ranges.append((start, data))
            self.ranges.sort(key=lambda item: item[0])
            self.retained_bytes += len(data)
        else:
            self.uncached_bytes += len(data)
        return data


class _CachedInput(io.RawIOBase):
    def __init__(self, cache: _RangeCache):
        super().__init__()
        self.cache = cache
        self.position = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        self._checkClosed()
        return self.position

    def seek(self, offset, whence=0):
        self._checkClosed()
        offset = operator.index(offset)
        whence = operator.index(whence)
        if whence not in (0, 1, 2):
            raise ValueError(f"Invalid whence: {whence}")
        position = offset + (
            0 if whence == 0 else self.position if whence == 1 else self.cache.size
        )
        if position < 0:
            raise ValueError("Negative ORC seek")
        self.position = position
        return position

    def read(self, size=-1):
        self._checkClosed()
        if size is None or size < 0:
            size = max(0, self.cache.size - self.position)
        size = operator.index(size)
        data = self.cache.read(self.position, size)
        self.position += len(data)
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def _layout_from_source(source, path: str, size: int) -> OrcFileMetadata:
    backend = importlib.import_module("pyorc")
    try:
        # Stripe construction allocates a native row batch even without next().
        # Keep it small; it does not eliminate libORC's O(S²) initialization.
        reader = backend.Reader(source, batch_size=1)
        stripes = tuple(
            OrcStripeMetadata(
                i,
                len(stripe),
                stripe.row_offset,
                stripe.bytes_length,
                stripe.bytes_offset,
            )
            for i, stripe in enumerate(reader.iter_stripes())
        )
        row = 0
        byte_end = 0
        for stripe in stripes:
            if (
                stripe.num_rows < 0
                or stripe.row_offset != row
                or stripe.size_bytes < 0
                or stripe.bytes_offset < byte_end
                or stripe.bytes_offset + stripe.size_bytes > size
            ):
                raise ValueError(f"Invalid stripe {stripe.index} layout")
            row += stripe.num_rows
            byte_end = stripe.bytes_offset + stripe.size_bytes
        if row != len(reader):
            raise ValueError(f"Stripe rows {row} != file rows {len(reader)}")
        return OrcFileMetadata(len(reader), stripes)
    except (RuntimeError, ValueError, backend.ORCError) as error:
        raise pa.ArrowInvalid(
            f"Failed to read ORC metadata for {path}: {error}"
        ) from error


def read_orc_metadata(path: str, filesystem: Optional[FileSystem]) -> OrcFileMetadata:
    """Read a checked physical layout without column statistics or row iteration."""
    filesystem = filesystem or LocalFileSystem()

    def read():
        try:
            with filesystem.open_input_file(path) as source:
                return _layout_from_source(source, path, source.size())
        except OSError as error:
            raise type(error)(
                f"Failed to read ORC metadata for {path}: {error}"
            ) from error

    return call_with_retry(
        read,
        description=f"read ORC metadata for {path}",
        match=DataContext.get_current().retried_io_errors,
    )


def read_orc_schema_and_layout(path: str, filesystem: Optional[FileSystem]):
    """Share bounded raw footer ranges between Arrow schema and PyORC layout."""
    import pyarrow.orc as orc

    filesystem = filesystem or LocalFileSystem()

    def read():
        try:
            with filesystem.open_input_file(path) as source:
                cache = _RangeCache(source)
                with cache.view() as arrow_input:
                    schema = orc.ORCFile(arrow_input).schema
                with cache.view() as layout_input:
                    layout = _layout_from_source(layout_input, path, cache.size)
                if cache.uncached_bytes:
                    logger.debug(
                        "ORC bootstrap range cache capacity exceeded for %s: "
                        "%s bytes read through without retention",
                        path,
                        cache.uncached_bytes,
                    )
                return schema, OrcLayoutSeed(path, cache.size, layout)
        except (pa.ArrowInvalid, OSError) as error:
            raise type(error)(f"Failed to prepare ORC file {path}: {error}") from error

    return call_with_retry(
        read,
        description=f"prepare ORC file {path}",
        match=DataContext.get_current().retried_io_errors,
    )
