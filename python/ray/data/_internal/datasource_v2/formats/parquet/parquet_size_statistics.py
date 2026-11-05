"""Read the Parquet fields that PyArrow's Python bindings don't expose.

:func:`read_footer_stats` reads each leaf's ``SizeStatistics`` and data page count
from the footer bytes ``FileMetaData`` already holds, and :func:`read_dictionary_page`
a dictionary page's header, with the Thrift compact-protocol reader compiled from
``_compact_reader.pyx``. A leaf is a column of primitive values: a struct column of
two fields is two. 1,000 row groups of 1,000 leaves take 0.35 s (Apple M5), 40% of
it PyArrow re-serializing the footer, and hold about 185 bytes per leaf.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, NamedTuple, Optional, Tuple, Union

try:
    # Absent from a source tree Bazel hasn't built, such as one setup-dev.py links
    # in; both readers then return ``None`` and sizing falls back.
    from ray.data._internal.datasource_v2.formats.parquet import _compact_reader
except ImportError:
    _compact_reader = None

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow.parquet import FileMetaData


class LeafSizeStats(NamedTuple):
    """A leaf's ``SizeStatistics`` in a row group: string bytes, level histograms."""

    unencoded_byte_array_data_bytes: Optional[int]
    repetition_level_histogram: Tuple[int, ...]
    definition_level_histogram: Tuple[int, ...]


class FooterStats(NamedTuple):
    """What :func:`read_footer_stats` reads, each by ``[row_group][leaf]``."""

    size_statistics: List[List[Optional[LeafSizeStats]]]
    # From ``encoding_stats``; ``None`` where it counts no data pages.
    data_pages: List[List[Optional[int]]]


class DictionaryPage(NamedTuple):
    """A dictionary page's header: its entries and its uncompressed bytes."""

    num_values: int
    uncompressed_page_size: int


def footer_bytes(metadata: "FileMetaData") -> Optional[memoryview]:
    """The raw Thrift footer behind a ``FileMetaData``, or ``None``: from
    ``__reduce__``, which PyArrow pickles it with, so no I/O but not public API."""
    try:
        payload = metadata.__reduce__()[1][0]
    except Exception as exc:
        logger.error(f"Error getting footer bytes: {exc}")
        return None
    try:
        # Unsigned: ``memoryview(pyarrow.Buffer)`` gives signed bytes.
        return memoryview(payload).cast("B")
    except (TypeError, ValueError) as exc:
        logger.error(f"Error casting footer bytes: {exc}")
        return None


def read_footer_stats(metadata: "FileMetaData") -> Optional[FooterStats]:
    """Each leaf's ``SizeStatistics`` and data page count, or ``None`` when the
    reader isn't built, the footer can't be read, or its row-group or leaf counts
    come out wrong."""
    if _compact_reader is None:
        return None
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    try:
        row_groups, pages = _compact_reader.CompactReader(buf, LeafSizeStats).read()
    except Exception as exc:
        logger.error(f"Error reading footer stats: {exc}")
        # Sizing falls back, so a footer this can't decode must not fail the read.
        return None

    if len(row_groups) != metadata.num_row_groups:
        return None
    num_columns = metadata.num_columns
    if any(len(columns) != num_columns for columns in row_groups):
        return None
    # The walk fills ``pages`` alongside, so its counts match.
    return FooterStats(row_groups, pages)


# Enough for a dictionary page's header, which takes about 35 bytes.
DICTIONARY_PAGE_HEADER_READ_BYTES = 64


def read_dictionary_page(buf: Union[bytes, memoryview]) -> Optional[DictionaryPage]:
    """The dictionary page header ``buf`` starts with, or ``None`` if it doesn't
    start with a whole one or the reader isn't built."""
    if _compact_reader is None:
        return None
    try:
        header = _compact_reader.CompactReader(buf).dictionary_page()
    except (IndexError, _compact_reader.ThriftDesync):
        return None
    return None if header is None else DictionaryPage(*header)
