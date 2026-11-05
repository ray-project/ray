"""Read the Parquet fields that PyArrow's Python bindings don't expose.

A Parquet file has one footer, at its end, with metadata for every leaf of every
row group. A leaf is a column of primitive values: a flat column is one leaf, and
a struct column of two fields is two. :func:`read_footer_stats` reads each
leaf's ``SizeStatistics`` and data page count from the footer bytes
``FileMetaData`` already holds, and :func:`read_dictionary_page` reads a
dictionary page's header. Both run the compiled reader in ``_compact_reader.pyx``,
which decodes Thrift's compact protocol by hand, only the fields they need.

:func:`read_footer_stats` takes 0.35 s for 1,000 row groups of 1,000 leaves (Apple
M5, Python 3.12), 40% of it PyArrow re-serializing the footer. The result holds
about 185 bytes per row group per leaf; the footer bytes take up to twice the
footer's size in PyArrow's memory pool while it runs. A source tree Bazel hasn't
built, such as one ``setup-dev.py`` links in, lacks the reader, so both return
``None`` and sizing falls back.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, NamedTuple, Optional, Tuple, Union

try:
    # Absent from a source tree Bazel hasn't built, such as one setup-dev.py links in.
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
    """The raw Thrift footer behind a ``FileMetaData``, or ``None``.

    From ``__reduce__``, which PyArrow pickles it with: no I/O, but not public
    API either.
    """
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
    come out wrong.

    A reader that lost its place but kept the counts would return wrong numbers
    instead; the test against ``thriftpy2`` checks for that.
    """
    if _compact_reader is None:
        return None
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    try:
        row_groups, pages = _compact_reader.CompactReader(buf, LeafSizeStats).read()
    except Exception as exc:
        logger.error(f"Error reading footer stats: {exc}")
        # Sizing has a fallback, so a footer this can't decode must not fail the
        # read.
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
