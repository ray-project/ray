"""One row group's footer counts per leaf."""

from __future__ import annotations

import math
from typing import Iterable, Sequence

from pyarrow.parquet import ColumnChunkMetaData, RowGroupMetaData

from ray.data._internal.datasource_v2.formats.parquet.decoded_size.arrow_layout import (
    _BINARY,
    _NULL,
    _bytes_for_bits,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.schema_walk import (
    LeafProfile,
    _ArrowNode,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
)


class LeafChunk:
    """One leaf's chunk in a row group, with its counts from the footer."""

    def __init__(
        self,
        index: int,
        profile: LeafProfile,
        chunk: ColumnChunkMetaData,
        stats: LeafSizeStats | None,
        num_rows: int,
    ):
        self.index = index
        self.profile = profile
        self.chunk = chunk
        self.num_rows = num_rows
        self._max_def = profile.max_definition_level
        self._max_rep = profile.max_repetition_level
        # Below a struct, list or map.
        self._nested = len(profile.nodes) > 1
        # SizeStatistics' string bytes, unless the writer undercounts them.
        self.size_statistics_bytes = (
            None if stats is None else stats.unencoded_byte_array_data_bytes
        )
        if (
            self.size_statistics_bytes is not None
            and not profile.counts_delta_byte_array
            and detect_delta_byte_array(chunk)
        ):
            self.size_statistics_bytes = None
        # Histograms that disagree with the footer mean the walk drifted or the
        # writer miscounted (PyArrow with a write_batch_size above the rows); the
        # string bytes from the same statistics go with them.
        if stats is not None and not self._histograms_agree(stats=stats):
            stats = None
            self.size_statistics_bytes = None
        self._repetition = () if stats is None else stats.repetition_level_histogram
        self._definition = () if stats is None else stats.definition_level_histogram
        # Entries below the max definition level, read only for a flat leaf without
        # a definition histogram: building ``Statistics`` costs two thirds of an
        # estimate, and a nested leaf can't use it.
        self._null_count = None
        if self._max_def and not self._definition and not self._nested:
            statistics = chunk.statistics
            if (
                statistics is not None
                and statistics.has_null_count
                and 0 <= statistics.null_count <= chunk.num_values
            ):
                self._null_count = statistics.null_count
        # The values' bytes without their length prefixes: SizeStatistics', or from
        # the footer a guess for a string chunk with a dictionary page and a bound
        # for one without.
        self.string_bytes = self.size_statistics_bytes
        if self.string_bytes is None and profile.nodes[-1].kind == _BINARY:
            self.string_bytes = (
                self._dictionary_string_bytes()
                if chunk.has_dictionary_page
                else self._plain_string_bytes()
            )

    def modelled(self) -> bool:
        """Whether every count is exact; a string leaf needs its values' bytes."""
        strings_known = (
            self.profile.nodes[-1].kind != _BINARY or self.string_bytes is not None
        )
        return strings_known and self.levels_known()

    def _plain_string_bytes(self) -> int | None:
        """At least the values' bytes of a string chunk without a dictionary page,
        from its footer; ``None`` for any other chunk."""
        chunk = self.chunk
        if not set(chunk.encodings) <= _PLAIN_STRING_ENCODINGS:
            return None
        if not self.levels_known():
            return None
        # PLAIN stores each value as a 4-byte length and its bytes; the levels and
        # page headers also in the uncompressed size only raise the bound.
        bound = chunk.total_uncompressed_size
        if "DELTA_LENGTH_BYTE_ARRAY" not in chunk.encodings:
            bound -= 4 * (chunk.num_values - self._below(self._max_def))
        return max(bound, 0)

    def _dictionary_string_bytes(self) -> int | None:
        """A guess at the values' bytes of a string chunk with a dictionary page,
        from its footer: low when its min and max are shorter than most values."""
        chunk = self.chunk
        if not self.levels_known():
            return None
        values = chunk.num_values - self._below(self._max_def)
        if values <= 0:
            return 0
        statistics = chunk.statistics
        if statistics is None or not statistics.has_min_max:
            return None
        longest = max(len(statistics.min_raw), len(statistics.max_raw))
        # The uncompressed bytes hold most values' bytes once the dictionary outgrows
        # its page and the rest are written PLAIN, or when every value is distinct.
        return max(
            math.ceil(_MIN_MAX_LENGTH_FACTOR * longest * values),
            chunk.total_uncompressed_size,
        )

    def levels_known(self) -> bool:
        """Whether every array length and null count is exact."""
        repetition = bool(self._repetition) or self._max_rep <= 1
        definition = (
            bool(self._definition)
            or self._max_def == 0
            or (not self._nested and self._null_count is not None)
        )
        return repetition and definition

    def array_length(self, node: _ArrowNode | None = None) -> int:
        """An array's length, the leaf's by default."""
        node = self.profile.nodes[-1] if node is None else node
        if node.length_rep_level == 0:
            return self.num_rows * node.scale
        opened = self._opened(node.length_rep_level) - self._below(
            node.length_def_level
        )
        # Histograms that contradict each other can make this negative.
        return max(opened, 0) * node.scale

    def bitmap(self, node: _ArrowNode | None = None) -> int:
        """Bytes of an array's validity bitmap, the leaf's by default."""
        node = self.profile.nodes[-1] if node is None else node
        if not node.nullable or node.kind == _NULL:
            return 0
        if (
            not node.keeps_bitmap
            and self._nulls(
                length_def_level=node.length_def_level, def_level=node.def_level
            )
            == 0
        ):
            return 0
        return _bytes_for_bits(self.array_length(node))

    def _histograms_agree(self, stats: LeafSizeStats) -> bool:
        """Whether the writer's histograms fit the footer's own counts."""
        repetition = stats.repetition_level_histogram
        definition = stats.definition_level_histogram
        if repetition and (
            len(repetition) != self._max_rep + 1
            or repetition[0] != self.num_rows
            or sum(repetition) != self.chunk.num_values
        ):
            return False
        return not definition or (
            len(definition) == self._max_def + 1
            and sum(definition) == self.chunk.num_values
        )

    def _opened(self, depth: int) -> int:
        """Entries at repetition level ``depth`` or less, or every entry."""
        if self._repetition:
            return sum(self._repetition[: depth + 1])
        return self.chunk.num_values

    def _below(self, def_level: int) -> int:
        """Entries under definition level ``def_level``."""
        if self._definition:
            return sum(self._definition[:def_level])
        # A flat leaf, whose one level below the max the null count holds.
        return 0 if def_level <= 0 else self._null_count or 0

    def _nulls(self, length_def_level: int, def_level: int) -> int:
        """Elements that exist but are null (level in
        ``[length_def_level, def_level)``)."""
        return self._below(def_level) - self._below(length_def_level)


def leaf_chunks(
    row_group: RowGroupMetaData,
    leaf_profiles: Sequence[LeafProfile | None],
    leaf_indices: Iterable[int],
    size_stats: Sequence[LeafSizeStats | None] | None,
) -> dict[int, LeafChunk]:
    """A :class:`LeafChunk` by leaf index for each of ``leaf_indices`` in
    ``row_group`` the estimator models: a missing index means that leaf needs the
    caller's fallback."""
    stats = size_stats or ()
    chunks = (
        LeafChunk(
            index=leaf_idx,
            profile=profile,
            chunk=row_group.column(leaf_idx),
            stats=stats[leaf_idx] if leaf_idx < len(stats) else None,
            num_rows=row_group.num_rows,
        )
        for leaf_idx in leaf_indices
        if (profile := leaf_profiles[leaf_idx]) is not None
    )
    return {leaf.index: leaf for leaf in chunks if leaf.modelled()}


# A dictionary-encoded string chunk's values are guessed at this many times the
# longer of its min and max: on TPC-H and synthetic web logs that alone ran from
# 0.94 to 1.25 times the true bytes.
_MIN_MAX_LENGTH_FACTOR = 1.1

# A string chunk's encodings when its footer bounds its values' bytes: no
# dictionary or shared prefixes, and RLE or BIT_PACKED only for its levels.
_PLAIN_STRING_ENCODINGS = frozenset(
    {"PLAIN", "DELTA_LENGTH_BYTE_ARRAY", "RLE", "BIT_PACKED"}
)


# Any DELTA_BYTE_ARRAY page.
def detect_delta_byte_array(chunk: ColumnChunkMetaData) -> bool:
    return "DELTA_BYTE_ARRAY" in chunk.encodings
