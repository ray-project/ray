from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, Optional, Sequence

import pyarrow as pa

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class ColumnStatisticsState(Enum):
    """The mutually exclusive states of one file's column statistics.

    ``UNKNOWN`` is the only state in which the caller must read the file;
    the other three are exactly answerable from the footer.
    """

    UNKNOWN = "unknown"
    EMPTY = "empty"
    ALL_NULL = "all_null"
    BOUNDED = "bounded"


@DeveloperAPI
@dataclass(frozen=True)
class ColumnStatistics:
    """One file's footer view of one column.

    An exact result is in exactly one of these states:

    - ``EMPTY``: the file has no rows.
    - ``ALL_NULL``: every value is null, so there are no bounds.
    - ``BOUNDED``: at least one non-null value exists, so both bounds are set;
      ``null_count`` says how many nulls were mixed in.

    ``UNKNOWN`` means ``null_count``, ``min``, and ``max`` are unset, and the
    caller must read the file. The constructor rejects every field combination
    that contradicts the declared state, so a consumer can rely on these
    invariants instead of re-deriving them from individual fields.
    """

    num_rows: int
    null_count: Optional[int]
    min: Optional[Any]
    max: Optional[Any]
    state: ColumnStatisticsState

    def __post_init__(self) -> None:
        if not isinstance(self.state, ColumnStatisticsState):
            raise TypeError(
                f"state must be a ColumnStatisticsState, got {self.state!r}"
            )
        if self.num_rows < 0:
            raise ValueError(f"num_rows must be non-negative, got {self.num_rows}")

        if self.state is ColumnStatisticsState.UNKNOWN:
            if (
                self.null_count is not None
                or self.min is not None
                or self.max is not None
            ):
                raise ValueError(
                    "unknown statistics must not carry a null count or bounds"
                )
            return

        if self.null_count is None or not 0 <= self.null_count <= self.num_rows:
            raise ValueError(
                f"null_count {self.null_count} is outside [0, {self.num_rows}]"
            )

        if self.state is ColumnStatisticsState.EMPTY:
            if self.num_rows != 0 or self.min is not None or self.max is not None:
                raise ValueError("empty statistics require zero rows and no bounds")
        elif self.state is ColumnStatisticsState.ALL_NULL:
            if (
                self.num_rows == 0
                or self.null_count != self.num_rows
                or self.min is not None
                or self.max is not None
            ):
                raise ValueError(
                    "all-null statistics require rows, all values null, and no bounds"
                )
        elif (
            self.num_rows == 0
            or self.null_count >= self.num_rows
            or self.min is None
            or self.max is None
        ):
            raise ValueError(
                "bounded statistics require rows, a non-null value, and both bounds"
            )

    @classmethod
    def unknown(cls, num_rows: int) -> "ColumnStatistics":
        """Statistics the footer cannot settle; the caller must read the file."""
        return cls(
            num_rows=num_rows,
            null_count=None,
            min=None,
            max=None,
            state=ColumnStatisticsState.UNKNOWN,
        )


@DeveloperAPI
class SupportsColumnStatistics(ABC):
    """A reader that can answer per-column min/max from file metadata.

    Implementations must yield exactly one mapping per manifest row, in
    manifest order, containing every requested column. Each mapping's
    ``num_rows`` is that row's file row count.

    A manifest row is assumed to be a whole file. Chunked manifests are not
    supported: statistics are file-level, so a chunk row would answer for the
    whole file rather than for the chunk.

    ``state=UNKNOWN`` means "read this row"; it is never safe to infer a bound
    from an unknown result. All requested columns share one metadata read per
    file, since reading a footer is the expensive part of this API.
    """

    @abstractmethod
    def supports_column_type(self, column_type: pa.DataType) -> bool:
        """Whether footer statistics can exactly bound a column of this type.

        A planning-time hint only. ``False`` must leave the read plan untouched
        so the column keeps the normal read path; ``True`` does not guarantee
        that a particular file has statistics, which is the per-file
        ``state=UNKNOWN`` answer instead.
        """

    @abstractmethod
    def read_column_statistics(
        self, file_manifest: FileManifest, columns: Sequence[str]
    ) -> Iterator[Dict[str, ColumnStatistics]]:
        """Yield per-file statistics for ``columns``, in manifest order."""


__all__ = [
    "ColumnStatistics",
    "ColumnStatisticsState",
    "SupportsColumnStatistics",
]
