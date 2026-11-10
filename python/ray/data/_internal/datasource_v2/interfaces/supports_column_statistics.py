from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Sequence

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class ColumnStatistics:
    """One file's footer view of one column.

    ``min`` and ``max`` cover the file's non-null values. ``null_count`` is
    exact when set; ``exact`` is false when the footer cannot settle the
    column, in which case the caller must read this file.
    """

    num_rows: int
    null_count: Optional[int]
    min: Optional[Any]
    max: Optional[Any]
    exact: bool

    @classmethod
    def unknown(cls, num_rows: int) -> "ColumnStatistics":
        return cls(
            num_rows=num_rows,
            null_count=None,
            min=None,
            max=None,
            exact=False,
        )


@DeveloperAPI
class SupportsColumnStatistics(ABC):
    """A reader that can answer per-column min/max from file metadata.

    One mapping is yielded per manifest row, in manifest order, keyed by the
    requested columns. ``exact=False`` means "read this row"; it is never safe
    to infer a bound from an unknown result.
    """

    @abstractmethod
    def read_column_statistics(
        self, file_manifest: FileManifest, columns: Sequence[str]
    ) -> Iterator[Dict[str, ColumnStatistics]]:
        """Yield per-file statistics for ``columns``, in manifest order.

        All requested columns share one metadata read per file, since reading
        a footer is the expensive part of this API.
        """


__all__ = ["ColumnStatistics", "SupportsColumnStatistics"]
