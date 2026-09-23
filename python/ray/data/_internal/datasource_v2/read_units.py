"""Read units: sub-file units of work.

A read unit is a piece of one file that a reader scans on its own and can
name, so anything downstream can refer to that piece again. A Parquet row
group is one example; a reader that scans a file in a single pass reports
the file itself as the unit. The reader reports the unit of every table it
yields through
:class:`~ray.data._internal.datasource_v2.readers.synthesized_columns.ReadUnitPosition`,
so a synthesized column can position every row within its file.
"""

from dataclasses import dataclass
from typing import Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class ReadUnit:
    """The smallest piece of an input split a reader scans on its own.

    Attributes:
        id: Stable name for the piece. The same row group gets the same
            ``id`` on every run, however listing grouped files into tasks.
            Compared by equality only; nothing parses it.
        source: The file path the unit belongs to.
        index: Position of the unit within ``source`` (the row group index).
        count: Number of units in ``source``, when the manifest knows it.
        num_rows: Rows in the unit, when the manifest knows it.
    """

    id: str
    source: str
    index: int = 0
    count: Optional[int] = None
    num_rows: Optional[int] = None
