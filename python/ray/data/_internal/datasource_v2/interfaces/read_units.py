"""Read units: sub-file units of work.

A read unit is a piece of one file that a reader scans on its own and can
name, so anything downstream can refer to that piece again. A Parquet row
group is one example; a reader that scans a file in a single pass reports
the file itself as the unit.

Three small types share the word "unit"; each answers a different question:

- :class:`ReadUnit` -- *what* is being read: a stable id, the file, and the
  unit's place in that file. This is the object a checkpoint names.
- :class:`ReadUnitFragment` -- *how* a file reader scans one unit: the
  pyarrow fragment covering exactly that unit, plus ``unit_start_row``, the
  row index in the file where the unit begins. Internal to file readers.
- :class:`~ray.data._internal.datasource_v2.interfaces.synthesized_columns.ReadUnitPosition`
  -- *where* one table of yielded rows sits: the unit, the same
  ``unit_start_row``, and ``rows_before``, a cursor counting the rows the
  reader already produced from that unit. The reader attaches one to every
  table, and it is the input to a synthesized column.

Two row counts appear in these types and mean different things.
``unit_start_row`` is a pre-filter index into the file (row groups have fixed
row counts, so it comes from the footer). ``rows_before`` is a post-filter
cursor (it counts rows that survived a pushed-down filter, in the order they
came out). ``row_hash`` adds the two to place a row within its file.

A checkpoint that records the ids of the units it finished can hand them back
through ``TaskContext.kwargs[EXCLUDED_READ_UNIT_IDS_KWARG_NAME]`` on the
``ListFiles`` operator; the listing task passes them to
``FileIndexer.list_files(excluded_read_unit_ids=...)`` so those units never
reach the partitioner or a read task. Nothing in this module performs
checkpointing.
"""

from dataclasses import dataclass
from typing import Optional

import pyarrow.dataset as pds

from ray.util.annotations import DeveloperAPI

# ``TaskContext.kwargs`` key for the set of :attr:`ReadUnit.id` strings a
# resumed job must not read again. Set with ``MapOperator.create(
# map_task_kwargs=...)`` on the ``ListFiles`` operator.
EXCLUDED_READ_UNIT_IDS_KWARG_NAME = "excluded_read_unit_ids"


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


@DeveloperAPI
@dataclass(frozen=True)
class ReadUnitFragment:
    """How a file reader scans one read unit: the pyarrow fragment covering
    exactly that unit, and where the unit starts in its file.

    Attributes:
        fragment: The pyarrow dataset fragment to scan. For a row-group unit
            this is ``ParquetFileFragment.subset(row_group_ids=[i])``; for a
            whole-file unit it is the file's fragment.
        unit: The read unit the fragment covers.
        unit_start_row: Pre-filter index in the file of the unit's first row
            (the summed row counts of the row groups before it, from the
            footer); ``0`` for a whole-file unit. Copied onto every
            :class:`~ray.data._internal.datasource_v2.interfaces.synthesized_columns.ReadUnitPosition`
            the reader yields for this unit.
    """

    fragment: pds.Fragment
    unit: ReadUnit
    unit_start_row: int = 0
