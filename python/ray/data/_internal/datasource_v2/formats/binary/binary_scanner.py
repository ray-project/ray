"""The binary ``Scanner``: the read's options plus the optimizer's pushdowns."""

from dataclasses import dataclass

from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_stream_scanner import (
    FileStreamScanner,
)
from ray.data._internal.datasource_v2.formats.binary.binary_reader import BinaryReader
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class BinaryScanner(FileStreamScanner):
    """Scanner for binary files on DataSource V2.

    Supports column pruning, a per-task row limit and partition pruning, and
    answers ``count()`` from the listing. Filter pushdown is not claimed.
    """

    @override
    def metadata_row_count_is_exact(self) -> bool:
        """Every file is one row, so the listing counts rows exactly unless a
        pushdown drops some."""
        return self.limit is None and self.partition_predicate is None

    @override
    def create_reader(self) -> BinaryReader:
        return BinaryReader(**self._reader_kwargs())
