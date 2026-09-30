from typing import List, Optional, Sequence

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
    FileReader,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches; this reader does not provide ORC-native stripe pruning.
    """

    def __init__(
        self,
        batch_size: int = _ARROW_DEFAULT_BATCH_SIZE,
        columns: Optional[List[str]] = None,
        predicate: Optional[Expr] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        ignore_prefixes: Optional[List[str]] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
        schema: Optional[pa.Schema] = None,
    ):
        """Configure a whole-file ORC scan.

        Args:
            batch_size: Maximum number of output rows per Arrow batch.
            columns: Columns to read, or None for all columns.
            predicate: Row filter applied by the Arrow scanner.
            limit: Maximum number of output rows.
            filesystem: Filesystem used to open ORC files.
            partitioning: Optional path partitioning specification.
            ignore_prefixes: File prefixes to ignore.
            synthesized_columns: Columns appended after scanning each batch.
            schema: Logical schema used to align file fragments.
        """
        super().__init__(
            format=FileFormat.ORC,
            batch_size=batch_size,
            columns=columns,
            predicate=predicate,
            limit=limit,
            filesystem=filesystem,
            partitioning=partitioning,
            ignore_prefixes=ignore_prefixes,
            synthesized_columns=synthesized_columns,
            schema=schema,
        )

    @override
    def _make_format(self) -> pds.OrcFileFormat:
        return pds.OrcFileFormat()

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
