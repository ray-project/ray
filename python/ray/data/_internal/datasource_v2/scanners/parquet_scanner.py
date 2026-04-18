import logging
from dataclasses import dataclass
from typing import Callable, Optional

import pyarrow as pa

from ray.data._internal.datasource.parquet_datasource import (
    check_for_legacy_tensor_type,
)
from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data.block import Block
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
@dataclass(frozen=True)
class ParquetScanner(ArrowFileScanner):
    """Parquet-specific scanner implementation.

    Inherits filter pushdown, column pruning, limit pushdown, partition
    pruning, and file shuffle from ArrowFileScanner. Adds Parquet-specific
    reader creation with adaptive batch sizing and the Parquet-only
    legacy-tensor-type schema check.
    """

    target_block_size: Optional[int] = None
    include_paths: bool = False
    _block_udf: Optional[Callable[[Block], Block]] = None

    def read_schema(self) -> pa.Schema:
        """Return schema after column pruning, UDF inference, and tensor check.

        The UDF is invoked on an empty table to derive the output schema; any
        exception is swallowed with a debug log so schema inference doesn't
        crash on a misbehaving UDF.
        """
        schema = super().read_schema()
        if self.include_paths and schema.get_field_index("path") == -1:
            schema = schema.append(pa.field("path", pa.string()))

        if self._block_udf is not None:
            try:
                udf_schema = self._block_udf(schema.empty_table()).schema
                schema = udf_schema.with_metadata(schema.metadata)
            except Exception:
                logger.debug(
                    "Failed to infer schema by running block UDF on empty table.",
                    exc_info=True,
                )

        check_for_legacy_tensor_type(schema)
        return schema

    def create_reader(self) -> ParquetFileReader:
        """Create a ParquetFileReader configured for this scanner.

        Returns:
            ParquetFileReader with all pushdowns and adaptive batch sizing.
        """
        return ParquetFileReader(
            batch_size=self.batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning.to_pyarrow() if self.partitioning else None,
            ignore_prefixes=self.ignore_prefixes,
            target_block_size=self.target_block_size,
            include_paths=self.include_paths,
            _block_udf=self._block_udf,
        )
