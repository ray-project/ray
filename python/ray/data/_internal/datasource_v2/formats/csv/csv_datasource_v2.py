"""Concrete ``DataSourceV2`` for CSV files."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, List, Optional, Tuple

import pyarrow as pa
import pyarrow.csv as pcsv
from typing_extensions import override

from ray.data._internal.datasource_v2.common.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
)
from ray.data._internal.datasource_v2.common.line_chunkable_datasource_v2 import (
    LineChunkableDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.csv.csv_file_reader import (
    CsvFileReader,
    csv_file_format,
    invalid_csv_error,
    read_snappy_buffer,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import infer_compression
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

# ``read_csv`` keyword arguments this datasource takes; any other goes to V1.
CSV_V2_ARGS = frozenset({"read_options", "parse_options", "convert_options"})


@DeveloperAPI
@dataclass(frozen=True)
class CsvScanner(ArrowFileScanner):
    """Configure CSV reads through :class:`CsvFileReader`."""

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    partition_columns_in_file: FrozenSet[str] = frozenset()
    chunk_size: int = 0
    read_options: Optional[pcsv.ReadOptions] = None
    parse_options: Optional[pcsv.ParseOptions] = None
    convert_options: Optional[pcsv.ConvertOptions] = None

    @override
    def push_filters(
        self, predicate: Expr
    ) -> Tuple["ArrowFileScanner", Optional[Expr]]:
        """Keep filters above the read, as V1 ``read_csv`` does.

        Column types are inferred per chunk, so a filter typed against the
        sampled schema may not bind to every chunk.
        """
        return self, predicate

    @override
    def create_reader(self) -> CsvFileReader:
        return CsvFileReader(
            read_options=self.read_options,
            parse_options=self.parse_options,
            convert_options=self.convert_options,
            partition_columns_in_file=self.partition_columns_in_file,
            chunk_size=self.chunk_size,
            batch_size=self.batch_size or _ARROW_DEFAULT_BATCH_SIZE,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )


@DeveloperAPI
class CsvDatasourceV2(LineChunkableDatasourceV2):
    """V2 datasource that lists CSV files and reads them with ``CsvScanner``.

    Splits a large uncompressed file into byte-range chunks unless the parse
    options let a row span lines (``newlines_in_values``), skip rows at the
    start of each file, or use a non-UTF-8 encoding.
    """

    _FORMAT_NAME = "CSV"

    def __init__(
        self,
        paths: List[str],
        *,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        **line_chunkable_kwargs: Any,
    ):
        super().__init__(paths, name="CsvV2", **line_chunkable_kwargs)
        arrow_csv_args = dict(arrow_csv_args or {})
        assert set(arrow_csv_args) <= CSV_V2_ARGS, sorted(arrow_csv_args)
        self._read_options: pcsv.ReadOptions = arrow_csv_args.get(
            "read_options"
        ) or pcsv.ReadOptions(use_threads=False)
        self._parse_options: pcsv.ParseOptions = (
            arrow_csv_args.get("parse_options") or pcsv.ParseOptions()
        )
        self._convert_options: pcsv.ConvertOptions = (
            arrow_csv_args.get("convert_options") or pcsv.ConvertOptions()
        )

    @override
    def _can_split_files(self) -> bool:
        encoding = self._read_options.encoding.lower().replace("-", "")
        return (
            not self._parse_options.newlines_in_values
            and self._read_options.skip_rows == 0
            and self._read_options.skip_rows_after_names == 0
            and encoding == "utf8"
        )

    @override
    def _read_file_schema(self, path: str) -> pa.Schema:
        file_format = csv_file_format(
            self._read_options, self._parse_options, self._convert_options
        )
        try:
            if infer_compression(path) == "snappy":
                return file_format.inspect(read_snappy_buffer(path, self._filesystem))
            return file_format.inspect(path, filesystem=self._filesystem)
        except pa.ArrowInvalid as e:
            raise invalid_csv_error(path) from e

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options,
    ) -> CsvScanner:
        return CsvScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
            partition_columns_in_file=self._partition_columns_in_file,
            chunk_size=self._chunk_size,
            read_options=self._read_options,
            parse_options=self._parse_options,
            convert_options=self._convert_options,
        )
