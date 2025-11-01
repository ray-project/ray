from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class CSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading and writing CSV files."""

    _FILE_EXTENSIONS = [
        "csv",
        "csv.gz",  # gzip-compressed files
        "csv.br",  # Brotli-compressed files
        "csv.zst",  # Zstandard-compressed files
        "csv.lz4",  # lz4-compressed files
    ]

    def __init__(
        self,
        paths: Union[str, List[str]],
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        from pyarrow import csv

        super().__init__(paths, **file_based_datasource_kwargs)

        if arrow_csv_args is None:
            arrow_csv_args = {}

        self.read_options = arrow_csv_args.pop(
            "read_options", csv.ReadOptions(use_threads=False)
        )
        self.parse_options = arrow_csv_args.pop("parse_options", csv.ParseOptions())
        self.arrow_csv_args = arrow_csv_args

        # Initialize projection pushdown attributes
        self._data_columns = None
        self._data_columns_rename_map = None

    def supports_predicate_pushdown(self) -> bool:
        return True

    def supports_projection_pushdown(self) -> bool:
        return True

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import pyarrow as pa
        from pyarrow import csv

        from ray.data.datasource.datasource import _DatasourceProjectionPushdownMixin

        # Re-init invalid row handler: https://issues.apache.org/jira/browse/ARROW-17641
        if hasattr(self.parse_options, "invalid_row_handler"):
            self.parse_options.invalid_row_handler = (
                self.parse_options.invalid_row_handler
            )

        filter_expr = (
            self._predicate_expr.to_pyarrow()
            if self._predicate_expr is not None
            else None
        )

        # Set up column selection if projection pushdown is active
        csv_args = dict(self.arrow_csv_args)
        if self._data_columns is not None:
            # Apply projection: only include specified columns
            convert_options = csv_args.get("convert_options", csv.ConvertOptions())
            convert_options.include_columns = self._data_columns
            csv_args["convert_options"] = convert_options

        try:
            reader = csv.open_csv(
                f,
                read_options=self.read_options,
                parse_options=self.parse_options,
                **csv_args,
            )
            schema = None
            while True:
                try:
                    batch = reader.read_next_batch()
                    table = pa.Table.from_batches([batch], schema=schema)
                    if schema is None:
                        schema = table.schema
                    if filter_expr is not None:
                        table = table.filter(filter_expr)

                    # Apply column renames using shared helper
                    table = _DatasourceProjectionPushdownMixin._apply_rename(
                        table, self._data_columns_rename_map
                    )

                    yield table
                except StopIteration:
                    return
        except pa.lib.ArrowInvalid as e:
            raise ValueError(
                f"Failed to read CSV file: {path}. "
                "Please check the CSV file has correct format, or filter out non-CSV "
                "file with 'partition_filter' field. See read_csv() documentation for "
                "more details."
            ) from e
