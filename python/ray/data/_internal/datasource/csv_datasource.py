from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class CSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading and writing CSV files."""

    _FILE_EXTENSIONS = ["csv"]

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

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import pyarrow as pa
        from pyarrow import csv

        # Re-init invalid row handler: https://issues.apache.org/jira/browse/ARROW-17641
        if hasattr(self.parse_options, "invalid_row_handler"):
            self.parse_options.invalid_row_handler = (
                self.parse_options.invalid_row_handler
            )

        try:
            reader = csv.open_csv(
                f,
                read_options=self.read_options,
                parse_options=self.parse_options,
                **self.arrow_csv_args,
            )
            schema = None
            while True:
                try:
                    batch = reader.read_next_batch()
                    table = pa.Table.from_batches([batch], schema=schema)
                    if schema is None:
                        schema = table.schema
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
