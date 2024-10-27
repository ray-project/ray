from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from .native_file_reader import NativeFileReader
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


class CSVReader(NativeFileReader):
    def __init__(
        self,
        *,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        **file_reader_kwargs,
    ):
        from pyarrow import csv

        super().__init__(**file_reader_kwargs)

        if arrow_csv_args is None:
            arrow_csv_args = {}

        self._read_options = arrow_csv_args.pop(
            "read_options", csv.ReadOptions(use_threads=False)
        )
        self._parse_options = arrow_csv_args.pop("parse_options", csv.ParseOptions())
        self._arrow_csv_args = arrow_csv_args

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        import pyarrow as pa
        from pyarrow import csv

        # Re-init invalid row handler: https://issues.apache.org/jira/browse/ARROW-17641
        if hasattr(self._parse_options, "invalid_row_handler"):
            self._parse_options.invalid_row_handler = (
                self._parse_options.invalid_row_handler
            )

        try:
            reader = csv.open_csv(
                file,
                read_options=self._read_options,
                parse_options=self._parse_options,
                **self._arrow_csv_args,
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
