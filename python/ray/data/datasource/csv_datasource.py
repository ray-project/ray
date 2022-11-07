from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator

from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_kwargs,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class CSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading and writing CSV files.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import CSVDatasource
        >>> source = CSVDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [{"a": 1, "b": "foo"}, ...]
    """

    _FILE_EXTENSION = "csv"

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[Block]:
        import pyarrow as pa
        from pyarrow import csv

        read_options = reader_args.pop(
            "read_options", csv.ReadOptions(use_threads=False)
        )
        parse_options = reader_args.pop("parse_options", csv.ParseOptions())
        # Re-init invalid row handler: https://issues.apache.org/jira/browse/ARROW-17641
        if hasattr(parse_options, "invalid_row_handler"):
            parse_options.invalid_row_handler = parse_options.invalid_row_handler

        try:
            reader = csv.open_csv(
                f, read_options=read_options, parse_options=parse_options, **reader_args
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

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        from pyarrow import csv

        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        write_options = writer_args.pop("write_options", None)
        csv.write_csv(block.to_arrow(), f, write_options, **writer_args)
