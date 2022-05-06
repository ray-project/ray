from typing import TYPE_CHECKING, Any, Dict, Callable, Iterator

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import BlockAccessor, Block
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_kwargs,
)


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

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[Block]:
        import pyarrow
        from pyarrow import csv

        read_options = reader_args.pop(
            "read_options", csv.ReadOptions(use_threads=False)
        )
        reader = csv.open_csv(f, read_options=read_options, **reader_args)
        schema = None
        while True:
            try:
                batch = reader.read_next_batch()
                table = pyarrow.Table.from_batches([batch], schema=schema)
                if schema is None:
                    schema = table.schema
                yield table
            except StopIteration:
                return

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

    def _file_format(self):
        return "csv"
