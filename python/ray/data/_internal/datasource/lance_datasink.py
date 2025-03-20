from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    Literal,
    Optional,
    TYPE_CHECKING,
    List,
    Tuple,
    Iterable,
    Union,
)
import pickle

from ray.data._internal.util import _check_import
from ray.data.datasource.datasink import Datasink
from ray.data.block import BlockAccessor
import pyarrow as pa


if TYPE_CHECKING:
    import pandas as pd
    from lance.fragment import FragmentMetadata


def _write_fragment(
    stream: Iterable[Union["pa.Table", "pd.DataFrame"]],
    uri: str,
    *,
    schema: Optional["pa.Schema"] = None,
    max_rows_per_file: int = 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    max_rows_per_group: int = 1024,  # Only useful for v1 writer.
    data_storage_version: Optional[str] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> List[Tuple["FragmentMetadata", "pa.Schema"]]:
    import pandas as pd
    from lance.fragment import write_fragments, DEFAULT_MAX_BYTES_PER_FILE

    if schema is None:
        first = next(stream)
        if isinstance(first, pd.DataFrame):
            schema = pa.Schema.from_pandas(first).remove_metadata()
        else:
            schema = first.schema
        if len(schema.names) == 0:
            # Empty table.
            schema = None

        stream = chain([first], stream)

    def record_batch_converter():
        for block in stream:
            tbl = BlockAccessor.for_block(block).to_arrow()
            yield from tbl.to_batches()

    max_bytes_per_file = (
        DEFAULT_MAX_BYTES_PER_FILE if max_bytes_per_file is None else max_bytes_per_file
    )

    reader = pa.RecordBatchReader.from_batches(schema, record_batch_converter())
    fragments = write_fragments(
        reader,
        uri,
        schema=schema,
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=max_rows_per_group,
        max_bytes_per_file=max_bytes_per_file,
        data_storage_version=data_storage_version,
        storage_options=storage_options,
    )
    return [(fragment, schema) for fragment in fragments]


def _merge_columns(
    stream: Iterable[Union["pa.Table", "pd.DataFrame"]],
    ds,
    transform: Optional[str] = None,
    read_columns: Optional[List[str]] = None,
    batch_size: int = 1024,
):
    commit_messages = []
    for block in stream:
        tbl = BlockAccessor.for_block(block).to_arrow()
        if tbl.num_rows == 0:
            continue
        if tbl.num_columns > 1:
            raise ValueError("merge column block only contain fragment_id col")
        for fragment_id in tbl.to_pydict()["fragment_id"]:
            fragment = ds.get_fragment(fragment_id)
            new_fragment, new_schema = fragment.merge_columns(
                transform, read_columns, batch_size=batch_size
            )
            commit_messages.append((new_fragment, new_schema))
    return commit_messages


class _BaseLanceDatasink(Datasink):
    """Base class for Lance Datasink."""

    def __init__(
        self,
        uri: str,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        storage_options: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.uri = uri
        self.schema = schema
        self.mode = mode

        self.read_version: Optional[int] = None
        self.storage_options = storage_options

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self):
        _check_import(self, module="lance", package="pylance")

        import lance

        if self.mode in ["append", "merge_column"]:
            ds = lance.LanceDataset(self.uri, storage_options=self.storage_options)
            self.read_version = ds.version
            if self.schema is None:
                self.schema = ds.schema

    def on_write_complete(
        self,
        write_results: List[List[Tuple[str, str]]],
    ):
        import warnings
        import lance

        if not write_results:
            warnings.warn(
                "write_results is empty.",
                DeprecationWarning,
            )
            return
        if hasattr(write_results, "write_returns"):
            write_results = write_results.write_returns

        if len(write_results) == 0:
            warnings.warn(
                "write results is empty. please check ray version or internal error",
                DeprecationWarning,
            )
            return

        fragments = []
        schema = None
        for batch in write_results:
            for fragment_str, schema_str in batch:
                fragment = pickle.loads(fragment_str)
                fragments.append(fragment)
                schema = pickle.loads(schema_str)
        # Check weather writer has fragments or not.
        # Skip commit when there are no fragments.
        if not schema:
            return
        if self.mode in {"create", "overwrite"}:
            op = lance.LanceOperation.Overwrite(schema, fragments)
        elif self.mode == "append":
            op = lance.LanceOperation.Append(fragments)
        elif self.mode == "merge_column":
            op = lance.LanceOperation.Merge(fragments, schema)
        lance.LanceDataset.commit(
            self.uri,
            op,
            read_version=self.read_version,
            storage_options=self.storage_options,
        )


class LanceDatasink(_BaseLanceDatasink):
    """Lance Ray Datasink.

    Write a Ray dataset to lance.

    If we expect to write larger-than-memory files,
    we can use `LanceFragmentWriter` and `LanceCommitter`.

    Args:
        uri : the base URI of the dataset.
        schema : pyarrow.Schema, optional.
            The schema of the dataset.
        mode : str, optional
            The write mode. Default is 'append'.
            Choices are 'append', 'create', 'overwrite'.
        max_rows_per_file : int, optional
            The maximum number of rows per file. Default is 1024 * 1024.
        data_storage_version: optional, str, default None
            The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read.  The default is
            "legacy" which will use the legacy v1 version.  See the user guide
            for more details.
        storage_options : Dict[str, Any], optional
            The storage options for the writer. Default is None.
    """

    NAME = "Lance"

    global SUPPORT_OPERATIONS
    SUPPORT_OPERATIONS = ["create", "append", "overwrite", "merge_column"]

    def __init__(
        self,
        uri: str,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite", "merge_column"] = "create",
        max_rows_per_file: int = 1024 * 1024,
        data_storage_version: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        transform: Optional[
            Dict[str, str] | Callable[[pa.RecordBatch], pa.RecordBatch]
        ] = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            uri,
            schema=schema,
            mode=mode,
            storage_options=storage_options,
            *args,
            **kwargs,
        )

        import lance

        self.max_rows_per_file = max_rows_per_file
        self.data_storage_version = data_storage_version
        # if mode is append, read_version is read from existing dataset.
        self.read_version: Optional[int] = None
        if mode not in SUPPORT_OPERATIONS:
            raise ValueError(
                f"Invalid mode: {mode}. Mode must be one of {SUPPORT_OPERATIONS}"
            )
        self.mode = mode
        self.transform = transform
        # if mode is merge_column, read_version is read from existing dataset.
        if self.mode in ["merge_column", "append"]:
            self.ds = lance.LanceDataset(self.uri, storage_options=self.storage_options)

    @property
    def num_rows_per_write(self) -> int:
        return self.max_rows_per_file

    def get_name(self) -> str:
        return self.NAME

    def write(
        self,
        blocks: Iterable[Union[pa.Table, "pd.DataFrame"]],
        _ctx,
    ):
        fragments_and_schema = []
        if self.mode in ["append", "overwrite", "create"]:
            fragments_and_schema = _write_fragment(
                blocks,
                self.uri,
                schema=self.schema,
                max_rows_per_file=self.max_rows_per_file,
                data_storage_version=self.data_storage_version,
                storage_options=self.storage_options,
            )
        elif self.mode == "merge_column":
            fragments_and_schema = _merge_columns(
                stream=blocks,
                ds=self.ds,
                transform=self.transform,
            )
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
        return [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]
