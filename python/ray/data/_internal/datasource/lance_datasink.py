import pickle
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa
from lance.namespace import LanceNamespace

from ray.data._internal.datasource.lance_utils import (
    create_storage_options_provider,
    get_or_create_namespace,
)
from ray.data._internal.util import _check_import
from ray.data.block import BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    import pandas as pd
    from lance.fragment import FragmentMetadata


def _declare_table_with_fallback(
    namespace: LanceNamespace, table_id: List[str]
) -> Tuple[str, Optional[Dict[str, str]]]:
    """Declare a table using declare_table, falling back to create_empty_table.

    Args:
        namespace: The namespace instance to use for table operations.
        table_id: List of strings identifying the table within the namespace.

    Returns:
        Tuple of (uri, storage_options)
    """
    try:
        from lance_namespace import DeclareTableRequest

        declare_request = DeclareTableRequest(id=table_id, location=None)
        declare_response = namespace.declare_table(declare_request)
        return declare_response.location, declare_response.storage_options
    except (AttributeError, NotImplementedError):
        # Fallback for older namespace implementations without declare_table
        from lance_namespace import CreateEmptyTableRequest

        create_request = CreateEmptyTableRequest(id=table_id)
        create_response = namespace.create_empty_table(create_request)
        return create_response.location, create_response.storage_options


def _write_fragment(
    stream: Iterable[Union["pa.Table", "pd.DataFrame"]],
    uri: str,
    *,
    schema: Optional["pa.Schema"] = None,
    max_rows_per_file: int = 64 * 1024 * 1024,
    max_bytes_per_file: Optional[int] = None,
    max_rows_per_group: int = 1024,  # Only useful for v1 writer.
    data_storage_version: Optional[str] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    storage_options_provider: Optional[Callable[[], Dict[str, Any]]] = None,
) -> List[Tuple["FragmentMetadata", "pa.Schema"]]:
    import pandas as pd
    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

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
        storage_options_provider=storage_options_provider,
    )
    return [(fragment, schema) for fragment in fragments]


class _BaseLanceDatasink(Datasink):
    """Base class for Lance Datasink."""

    def __init__(
        self,
        uri: Optional[str] = None,
        table_id: Optional[List[str]] = None,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "create",
        storage_options: Optional[Dict[str, Any]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[Dict[str, str]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        merged_storage_options = dict()
        if storage_options:
            merged_storage_options.update(storage_options)

        # Store namespace_impl and namespace_properties for worker reconstruction
        self._namespace_impl = namespace_impl
        self._namespace_properties = namespace_properties

        # Construct namespace from impl and properties (cached per worker)
        namespace = get_or_create_namespace(namespace_impl, namespace_properties)

        if namespace is not None and table_id is not None:
            self.table_id = table_id
            has_namespace_storage_options = False

            if mode == "append":
                # For append mode, we need to get existing table URI
                from lance_namespace import DescribeTableRequest

                describe_request = DescribeTableRequest(id=table_id)
                describe_response = namespace.describe_table(describe_request)
                self.uri = describe_response.location
                if describe_response.storage_options:
                    merged_storage_options.update(describe_response.storage_options)
                    has_namespace_storage_options = True
            elif mode == "overwrite":
                # For overwrite mode, try to get existing table, fallback to declare
                try:
                    from lance_namespace import DescribeTableRequest

                    describe_request = DescribeTableRequest(id=table_id)
                    describe_response = namespace.describe_table(describe_request)
                    self.uri = describe_response.location
                    if describe_response.storage_options:
                        merged_storage_options.update(describe_response.storage_options)
                        has_namespace_storage_options = True
                except Exception:
                    uri_result, ns_storage_options = _declare_table_with_fallback(
                        namespace, table_id
                    )
                    self.uri = uri_result
                    if ns_storage_options:
                        merged_storage_options.update(ns_storage_options)
                        has_namespace_storage_options = True
            else:
                # create mode, declare a new table
                uri_result, ns_storage_options = _declare_table_with_fallback(
                    namespace, table_id
                )
                self.uri = uri_result
                if ns_storage_options:
                    merged_storage_options.update(ns_storage_options)
                    has_namespace_storage_options = True

            # Mark that we have namespace storage options for provider creation
            self._has_namespace_storage_options = has_namespace_storage_options
        else:
            self.table_id = None
            self.uri = uri
            self._has_namespace_storage_options = False

        self.schema = schema
        self.mode = mode
        self.read_version: Optional[int] = None
        self.storage_options = merged_storage_options

    @property
    def storage_options_provider(
        self,
    ) -> Optional[Callable[[], Dict[str, Any]]]:
        """Lazily create storage options provider using namespace_impl/properties."""
        if not self._has_namespace_storage_options:
            return None
        return create_storage_options_provider(
            self._namespace_impl, self._namespace_properties, self.table_id
        )

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        _check_import(self, module="lance", package="pylance")

        import lance

        if self.mode == "append":
            ds = lance.LanceDataset(
                self.uri,
                storage_options=self.storage_options,
                storage_options_provider=self.storage_options_provider,
            )
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
        lance.LanceDataset.commit(
            self.uri,
            op,
            read_version=self.read_version,
            storage_options=self.storage_options,
            storage_options_provider=self.storage_options_provider,
        )


class LanceDatasink(_BaseLanceDatasink):
    """Lance Ray Datasink.

    Write a Ray dataset to lance.

    If we expect to write larger-than-memory files,
    we can use `LanceFragmentWriter` and `LanceCommitter`.

    Args:
        uri: The base URI of the dataset. Optional if using table_id with namespace.
        table_id: List of strings identifying the table within the namespace.
            Used together with namespace_impl and namespace_properties.
        schema: pyarrow.Schema, optional. The schema of the dataset.
        mode: The write mode. Default is 'append'.
            Choices are 'append', 'create', 'overwrite'.
        min_rows_per_file: The minimum number of rows per file. Default is 1024 * 1024.
        max_rows_per_file: The maximum number of rows per file.
            Default is 64 * 1024 * 1024.
        data_storage_version: The version of the data storage format to use.
            Newer versions are more efficient but require newer versions of lance
            to read. The default is "legacy" which will use the legacy v1 version.
            See the user guide for more details.
        storage_options: The storage options for the writer. Default is None.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id for credentials
            vending in distributed workers.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id for credentials vending.
        *args: Additional positional arguments.
        **kwargs: Additional keyword arguments.
    """

    NAME = "Lance"

    def __init__(
        self,
        uri: Optional[str] = None,
        table_id: Optional[List[str]] = None,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "append", "overwrite"] = "append",
        min_rows_per_file: int = 1024 * 1024,
        max_rows_per_file: int = 64 * 1024 * 1024,
        data_storage_version: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[Dict[str, str]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            uri,
            table_id,
            schema=schema,
            mode=mode,
            storage_options=storage_options,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            *args,
            **kwargs,
        )

        self.min_rows_per_file = min_rows_per_file
        self.max_rows_per_file = max_rows_per_file
        self.data_storage_version = data_storage_version
        # if mode is append, read_version is read from existing dataset.
        self.read_version: Optional[int] = None

    @property
    def min_rows_per_write(self) -> int:
        return self.min_rows_per_file

    def get_name(self) -> str:
        return self.NAME

    def write(
        self,
        blocks: Iterable[Union[pa.Table, "pd.DataFrame"]],
        _ctx,
    ):
        fragments_and_schema = _write_fragment(
            blocks,
            self.uri,
            schema=self.schema,
            max_rows_per_file=self.max_rows_per_file,
            data_storage_version=self.data_storage_version,
            storage_options=self.storage_options,
            storage_options_provider=self.storage_options_provider,
        )
        return [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]
