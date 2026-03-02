import pickle
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

from ray._common.retry import call_with_retry
from ray.data._internal.datasource.lance_utils import (
    create_storage_options_provider,
    get_or_create_namespace,
)
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import _check_import, unify_schemas_with_validation
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    import pandas as pd
    from lance.fragment import FragmentMetadata


def _declare_table_with_fallback(
    namespace, table_id: List[str]
) -> Tuple[str, Optional[Dict[str, str]]]:
    """Declare a table using declare_table, falling back to create_empty_table."""
    try:
        from lance_namespace import DeclareTableRequest

        declare_request = DeclareTableRequest(id=table_id, location=None)
        declare_response = namespace.declare_table(declare_request)
        return declare_response.location, declare_response.storage_options
    except (AttributeError, NotImplementedError):
        # Fallback for older namespace implementations without declare_table.
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
    namespace_impl: Optional[str] = None,
    namespace_properties: Optional[Dict[str, str]] = None,
    table_id: Optional[List[str]] = None,
    retry_params: Optional[Dict[str, Any]] = None,
) -> List[Tuple["FragmentMetadata", "pa.Schema"]]:
    import pandas as pd
    from lance.fragment import DEFAULT_MAX_BYTES_PER_FILE, write_fragments

    stream = list(stream)
    if not stream:
        return []

    if schema is None:
        first = stream[0]
        if isinstance(first, pd.DataFrame):
            schema = pa.Schema.from_pandas(first).remove_metadata()
        else:
            schema = first.schema
        if len(schema.names) == 0:
            # Empty table.
            schema = None

    def record_batch_converter():
        for block in stream:
            tbl = BlockAccessor.for_block(block).to_arrow()
            yield from tbl.to_batches()

    max_bytes_per_file = (
        DEFAULT_MAX_BYTES_PER_FILE if max_bytes_per_file is None else max_bytes_per_file
    )

    if retry_params is None:
        retry_params = {
            "description": "write lance fragments",
            "match": [],
            "max_attempts": 1,
            "max_backoff_s": 0,
        }

    storage_options_provider = create_storage_options_provider(
        namespace_impl,
        namespace_properties,
        table_id,
    )

    def _write_once():
        reader = pa.RecordBatchReader.from_batches(schema, record_batch_converter())
        return write_fragments(
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

    fragments = call_with_retry(_write_once, **retry_params)
    return [(fragment, schema) for fragment in fragments]


class _BaseLanceDatasink(Datasink):
    """Base class for Lance Datasink."""

    def __init__(
        self,
        uri: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        mode: SaveMode = SaveMode.CREATE,
        storage_options: Optional[Dict[str, Any]] = None,
        table_id: Optional[List[str]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[Dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        if mode not in {SaveMode.CREATE, SaveMode.APPEND, SaveMode.OVERWRITE}:
            raise ValueError(
                f"Unsupported Lance write mode: {mode!r}. "
                "Supported modes are SaveMode.CREATE, SaveMode.APPEND, and SaveMode.OVERWRITE."
            )

        merged_storage_options: Dict[str, Any] = {}
        if storage_options:
            merged_storage_options.update(storage_options)

        self._namespace_impl = namespace_impl
        self._namespace_properties = namespace_properties

        namespace = get_or_create_namespace(namespace_impl, namespace_properties)

        if namespace is not None and table_id is not None:
            if uri is not None:
                import warnings

                warnings.warn(
                    "The 'uri' argument is ignored when namespace parameters are "
                    "provided. The resolved namespace location will be used instead.",
                    UserWarning,
                    stacklevel=2,
                )

            self.table_id = table_id
            if mode != SaveMode.CREATE:
                raise ValueError(
                    "Namespace writes currently only support mode='create'. "
                    "Use mode='create' for now."
                )

            uri, ns_storage_options = _declare_table_with_fallback(namespace, table_id)
            self.uri = uri
            if ns_storage_options:
                merged_storage_options.update(ns_storage_options)
            self._has_namespace_storage_options = True
        else:
            self.table_id = None
            if uri is None:
                raise ValueError(
                    "Must provide either 'uri' or ('namespace_impl' and 'table_id')."
                )
            self.uri = uri
            self._has_namespace_storage_options = False

        self.schema = schema
        self.mode = mode

        self.read_version: Optional[int] = None
        self.storage_options = merged_storage_options

    @property
    def storage_options_provider(self):
        """Lazily create storage options provider using namespace_impl/properties."""
        if not self._has_namespace_storage_options:
            return None
        return create_storage_options_provider(
            self._namespace_impl,
            self._namespace_properties,
            self.table_id,
        )

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        _check_import(self, module="lance", package="pylance")

        import lance

        if self.mode == SaveMode.APPEND:
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
        schemas = []
        for batch in write_results:
            for fragment_str, schema_str in batch:
                fragment = pickle.loads(fragment_str)
                fragments.append(fragment)
                schema = pickle.loads(schema_str)
                if schema is not None:
                    schemas.append(schema)
        # Skip commit when there are no fragments/schemas to commit.
        if not schemas:
            return

        unified_schema = unify_schemas_with_validation(schemas)
        if unified_schema is None:
            return

        if self.mode in {SaveMode.CREATE, SaveMode.OVERWRITE}:
            op = lance.LanceOperation.Overwrite(unified_schema, fragments)
        elif self.mode == SaveMode.APPEND:
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
        uri: The base URI of the dataset.
        schema: The schema of the dataset.
        mode: The write mode. Default is SaveMode.CREATE. Choices are
            SaveMode.CREATE, SaveMode.APPEND, SaveMode.OVERWRITE.
        min_rows_per_file: The minimum number of rows per file. Default is 1024 * 1024.
        max_rows_per_file: The maximum number of rows per file. Default is 64 * 1024 * 1024.
        data_storage_version: The version of the data storage format to use. Newer versions are more
            efficient but require newer versions of lance to read. The default is "legacy",
            which will use the legacy v1 version. See the user guide for more details.
        storage_options: The storage options for the writer. Default is None.
        table_id: The table identifier as a list of strings, used with namespace params.
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
            Used together with namespace_properties and table_id for credentials vending.
        namespace_properties: Properties for connecting to the namespace.
            Used together with namespace_impl and table_id for credentials vending.
        *args: Additional positional arguments forwarded to the base class.
        **kwargs: Additional keyword arguments forwarded to the base class.
    """

    NAME = "Lance"

    def __init__(
        self,
        uri: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        mode: SaveMode = SaveMode.CREATE,
        min_rows_per_file: int = 1024 * 1024,
        max_rows_per_file: int = 64 * 1024 * 1024,
        data_storage_version: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        table_id: Optional[List[str]] = None,
        namespace_impl: Optional[str] = None,
        namespace_properties: Optional[Dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(
            uri,
            schema=schema,
            mode=mode,
            storage_options=storage_options,
            table_id=table_id,
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

        data_context = DataContext.get_current()
        lance_config = data_context.lance_config
        match = []
        match.extend(lance_config.write_fragments_errors_to_retry)
        match.extend(data_context.retried_io_errors)
        self._retry_params = {
            "description": "write lance fragments",
            "match": match,
            "max_attempts": lance_config.write_fragments_max_attempts,
            "max_backoff_s": lance_config.write_fragments_retry_max_backoff_s,
        }

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
            namespace_impl=self._namespace_impl,
            namespace_properties=self._namespace_properties,
            table_id=self.table_id,
            retry_params=self._retry_params,
        )
        return [
            (pickle.dumps(fragment), pickle.dumps(schema))
            for fragment, schema in fragments_and_schema
        ]
