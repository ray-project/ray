import json
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from deltalake import (
    WriterProperties,
    CommitProperties,
    PostCommitHookProperties,
)
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs
from packaging.version import parse as parse_version
from pyarrow.parquet import FileMetaData

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray._raylet import ObjectRef
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_datasink import _FileDatasink


logger = logging.getLogger(__name__)


class WriteMode(Enum):
    # Error if the table already exists
    ERROR = "error"
    # Append to the table if it exists
    APPEND = "append"
    # Overwrite the table if it exists
    OVERWRITE = "overwrite"
    # Ignore the write if the table already exists
    IGNORE = "ignore"
    # Merge the table if it exists
    MERGE = "merge"


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


@dataclass
class DeltaSinkWriteResult:
    actions: List[AddAction]
    schema: Optional[pa.Schema] = None


class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class AWSUtilities:
    @staticmethod
    def _get_aws_credentials():
        # TODO: This is a temporary fix to get the region from the cluster resources,
        # TODO: This should be replaced with a proper implementation as part of a shared Anyscale utils library
        from boto3 import Session

        # Grab the credentials from the current session, needed to authenticate the Delta Lake library
        session = Session()
        credentials = session.get_credentials()
        return credentials

    @staticmethod
    def _get_aws_region():
        # TODO: This is a temporary fix to get the region from the cluster resources,
        # TODO: This should be replaced with a proper implementation as part of a shared Anyscale utils library
        # Run in the if block to avoid the reinit step and logging
        if not ray.is_initialized():
            ray.init()

        # The cleanest apparent way of grabbing the current region is through the naming convention
        # of the cluster resources. The region is stored in the key like "anyscale/region:us-west-2"

        # Somehow Boto3 doesn't have a clean way of grabbing the current region??
        cluster_resources = ray.cluster_resources()
        region = [
            k.split(":")[1]
            for k in cluster_resources.keys()
            if k.startswith("anyscale/region:")
        ]
        region = region[0] if len(region) > 0 else None
        return region


def try_get_deltatable(
    table_uri: Union[str, Path], storage_options: Optional[Dict[str, str]]
):
    """
    Attempts to get a DeltaTable from the given URI.
    """
    from deltalake.table import DeltaTable

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except Exception as e:
        print(f"Warning: Failed to get DeltaTable from {table_uri}: {e}")
        return None


@dataclass
class DeltaWriteConfig:
    """
    Configuration object for writing data to a DeltaTable.
    All parameters default to None unless specified.
    """

    # --- Required for file/block writing ---
    schema: Optional[pa.Schema] = None

    # --- Partitioning ---
    partition_cols: Optional[List[str]] = None
    partition_by: Optional[Union[List[str], str]] = None
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None

    # --- File writing options ---
    file_options: Optional[Any] = None  # e.g. ParquetFileWriteOptions
    max_partitions: Optional[int] = None
    max_open_files: int = 1024
    max_rows_per_file: int = 10 * 1024 * 1024
    max_rows_per_group: int = 128 * 1024
    min_rows_per_group: int = 64 * 1024

    # --- Table (delta metadata) ---
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[Mapping[str, Optional[str]]] = None

    # --- Schema/overwrite ---
    overwrite_schema: bool = (
        False  # When used, mapped to schema_mode='overwrite' in new API
    )

    # --- Storage/engine ---
    storage_options: Optional[Dict[str, str]] = None
    engine: Literal["pyarrow", "rust"] = "rust"
    large_dtypes: bool = False
    writer_properties: Optional[WriterProperties] = None  # Native Delta/WritersOptions
    predicate: Optional[str] = None  # Overwrite predicate
    target_file_size: Optional[int] = None

    # --- Commit & transaction ---
    commit_properties: Optional[CommitProperties] = None
    post_commithook_properties: Optional[PostCommitHookProperties] = None

    @classmethod
    def from_dict(cls, config_dict: dict):
        """
        Create a DeltaWriteConfig from a dictionary. Only sets attributes present in the class.
        """
        obj = cls()
        for key, val in config_dict.items():
            if hasattr(obj, key):
                setattr(obj, key, val)
            else:
                raise ValueError(f"Unknown config key: {key}")
        return obj


class DeltaUtilities:
    def __init__(
        self,
        delta_uri: str,
        schema: pa.schema,
        filesystem: pa_fs.FileSystem,
        partition_cols: Optional[List[str]] = None,
        max_partitions: Optional[int] = None,
        config: Optional[DeltaWriteConfig] = None,
    ) -> None:
        """
        Handles writing data batches to a Delta Table

        Examples:
            .. testcode::
                :skipif: True

                writer = DeltaDatasetWriter(
                    delta_uri="s3://bucket/path/to/delta-table",
                    schema=schema,
                    filesystem=fs,
                    partition_cols=["country", "date"]
                )
                add_actions = writer.write(batch_ref, file_options=file_opts)

        Args:
            delta_uri: The root URI of the Delta table.
            schema: The expected Arrow schema of the data.
            filesystem: The Arrow-compatible filesystem object.
            partition_cols: Columns to partition by; optional.
            max_partitions: Upper bound for file parallelism (default 128).
            config: Optional DeltaConfig object
        """
        self.delta_uri = delta_uri
        self.schema = schema
        self.filesystem = filesystem
        self.partition_cols = partition_cols or []
        self.max_partitions = max_partitions
        self._init_partitioning()
        self.write_config = config

    def _init_partitioning(self) -> None:
        """
        Initialize partitioning for PyArrow based on partition_cols.
        """
        if self.partition_cols:
            partition_schema = pa.schema(
                [
                    (name, typ)
                    for name, typ in zip(self.schema.names, self.schema.types)
                    if name in self.partition_cols
                ]
            )
            self.partitioning = pa_ds.partitioning(partition_schema, flavor="hive")
        else:
            self.partitioning = None

    def _get_add_action_for_written_file(
        self, written_file: Any, delta_uri: str
    ) -> Any:
        """
        Converts a written file record into an AddAction object.
        You may want to adapt this for your AddAction and file stats logic.

        Args:
            written_file: Object from PyArrow's file_visitor.
            delta_uri: Root URI of the Delta table.

        Returns:
            An AddAction object.
        """
        # Partition extraction, Delta path normalization
        path, partition_values = self._get_partitions_from_path(written_file.path)
        path = self._add_scheme_to_path(urlparse(delta_uri).scheme, path)
        stats = self._get_file_stats_from_metadata(written_file.metadata)
        if str(get_pyarrow_version()) >= "9.0.0":
            size = written_file.size
        else:
            size = self.filesystem.get_file_info([path])[0].size
        add_action = AddAction(
            path=path,
            size=size,
            partition_values=partition_values,
            modification_time=int(datetime.now().timestamp() * 1000),
            data_change=True,
            stats=json.dumps(stats, cls=DeltaJSONEncoder),
        )
        return add_action

    @staticmethod
    def _validate_schema(
        schema: pa.Schema,
        partition_cols: Optional[List[str]] = None,
    ) -> None:
        """
        Validates the schema against the partition columns.
        Args:
            schema: The schema to validate.
            partition_cols: The partition columns to validate against.
        Raises:
            ValueError: If the schema is invalid.
        """
        if partition_cols is not None:
            for col in partition_cols:
                if col not in schema.names:
                    raise ValueError(f"Partition column {col} not found in schema.")

    @staticmethod
    def _add_scheme_to_path(scheme: str, path: str) -> str:
        """Ensure the path has the correct URI scheme.

        Args:
            scheme: File format scheme
            path: The path to parse
        Returns:
            String of the {scheme}://{path}
        """
        parsed = urlparse(path)
        if parsed.scheme:
            return path
        return f"{scheme}://{path}"

    @staticmethod
    def _get_partitions_from_path(path: str) -> Tuple[str, Dict[str, Optional[str]]]:
        """
        Parses the path to extract partition information.

        Args:
            path: The path to parse.

        Returns:
            A tuple containing the path and a dictionary of partition values.
        """
        if path[0] == "/":
            path = path[1:]
        parts = path.split("/")
        parts.pop()  # remove filename
        out: Dict[str, Optional[str]] = {}

        for part in parts:
            if part == "":
                continue
            if "=" in part:
                key, value = part.split("=", maxsplit=1)
                if value == "__HIVE_DEFAULT_PARTITION__":
                    out[key] = None
                else:
                    out[key] = value
        return path, out

    @staticmethod
    def _get_file_stats_from_metadata(
        metadata: FileMetaData,
    ) -> Dict[str, Union[int, Dict[str, Any]]]:
        """
        Extracts statistics from the metadata.

        Args:
            metadata: The metadata to extract statistics from.

        Returns:
            dictionary containing the statistics.
        """
        stats = {
            "numRecords": metadata.num_rows,
            "minValues": {},
            "maxValues": {},
            "nullCount": {},
        }

        def iter_groups(metadata) -> Iterator[Any]:
            for i in range(metadata.num_row_groups):
                yield metadata.row_group(i)

        for column_idx in range(metadata.num_columns):
            if metadata.num_row_groups > 0:
                name = metadata.row_group(0).column(column_idx).path_in_schema
                # If stats missing, then we can't know aggregate stats
                if all(
                    group.column(column_idx).is_stats_set
                    for group in iter_groups(metadata)
                ):
                    stats["nullCount"][name] = sum(
                        group.column(column_idx).statistics.null_count
                        for group in iter_groups(metadata)
                    )

                    # Min / max may not exist for some column types, or if all values are null
                    if any(
                        group.column(column_idx).statistics.has_min_max
                        for group in iter_groups(metadata)
                    ):
                        # Min and Max are recorded in physical type, not logical type
                        # https://stackoverflow.com/questions/66753485/decoding-parquet-min-max-statistics-for-decimal-type
                        # TODO: Add logic to decode physical type for DATE, DECIMAL
                        logical_type = (
                            metadata.row_group(0)
                            .column(column_idx)
                            .statistics.logical_type.type
                        )
                        if get_pyarrow_version() < parse_version(
                            "8.0.0"
                        ) and logical_type not in [
                            "STRING",
                            "INT",
                            "TIMESTAMP",
                            "NONE",
                        ]:
                            continue

                        minimums = (
                            group.column(column_idx).statistics.min
                            for group in iter_groups(metadata)
                        )
                        # If some row groups have all null values, their min and max will be null too.
                        stats["minValues"][name] = min(
                            minimum for minimum in minimums if minimum is not None
                        )
                        maximums = (
                            group.column(column_idx).statistics.max
                            for group in iter_groups(metadata)
                        )
                        stats["maxValues"][name] = max(
                            maximum for maximum in maximums if maximum is not None
                        )
        return stats

    def _get_current_delta_version(self, storage_options: Optional[dict] = None) -> int:
        """
        Fetch the current version of the Delta table, or -1 if new.

        Args:
            storage_options: Optional storage options to pass to ``try_get_deltatable``

        Returns:
            The integer version.
        """
        table = try_get_deltatable(self.delta_uri, storage_options=storage_options)
        if table is None:
            return -1
        else:
            table.update_incremental()
            return table.version()

    def write_raw_data(
        self,
        batch_ref: ObjectRef,
        file_options: Optional[Dict] = None,
        storage_options: Optional[Dict] = None,
    ) -> List[Any]:
        """
        Write a batch (Arrow Table, Dataset, etc.) to the Delta table.

        Args:
            batch_ref: The data to write
            file_options: File-level write options.
            storage_options: Optional storage options to pass to ``_get_current_delta_version``

        Returns:
            A list of AddAction objects for the new files.
        """
        add_actions = []

        # Get the latest version of the Delta table in case of concurrent writes
        version = self._get_current_delta_version(storage_options=storage_options)

        # Extract the base directory from the Delta URI
        # base_dir = urlparse(self.delta_uri).path.lstrip("/")

        parsed = urlparse(self.delta_uri)
        base_dir = f"{parsed.netloc}{parsed.path}"
        base_dir = base_dir.lstrip("/")

        # Generate a unique basename for the files to be written
        basename_template = f"{version + 1}-{uuid.uuid4()}-{{i}}.parquet"

        def visitor(written_file: Any) -> None:
            add_action = self._get_add_action_for_written_file(
                written_file, self.delta_uri
            )
            add_actions.append(add_action)

        arrow_table = BlockAccessor.for_block(batch_ref).to_arrow()

        pa_ds.write_dataset(
            arrow_table,
            base_dir=base_dir,
            basename_template=basename_template,
            format="parquet",
            partitioning=self.partitioning,
            schema=self.schema,
            file_visitor=visitor,
            existing_data_behavior="overwrite_or_ignore",
            file_options=file_options,
            max_open_files=self.write_config.max_open_files,
            max_rows_per_file=self.write_config.max_rows_per_file,
            min_rows_per_group=self.write_config.min_rows_per_group,
            max_rows_per_group=self.write_config.max_rows_per_group,
            filesystem=self.filesystem,
            max_partitions=self.max_partitions,
        )
        return add_actions


class DeltaDatasink(_FileDatasink):

    NAME = "Delta"

    def __init__(
        self,
        path: str,
        *,
        mode: str = WriteMode.APPEND.value,
        filesystem: Optional[pa_fs.FileSystem] = None,
        delta_config: Optional[Union[DeltaWriteConfig, dict]] = None,
        **file_datasink_kwargs,
    ):
        """
        Ray Data datasink for writing blocks to a Delta Lake table.

        This datasink allows writing Parquet files and delta transaction logs
        adhering to the Delta Lake standard (delta-rs backend). It supports both
        table creation and updates (append/overwrite/ignore/error modes), custom file writing
        options, partitioning, and cloud storage through PyArrow Filesystem abstraction.

        Args:
            path: URI or local filesystem path to the root of the Delta Lake table.
            mode: Write behavior mode. Choices are "append" (default),
                "overwrite", "error", "ignore", "merge".
            filesystem: Arrow-compatible filesystem object (for S3, local, etc.). If None, inferred from path.
            delta_config: Delta writer configuration as either a DeltaWriteConfig
                dataclass or a plain dictionary (see DeltaWriteConfig for available options).
            **file_datasink_kwargs: Additional keyword arguments for the parent file datasink (compression, etc).

        Example:
            >>> datasink = DeltaDatasink(
            ...     path="s3://bucket/my-table",
            ...     delta_config={"engine": "rust", "max_open_files": 128}
            ... )
        """
        super().__init__(path, **file_datasink_kwargs)
        _check_import(self, module="deltalake", package="deltalake")
        if not isinstance(mode, str):
            raise ValueError(f"mode must be a string, got {type(mode)}")
        valid_modes = [mode.value for mode in WriteMode]
        if mode not in valid_modes:
            raise ValueError(f"mode must be one of {valid_modes}, got {mode}")

        # If config is a dict, convert to DeltaWriteConfig; otherwise use as is or default.
        if delta_config is None:
            self.config = DeltaWriteConfig()
        elif isinstance(delta_config, dict):
            self.config = DeltaWriteConfig.from_dict(delta_config)
        elif isinstance(delta_config, DeltaWriteConfig):
            self.config = delta_config
        else:
            raise ValueError(
                "delta_config must be a DeltaWriteConfig instance or a dict"
            )
        self.path = path

        # These can be dynamically set/discovered at runtime, so keep direct attribute.
        self.schema = self.config.schema
        self.partition_cols = self.config.partition_cols
        if isinstance(self.partition_cols, str):
            self.partition_cols = [self.partition_cols]
        self.mode = mode
        if filesystem is not None:
            self.filesystem = filesystem

        self.is_aws = True if self.path.startswith("s3://") else False
        # TODO: Add support for GCP and Azure
        self.is_gcp = True if self.path.startswith("gs://") else False
        self.is_azure = True if self.path.startswith("abfss://") else False
        self.storage_options = self._get_storage_options()

    def _get_storage_options(self):
        if self.is_aws is True:
            credentials = AWSUtilities._get_aws_credentials()
            region = AWSUtilities._get_aws_region()
            _storage_options = (
                {}
                if self.config.storage_options is None
                else self.config.storage_options.copy()
            )
            _storage_options["AWS_SECRET_ACCESS_KEY"] = credentials.access_key
            _storage_options["AWS_REGION"] = region
            _storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
        else:
            _storage_options = self.config.storage_options
        return _storage_options

    @property
    def max_rows_per_file(self) -> int:
        return self.config.max_rows_per_file

    @staticmethod
    def _get_table_and_table_uri(
        path: str,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        """Parses the `path`.

        Args:

            path: URI of a table or a DeltaTable object.
            storage_options: Options passed to the native delta filesystem.

        Returns:
            DeltaTable object, URI of the table
        """
        from deltalake.table import DeltaTable

        if not isinstance(path, (str, Path, DeltaTable)):
            raise ValueError("path must be a str, Path or DeltaTable")

        if isinstance(path, (str, Path)):
            table = try_get_deltatable(path, storage_options)
            table_uri = str(path)
        else:
            raise TypeError(f"Invalid path arg, must be str: {path}")

        return (table, table_uri)

    @staticmethod
    def _validate_mode(
        configuration: Optional[Mapping[str, Optional[str]]],
        mode: Optional[WriteMode] = WriteMode.APPEND.value,
    ) -> None:
        config_delta_append_only = (
            configuration and configuration.get("delta.appendOnly", "false") == "true"
        )
        if config_delta_append_only and mode != "append":
            raise ValueError(
                "If configuration has delta.appendOnly = 'true', mode must be 'append'."
                f" Mode is currently {mode}"
            )

    def _extract_schema_and_partition_cols(self, blocks: Iterable[Block]):
        # Finds and returns arrow schema and partition columns from the first non-empty block.
        for block in blocks:
            acc = BlockAccessor.for_block(block)
            if acc.num_rows() > 0:
                # Assumes all blocks have the same schema
                table = acc.to_arrow()
                return table.schema, self.partition_cols or []
        return None, self.partition_cols or []

    def _get_schema(self, write_result: DeltaSinkWriteResult):
        schema = None
        if hasattr(write_result, "schema") and write_result.schema is not None:
            schema = write_result.schema

        elif hasattr(write_result, "write_returns"):
            for res in write_result.write_returns:
                if hasattr(res, "schema") and res.schema is not None:
                    schema = res.schema
                    break

        if schema is not None:
            self.schema = schema

        if self.schema is None:
            raise ValueError(
                "Schema could not be determined for Delta table commit. "
                "Either provide it at construction, or ensure it is available from DataSink write."
            )
        return self.schema

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> DeltaSinkWriteResult:
        blocks = list(blocks)
        if all(BlockAccessor.for_block(block).num_rows() == 0 for block in blocks):
            return DeltaSinkWriteResult(actions=[])
        # --- Extract or check schema ---
        if self.schema is None:
            schema, pcols = self._extract_schema_and_partition_cols(blocks)
            if schema is None:
                raise ValueError(
                    "No non-empty data blocks found; unable to infer schema."
                )
            self.schema = schema
            self.partition_cols = pcols

        # print(f"DeltaDatasink: Writing {len(blocks)} blocks with schema {self.schema} and partition columns {self.partition_cols}")
        # print(f"DeltaDatasink: Writing to {self.path} with mode {self.mode} and storage options {self.storage_options}")

        # Get the table and table URI from the path argument
        # This is done within the write function as the table object cannot be serialized
        table, table_uri = self._get_table_and_table_uri(
            self.path, self.storage_options
        )

        # Update the table to get the latest config in case of a concurrent write
        if table:
            table.update_incremental()

        # Check if the table is append-only and if it matches with the write config
        self._validate_mode(configuration=self.config.configuration, mode=self.mode)

        # If the table exists, check if the schema matches
        if table:
            if self.schema != table.schema().to_pyarrow() and not (
                self.mode == WriteMode.OVERWRITE.value and self.config.overwrite_schema
            ):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Table schema:\n{self.schema}\nData Schema:\n{table.schema().to_pyarrow()}"
                )
            # If the table exists and the mode is either "error" or "ignore", raise an error or ignore
            if self.mode == WriteMode.ERROR.value:
                raise FileExistsError(
                    "DeltaTable already exists (using writing mode=error)"
                )
            elif self.mode == WriteMode.IGNORE.value:
                print(
                    "DeltaTable already exists, skipping write (using writing mode=ignore)"
                )
                return DeltaSinkWriteResult(actions=[])

            # Ensure that the partition scheme matches the existing table
            if self.partition_cols:
                assert self.partition_cols == table.metadata().partition_columns
            else:
                # If the table exists and the partition_cols is not set, set it to the table's partition columns
                self.partition_cols = table.metadata().partition_columns

        # Store the write actions to add to the metadata later
        add_actions = []

        delta_writer = DeltaUtilities(
            delta_uri=table_uri,
            schema=self.schema,
            filesystem=self.filesystem,
            partition_cols=self.partition_cols,
            max_partitions=self.config.max_partitions,
            config=self.config,
        )

        # Write the dataset to the Delta table and get the actions as a result
        # This is done separate from the write function to avoid concurrent requests
        # against the metadata that would block each other
        for batch_ref in blocks:
            add_actions.append(
                delta_writer.write_raw_data(
                    batch_ref=batch_ref,
                    file_options=self.config.file_options,
                    storage_options=self.storage_options,
                )
            )

        final_add_actions = []
        for action in add_actions:
            if action is not None:
                for sub_action in action:
                    final_add_actions.append(sub_action)

        return DeltaSinkWriteResult(actions=final_add_actions, schema=self.schema)

    def on_write_complete(self, write_result):
        from deltalake import write_deltalake

        # Defensive: update self.schema from the write result if available
        self.schema = self._get_schema(write_result)

        # Only append/overwrite supported, else error
        if self.mode not in [WriteMode.APPEND.value, WriteMode.OVERWRITE.value]:
            raise NotImplementedError(
                "Only append/overwrite modes are supported with the new API."
            )

        # Collect AddActions into a flat list
        final_add_actions = []
        if hasattr(write_result, "write_returns"):  # Ray distributed result style
            for result in write_result.write_returns:
                final_add_actions.extend(result.actions)
        elif hasattr(write_result, "actions"):
            final_add_actions.extend(write_result.actions)
        if len(final_add_actions) == 0:
            return  # Nothing to flush

        # Table may or may not exist: Both valid for write_deltalake
        table, table_uri = self._get_table_and_table_uri(
            self.path, self.storage_options
        )

        # Map overwrite_schema to schema_mode string for new API
        schema_mode = None
        if (
            getattr(self.config, "overwrite_schema", False)
            and self.mode == WriteMode.OVERWRITE.value
        ):
            schema_mode = "overwrite"

        # Set up commit properties and post commit hook properties if present on config
        commit_properties = getattr(self.config, "commit_properties", None)
        post_commithook_properties = getattr(
            self.config, "post_commithook_properties", None
        )

        # Partition argument: config wins, else use autodetected
        partition_by = self.config.partition_by or self.partition_cols

        # Actually write to Delta using new API
        write_deltalake(
            table_or_uri=table_uri,
            data=final_add_actions,
            partition_by=partition_by,
            mode=self.mode,
            name=self.config.name,
            description=self.config.description,
            configuration=self.config.configuration,
            schema_mode=schema_mode,
            storage_options=self.storage_options,
            predicate=self.config.predicate,
            target_file_size=self.config.target_file_size,
            writer_properties=self.config.writer_properties,
            post_commithook_properties=post_commithook_properties,
            commit_properties=commit_properties,
        )

        return write_result
