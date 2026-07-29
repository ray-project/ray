import json
import logging
import posixpath
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional

from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.fs as pafs
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)

_SUPPORTED_MODES = {SaveMode.APPEND, SaveMode.OVERWRITE}

# ``schema_mode`` values this prototype supports for reconciling an APPEND's
# incoming schema against an existing table's schema. See ``DeltaDatasink``'s
# docstring and the ``schema_mode`` arg doc below for what each one does.
_SUPPORTED_SCHEMA_MODES = {"merge", "error"}

# Serializes the scoped ``GOOGLE_APPLICATION_CREDENTIALS`` mutation below.
# ``os.environ`` is process-wide, so two threads building GCS filesystems at
# once could otherwise observe (or restore) each other's value.
_gcs_env_lock = threading.Lock()


@dataclass
class DeltaWriteResult:
    """Result returned from each worker's ``write`` task.

    Attributes:
        add_actions: ``AddAction`` metadata for every Parquet file the worker wrote.
        schemas: PyArrow schema of each block the worker wrote, one per block. The
            driver unifies these across all workers to get the schema to commit.
    """

    add_actions: List["AddAction"] = field(default_factory=list)
    schemas: List["pa.Schema"] = field(default_factory=list)


@DeveloperAPI
class DeltaDatasink(Datasink[DeltaWriteResult]):
    """Datasink that writes a Ray Dataset to a Delta Lake table.

    Workers write Parquet files and return file metadata; the driver commits all
    files in a single Delta transaction. Supports ``SaveMode.APPEND`` and
    ``SaveMode.OVERWRITE``.

    Schema evolution on APPEND (``schema_mode``):
        A column present in the incoming data but not in the table is either
        added (``"merge"``, the default) or rejected (``"error"``). A column
        both schemas have with an incompatible type always raises.

        The schema is evolved in ``on_write_complete``, immediately before the
        data commit, so a failure anywhere earlier leaves the table's schema
        untouched. (Iceberg has to evolve in ``on_write_start`` instead,
        because PyIceberg binds field IDs at write time; Delta workers write
        plain Parquet and the schema is only established at commit.)
    """

    def __init__(
        self,
        path: str,
        *,
        mode: SaveMode = SaveMode.APPEND,
        partition_by: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        schema_mode: str = "merge",
        name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """Initialize the DeltaDatasink.

        Args:
            path: URI of the Delta table (e.g. ``/tmp/my_table`` or
                ``s3://bucket/my_table``).
            mode: Write mode. Only ``SaveMode.APPEND`` and ``SaveMode.OVERWRITE``
                are supported in this prototype.
            partition_by: Optional list of columns to partition the table by.
            storage_options: Backend storage options forwarded to ``deltalake`` for
                the commit (e.g. cloud credentials).
            schema_mode: How an ``APPEND`` reconciles its incoming schema
                against an existing table's schema, when the incoming data
                has a column the table doesn't. Has no effect on
                ``OVERWRITE`` (which always replaces the table's schema
                wholesale) or on a brand-new table (nothing to reconcile
                against yet). One of:

                * ``"merge"`` (default): add the new column(s) to the table
                  before committing, as an ``ALTER TABLE ADD COLUMN``-style
                  operation. New columns are always added as nullable --
                  every row already in the table has no value for a
                  brand-new column, so a non-nullable declaration would make
                  the schema self-contradictory.
                * ``"error"``: reject the write with a clear ``ValueError``
                  instead of evolving the schema. Nothing about the table
                  changes.

                Either way, a column both schemas already have, but with an
                incompatible type, always raises -- schema evolution here
                only ever *adds* columns, it never changes an existing
                column's type.
            name: Optional table name recorded in the Delta metadata (new tables).
            description: Optional table description recorded in the Delta metadata
                (new tables).
        """
        try:
            # ``SaveMode`` is a ``(str, Enum)``, so a plain ``"append"`` passes
            # membership checks but has no ``.value``. Normalize so downstream
            # code always has a real enum member.
            mode = SaveMode(mode)
        except ValueError:
            pass  # Not a valid SaveMode value at all; let the check below raise.

        if mode not in _SUPPORTED_MODES:
            raise ValueError(
                f"DeltaDatasink only supports "
                f"{sorted(m.value for m in _SUPPORTED_MODES)} in this "
                f"prototype, got {mode!r}."
            )
        if schema_mode not in _SUPPORTED_SCHEMA_MODES:
            raise ValueError(
                f"DeltaDatasink only supports schema_mode in "
                f"{sorted(_SUPPORTED_SCHEMA_MODES)}, got {schema_mode!r}."
            )

        self._path = path.rstrip("/")
        self._mode = mode
        self._partition_by = list(partition_by) if partition_by else []
        self._storage_options = dict(storage_options) if storage_options else None
        self._schema_mode = schema_mode
        self._name = name
        self._description = description
        # Captured from the first input bundle in ``on_write_start`` so the commit
        # has a schema even when the dataset is empty.
        self._schema: Optional["pa.Schema"] = None
        self._data_context = DataContext.get_current()

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        """Capture the dataset schema, and reconcile ``partition_by`` against
        an existing table's partition columns -- inheriting them when none
        were given, and rejecting a mismatch.

        Delta reads a partitioned column's value from ``AddAction`` metadata
        rather than the Parquet file, so writing a layout that disagrees with
        the table's ``partition_columns`` reads those values back as ``None``,
        or commits an unreadable version. Neither APPEND nor OVERWRITE resets
        a table's partitioning, and this prototype can't change it, so the
        only safe options are to match it or inherit it.

        This runs before any write task, both because workers need the
        partition columns up front and so a mismatch fails before anything has
        been written.
        """
        self._schema = schema

        from deltalake import DeltaTable

        if not DeltaTable.is_deltatable(
            self._path, storage_options=self._storage_options
        ):
            return

        existing_partition_by = list(
            DeltaTable(self._path, storage_options=self._storage_options)
            .metadata()
            .partition_columns
        )
        if not self._partition_by:
            self._partition_by = existing_partition_by
        elif self._partition_by != existing_partition_by:
            raise ValueError(
                f"Cannot write to Delta table '{self._path}': the table is "
                f"partitioned by {existing_partition_by}, but "
                f"partition_by={self._partition_by} was given. This prototype "
                "can't change an existing table's partition scheme -- pass the "
                "same columns in the same order, or omit partition_by to "
                "inherit the table's."
            )

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> DeltaWriteResult:
        """Write each block to its own Parquet file and return the resulting
        ``AddAction`` metadata plus the blocks' schemas.

        Runs on each worker. Only file metadata (not data) is sent to the
        driver, which unifies the schemas across all workers before committing.
        """
        add_actions: List["AddAction"] = []
        schemas: List["pa.Schema"] = []
        for block in blocks:
            table = BlockAccessor.for_block(block).to_arrow()
            if table.num_rows > 0:
                add_actions.extend(self._write_parquet(table, ctx))
                schemas.append(table.schema)

        return DeltaWriteResult(add_actions=add_actions, schemas=schemas)

    def on_write_complete(self, write_result: WriteResult[DeltaWriteResult]) -> None:
        """Commit all written files to the Delta log in a single transaction."""
        from deltalake import DeltaTable, Schema
        from deltalake.transaction import create_table_with_add_actions

        add_actions: List["AddAction"] = []
        schemas: List["pa.Schema"] = []
        for result in write_result.write_returns:
            if result is None:
                continue
            add_actions.extend(result.add_actions)
            schemas.extend(result.schemas)

        table_exists = DeltaTable.is_deltatable(
            self._path, storage_options=self._storage_options
        )
        delta_mode = self._mode.value

        if not add_actions and delta_mode == "append" and table_exists:
            logger.info(
                "No files to commit to Delta table '%s' (mode=append); skipping.",
                self._path,
            )
            return

        # ``self._schema`` (from ``on_write_start``) is only a fallback for a
        # dataset that produced no rows at all: for a file-listing-based read
        # it can be Ray Data's internal metadata schema rather than the row
        # schema, so real worker schemas always win.
        schema = unify_schemas(schemas) if schemas else self._schema
        if schema is None and delta_mode == "overwrite" and table_exists:
            # An empty OVERWRITE still truncates the table, so fall back to the
            # table's own schema rather than requiring one from the empty write.
            schema = (
                DeltaTable(self._path, storage_options=self._storage_options)
                .schema()
                .to_arrow()
            )
        if schema is None:
            raise ValueError(
                "Cannot write a Delta table without a schema: the dataset produced "
                "no data and no schema was provided."
            )

        if not table_exists:
            # Create a brand-new table with the initial set of files.
            create_table_with_add_actions(
                table_uri=self._path,
                schema=Schema.from_arrow(schema),
                add_actions=add_actions,
                mode="overwrite" if delta_mode == "overwrite" else "error",
                partition_by=self._partition_by or None,
                name=self._name,
                description=self._description,
                storage_options=self._storage_options,
            )
        else:
            dt = DeltaTable(self._path, storage_options=self._storage_options)
            if delta_mode == "append":
                dt = self._reconcile_schema_with_existing_table(schema, dt)
            dt.create_write_transaction(
                actions=add_actions,
                mode=delta_mode,
                schema=schema,
                partition_by=self._partition_by or None,
            )

        logger.info(
            "Committed %d file(s) to Delta table '%s' (mode=%s).",
            len(add_actions),
            self._path,
            delta_mode,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _reconcile_schema_with_existing_table(
        self, schema: "pa.Schema", dt: "DeltaTable"
    ) -> "DeltaTable":
        """Reconcile ``schema`` (the schema being committed) against ``dt``'s
        current table schema, per ``self._schema_mode``.

        A column in ``schema`` that ``dt`` doesn't have is either added to the
        table (``schema_mode="merge"``) or rejected (``schema_mode="error"``).
        A column both schemas have with an incompatible type always raises,
        regardless of ``schema_mode`` -- only adding columns is ever safe to
        do automatically.

        Returns the ``DeltaTable`` to commit against: ``dt`` itself, or a
        reloaded one if the schema was evolved.
        """
        import pyarrow as pa

        existing_schema = pa.schema(dt.schema().to_arrow())

        # Check types before evolving, so a write that both adds a column and
        # conflicts on an existing one fails without having already committed
        # the (permanent) schema change.
        #
        # ``unify_schemas`` reports an incompatibility as ArrowTypeError or
        # ArrowInvalid depending on the types involved, and can surface a few
        # unreconcilable shapes as KeyError/ValueError instead. The call below
        # is the only statement in the try, so any failure means these schemas
        # can't be unified -- wrap them all rather than leak a raw Arrow error.
        try:
            unify_schemas([existing_schema, schema])
        except Exception as e:
            raise ValueError(
                f"Cannot write to Delta table '{self._path}': the incoming "
                f"data's schema is not compatible with the table's existing "
                f"schema ({e}). schema_mode={self._schema_mode!r} only "
                "supports adding new columns, not changing an existing "
                "column's type."
            ) from e

        # Walk the incoming schema's field order, not a set difference: set
        # iteration order for strings is hash-randomized per process, which
        # would make the table's final column order vary between runs.
        existing_names = set(existing_schema.names)
        new_fields = [
            schema.field(name) for name in schema.names if name not in existing_names
        ]
        if new_fields:
            if self._schema_mode == "error":
                raise ValueError(
                    f"Cannot write to Delta table '{self._path}': column(s) "
                    f"{[f.name for f in new_fields]} are not present in the "
                    "table's existing schema. Pass schema_mode='merge' "
                    "(the default) to add new columns automatically, or "
                    "remove them from the incoming data."
                )
            dt = self._evolve_schema_for_new_columns(dt, new_fields)

        return dt

    def _evolve_schema_for_new_columns(
        self, dt: "DeltaTable", new_fields: List["pa.Field"]
    ) -> "DeltaTable":
        """Add ``new_fields`` to the table's schema in their own transaction
        and return a reloaded ``DeltaTable``.

        New columns are forced nullable: rows already in the table have no
        value for them, and ``alter.add_columns`` won't reject a non-nullable
        field on its own. ``alter.add_columns`` is also the only API that
        actually evolves the schema -- passing a wider ``schema=`` to the
        data-commit calls is silently ignored.
        """
        import pyarrow as pa
        from deltalake import DeltaTable, Schema as DeltaSchema

        nullable_fields = [f.with_nullable(True) for f in new_fields]
        delta_schema = DeltaSchema.from_arrow(pa.schema(nullable_fields))
        dt.alter.add_columns(delta_schema.fields)
        return DeltaTable(self._path, storage_options=self._storage_options)

    def _write_parquet(self, table: "pa.Table", ctx: TaskContext) -> List["AddAction"]:
        """Write a PyArrow table to Parquet under the table root, one file per
        partition, and return the corresponding ``AddAction`` metadata."""
        import pyarrow.dataset as pds
        import pyarrow.fs as pafs
        from deltalake.transaction import AddAction

        filesystem, root = pafs.FileSystem.from_uri(self._path)
        explicit_fs = _explicit_filesystem_from_storage_options(
            self._path, self._storage_options
        )
        if explicit_fs is not None:
            filesystem = explicit_fs
        modification_time = int(time.time() * 1000)
        # Unique prefix per write task so retries/concurrent tasks don't collide.
        task_idx = getattr(ctx, "task_idx", 0)
        write_uuid = uuid.uuid4().hex

        written: List["AddAction"] = []

        def _visit(written_file: "pds.WrittenFile") -> None:
            # The Delta log stores paths relative to the table root. Strip
            # leading slashes from both sides first: S3 can report a root
            # without one but written-file paths with one, and that mismatch
            # makes relpath walk upward into ``../../bucket/...``.
            rel_path = posixpath.relpath(
                written_file.path.lstrip("/"), root.lstrip("/")
            )
            partition_values = _parse_partition_values(rel_path, self._partition_by)
            metadata = written_file.metadata
            written.append(
                AddAction(
                    path=rel_path,
                    size=written_file.size,
                    partition_values=partition_values,
                    modification_time=modification_time,
                    data_change=True,
                    stats=json.dumps({"numRecords": metadata.num_rows}),
                )
            )

        partitioning = None
        if self._partition_by:
            # Check up front: ``Table.select`` would otherwise raise a bare
            # KeyError from inside a worker, naming neither the table nor the
            # fact that the partition columns may have been inherited.
            missing = [c for c in self._partition_by if c not in table.column_names]
            if missing:
                raise ValueError(
                    f"Cannot write to Delta table '{self._path}': the data "
                    f"being written is missing partition column(s) "
                    f"{missing}. The table is partitioned by "
                    f"{self._partition_by}, so every row written to it must "
                    "carry those columns."
                )
            partitioning = pds.partitioning(
                table.select(self._partition_by).schema, flavor="hive"
            )

        # Object stores have no real directories, so create_dir=True just makes
        # every task PutObject the same marker key -- enough concurrency against
        # a cold prefix to trigger S3 SLOW_DOWN. Real filesystems still need it.
        create_dir = not isinstance(
            filesystem, (pafs.S3FileSystem, pafs.GcsFileSystem, pafs.AzureFileSystem)
        )

        pds.write_dataset(
            table,
            base_dir=root,
            filesystem=filesystem,
            format="parquet",
            partitioning=partitioning,
            basename_template=f"part-{task_idx:05d}-{write_uuid}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
            file_visitor=_visit,
            create_dir=create_dir,
        )
        return written


def _explicit_filesystem_from_storage_options(
    path: str, storage_options: Optional[Dict[str, str]]
) -> Optional["pafs.FileSystem"]:
    """Build a credentialed PyArrow filesystem from ``storage_options`` for
    cloud paths, so worker-side Parquet writes use the same explicit
    credentials the driver's Delta commit already receives.

    ``pyarrow.fs.FileSystem.from_uri`` alone can't accept credentials, so
    without this an explicit ``storage_options=`` would only ever reach the
    driver-side ``DeltaTable``/``create_table_with_add_actions`` calls, while
    workers silently fell back to ambient/default credentials.

    Returns ``None`` when ``storage_options`` is empty/unset or doesn't
    match a recognized scheme, so the caller falls back to
    ``from_uri`` -- i.e. today's ambient-credential behavior is unchanged
    when no explicit ``storage_options`` are given.
    """
    import pyarrow.fs as pafs

    if not storage_options:
        return None
    path_lower = path.lower()
    if path_lower.startswith(("s3://", "s3a://")):
        access_key = storage_options.get("AWS_ACCESS_KEY_ID")
        secret_key = storage_options.get("AWS_SECRET_ACCESS_KEY")
        session_token = storage_options.get("AWS_SESSION_TOKEN")
        # Fall back to the boto-style AWS_DEFAULT_REGION, which the driver's
        # commit path already honors.
        region = storage_options.get("AWS_REGION") or storage_options.get(
            "AWS_DEFAULT_REGION"
        )
        # AWS_ENDPOINT_URL is deltalake's own key for a custom S3-compatible
        # endpoint (MinIO, moto). Without it, workers would target real AWS
        # while the driver's commit targets the custom endpoint.
        endpoint_url = storage_options.get("AWS_ENDPOINT_URL")
        if access_key or secret_key or session_token or region or endpoint_url:
            return pafs.S3FileSystem(
                access_key=access_key,
                secret_key=secret_key,
                session_token=session_token,
                region=region,
                endpoint_override=endpoint_url,
            )
    elif path_lower.startswith(("abfss://", "abfs://")):
        account_name = storage_options.get("AZURE_STORAGE_ACCOUNT_NAME")
        # Matches ray.data.catalog's vended-credential key (AZURE_STORAGE_SAS_TOKEN),
        # not a made-up name -- a mismatch here would leave workers uncredentialed
        # even when the driver's commit works fine.
        token = storage_options.get("AZURE_STORAGE_SAS_TOKEN")
        # An account key is a separate, mutually exclusive auth method from a
        # SAS token; without it a key-authenticated caller would get an
        # uncredentialed worker filesystem.
        account_key = storage_options.get("AZURE_STORAGE_ACCOUNT_KEY")
        if account_name and (token or account_key):
            return pafs.AzureFileSystem(
                account_name=account_name, sas_token=token, account_key=account_key
            )
        if account_name:
            return pafs.AzureFileSystem(account_name=account_name)
    elif path_lower.startswith(("gs://", "gcs://")):
        service_account = storage_options.get("GOOGLE_SERVICE_ACCOUNT")
        anonymous = storage_options.get("GOOGLE_ANONYMOUS", "").lower() == "true"
        if service_account or anonymous:
            if service_account:
                # GcsFileSystem has no constructor arg for a service-account
                # path -- it only reads GOOGLE_APPLICATION_CREDENTIALS. That's
                # process-wide, and Ray reuses worker processes across tasks,
                # so scope the mutation to this call and always restore it.
                import os

                key = "GOOGLE_APPLICATION_CREDENTIALS"
                with _gcs_env_lock:
                    previous = os.environ.get(key)
                    os.environ[key] = service_account
                    try:
                        return pafs.GcsFileSystem(anonymous=anonymous)
                    finally:
                        if previous is None:
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = previous
            return pafs.GcsFileSystem(anonymous=anonymous)
    return None


def _parse_partition_values(
    rel_path: str, partition_by: List[str]
) -> Dict[str, Optional[str]]:
    """Extract ``{col: value}`` from a hive-partitioned relative file path."""
    import urllib.parse

    if not partition_by:
        return {}
    values: Dict[str, Optional[str]] = {}
    for segment in rel_path.split("/")[:-1]:
        if "=" in segment:
            key, _, value = segment.partition("=")
            unquoted_key = urllib.parse.unquote(key)
            unquoted_value = urllib.parse.unquote(value)
            values[unquoted_key] = (
                None
                if unquoted_value == "__HIVE_DEFAULT_PARTITION__"
                else unquoted_value
            )
    return {col: values.get(col) for col in partition_by}
