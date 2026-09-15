import json
import logging
import os
import posixpath
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

from ray._common.retry import call_with_retry
from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas
from ray.data._internal.cloud_auth import (
    AUTH_ERROR_PATTERNS,
    is_auth_error,
    restore_environ,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.path_util import _filesystem_root_from_uri
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.fs as pafs
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

    from ray.data.catalog import Catalog

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


def _nested_schema_additions(
    existing_field: "pa.Field",
    incoming_field: "pa.Field",
    *,
    path: Optional[str] = None,
) -> Tuple[Optional["pa.Field"], List[str]]:
    """Build a schema patch for fields added below an existing nested field.

    ``DeltaTable.alter.add_columns`` merges a partial struct/array/map field
    into the existing field. The patch therefore contains only new descendants,
    with each new field made nullable so rows written before the evolution
    remain valid.
    """
    import pyarrow as pa

    path = path or incoming_field.name
    existing_type = existing_field.type
    incoming_type = incoming_field.type

    if pa.types.is_struct(existing_type) and pa.types.is_struct(incoming_type):
        existing_children = {field.name: field for field in existing_type}
        patch_children = []
        added_paths = []
        for incoming_child in incoming_type:
            child_path = f"{path}.{incoming_child.name}"
            existing_child = existing_children.get(incoming_child.name)
            if existing_child is None:
                patch_children.append(incoming_child.with_nullable(True))
                added_paths.append(child_path)
                continue

            child_patch, child_paths = _nested_schema_additions(
                existing_child,
                incoming_child,
                path=child_path,
            )
            if child_patch is not None:
                patch_children.append(child_patch)
                added_paths.extend(child_paths)

        if not patch_children:
            return None, []
        patch_type = pa.struct(patch_children)
    elif pa.types.is_list(existing_type) and pa.types.is_list(incoming_type):
        value_patch, added_paths = _nested_schema_additions(
            existing_type.value_field,
            incoming_type.value_field,
            path=f"{path}[]",
        )
        if value_patch is None:
            return None, []
        patch_type = pa.list_(value_patch)
    elif pa.types.is_map(existing_type) and pa.types.is_map(incoming_type):
        item_patch, added_paths = _nested_schema_additions(
            existing_type.item_field,
            incoming_type.item_field,
            path=f"{path}{{}}",
        )
        if item_patch is None:
            return None, []
        patch_type = pa.map_(
            incoming_type.key_type,
            item_patch,
            keys_sorted=incoming_type.keys_sorted,
        )
    else:
        return None, []

    return existing_field.with_type(patch_type), added_paths


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
        A field present in the incoming data but not in the table is either
        added (``"merge"``, the default) or rejected (``"error"``). This
        includes fields nested inside structs, arrays, and maps. A field both
        schemas have with an incompatible type always raises.

        The schema is evolved in ``on_write_complete``, immediately before the
        data commit, so a failure anywhere earlier leaves the table's schema
        untouched. (Iceberg has to evolve in ``on_write_start`` instead,
        because PyIceberg binds field IDs at write time; Delta workers write
        plain Parquet and the schema is only established at commit.)

    Credential refresh on an authentication error is attempted on both the
    driver's commit and each worker's Parquet write:

    * With no ``catalog=`` and no explicit ``filesystem=``, a plain retry is
      already a refresh -- PyArrow and deltalake both re-resolve the standard
      cloud SDK credential chain on each new filesystem/``DeltaTable``.
    * With ``catalog=``, an auth error re-calls ``catalog.resolve(...)`` for a
      fresh vended credential. This works on the driver for any cloud the
      catalog supports. On a worker it only works when the catalog returns an
      explicit picklable filesystem (AWS today); for other credential shapes
      the auth error propagates as it did before.
    """

    def __init__(
        self,
        path: str,
        *,
        mode: SaveMode = SaveMode.APPEND,
        partition_by: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        schema_mode: str = "merge",
        user_storage_options: Optional[Dict[str, str]] = None,
        filesystem: Optional["pafs.FileSystem"] = None,
        catalog: Optional["Catalog"] = None,
        table_identifier: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """Initialize the DeltaDatasink.

        Args:
            path: URI of the Delta table (e.g. ``/tmp/my_table`` or
                ``s3://bucket/my_table``). If ``catalog`` is set, this is
                already the physical location the catalog resolved
                ``table_identifier`` to.
            mode: Write mode. Only ``SaveMode.APPEND`` and ``SaveMode.OVERWRITE``
                are supported in this prototype.
            partition_by: Optional list of columns to partition the table by.
            storage_options: Backend storage options forwarded to ``deltalake`` for
                the commit (e.g. cloud credentials). When ``catalog`` is set, this
                is already the catalog-resolved defaults merged with
                ``user_storage_options`` (the latter winning).
            schema_mode: How an ``APPEND`` reconciles its incoming schema
                against an existing table's schema, when the incoming data
                has a top-level or nested field the table doesn't. Has no effect on
                ``OVERWRITE`` (which always replaces the table's schema
                wholesale) or on a brand-new table (nothing to reconcile
                against yet). One of:

                * ``"merge"`` (default): add the new field(s) to the table
                  before committing, as an ``ALTER TABLE ADD COLUMN``-style
                  operation. New fields are always added as nullable --
                  every row already in the table has no value for a
                  brand-new field, so a non-nullable declaration would make
                  the schema self-contradictory.
                * ``"error"``: reject the write with a clear ``ValueError``
                  instead of evolving the schema. Nothing about the table
                  changes.

                Either way, a field both schemas already have, but with an
                incompatible type, always raises -- schema evolution here
                only ever *adds* fields, it never changes an existing
                field's type.
            user_storage_options: The exact storage_options the caller passed to
                :meth:`Dataset.write_delta` directly, before any catalog-resolved
                defaults were merged in. Kept separately so a credential refresh
                re-merges fresh catalog values with *these* (which always take
                precedence) instead of with whatever the previous, now-stale
                merge produced. Defaults to ``storage_options`` when not given
                (i.e. no catalog was involved, so there's nothing to distinguish).
            filesystem: Optional pre-built PyArrow filesystem used for both the
                driver commit's worker-visible writes and each worker's own
                Parquet write, instead of one built from ``storage_options`` or
                ambient credentials.
            catalog: Optional catalog used to resolve ``table_identifier`` to
                ``path`` (and to ``storage_options``/``filesystem``, if the
                catalog returns them). Kept as-is on this datasink -- including
                when the datasink is pickled to run on a worker -- so that a
                worker can call ``catalog.resolve(...)`` again on a
                credential-expiry error, without needing a round-trip to the
                driver. Requires ``table_identifier``.
            table_identifier: The catalog's name for the table (e.g.
                ``"main.schema.table"``), as opposed to ``path``, which is the
                physical location the catalog resolved it *to*. Both are needed
                because a credential refresh re-calls ``catalog.resolve()``,
                which takes the identifier -- a physical URI can't be resolved.
                Required with ``catalog``, and unused without one.
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
        if catalog is not None and not table_identifier:
            raise ValueError(
                "table_identifier is required with catalog: a credential "
                "refresh re-calls catalog.resolve() with it, and the catalog "
                "resolves identifiers (e.g. 'main.schema.table'), not the "
                "physical location it already resolved one to."
            )

        self._path = path.rstrip("/")
        self._mode = mode
        self._partition_by = list(partition_by) if partition_by else []
        self._storage_options = dict(storage_options) if storage_options else None
        self._schema_mode = schema_mode
        # Only the caller's *own* keys belong here -- these are re-applied on
        # top of every catalog refresh, so anything the catalog itself vended
        # must be excluded or a stale value (e.g. the first session token)
        # would keep winning and make each refresh a no-op for exactly the
        # keys that needed to change. Empty when the caller supplied none,
        # rather than falling back to ``storage_options``: by the time a
        # catalog is involved that dict is already catalog-merged.
        self._user_storage_options = (
            dict(user_storage_options) if user_storage_options else {}
        )
        self._filesystem = filesystem
        self._catalog = catalog
        # No fallback to ``path``: the two are different kinds of string (a
        # catalog identifier vs. a physical location) and only the identifier
        # can be re-resolved, so defaulting one to the other would just defer
        # the failure into the refresh path as ``resolve(<physical URI>)``.
        self._table_identifier = table_identifier
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

        def _read_existing_partition_by() -> Optional[List[str]]:
            """The table's partition columns, or None if it doesn't exist yet."""
            if not DeltaTable.is_deltatable(
                self._path, storage_options=self._storage_options
            ):
                return None
            return list(
                DeltaTable(self._path, storage_options=self._storage_options)
                .metadata()
                .partition_columns
            )

        # Retried and credential-refreshed like the commit's own calls in
        # ``on_write_complete``: these are the same kind of driver-side Delta
        # log reads, so an expired token or a throttled request here would
        # otherwise fail the whole job before any refresh could happen. The
        # validation below stays outside the retry -- a partition mismatch is a
        # logical error that must not be retried.
        existing_partition_by = self._with_retry(
            _read_existing_partition_by,
            description=f"read partition columns of Delta table '{self._path}'",
            refresh=self._refresh_driver_filesystem,
        )
        if existing_partition_by is None:
            return

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
        for block_idx, block in enumerate(blocks):
            table = BlockAccessor.for_block(block).to_arrow()
            if table.num_rows > 0:
                add_actions.extend(self._write_parquet(table, ctx, block_idx))
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

        table_exists = self._with_retry(
            lambda: DeltaTable.is_deltatable(
                self._path, storage_options=self._storage_options
            ),
            description=f"check whether a Delta table exists at '{self._path}'",
            refresh=self._refresh_driver_filesystem,
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
            schema = self._with_retry(
                lambda: DeltaTable(self._path, storage_options=self._storage_options)
                .schema()
                .to_arrow(),
                description=f"read the existing schema of Delta table '{self._path}'",
                refresh=self._refresh_driver_filesystem,
            )
        if schema is None:
            raise ValueError(
                "Cannot write a Delta table without a schema: the dataset produced "
                "no data and no schema was provided."
            )

        if not table_exists:
            # Create a brand-new table with the initial set of files.
            self._with_retry(
                lambda: create_table_with_add_actions(
                    table_uri=self._path,
                    schema=Schema.from_arrow(schema),
                    add_actions=add_actions,
                    mode="overwrite" if delta_mode == "overwrite" else "error",
                    partition_by=self._partition_by or None,
                    name=self._name,
                    description=self._description,
                    storage_options=self._storage_options,
                ),
                description=f"create Delta table at '{self._path}'",
                refresh=self._refresh_driver_filesystem,
            )
        else:
            # `schema` is bound as a default argument (evaluated once, here)
            # rather than captured via closure, so its already-narrowed
            # non-None type (checked above) carries into `_commit` instead of
            # widening back to `Optional[pa.Schema]` across the closure
            # boundary.
            def _commit(schema: "pa.Schema" = schema) -> None:
                dt = DeltaTable(self._path, storage_options=self._storage_options)
                if delta_mode == "append":
                    dt = self._reconcile_schema_with_existing_table(schema, dt)
                dt.create_write_transaction(
                    actions=add_actions,
                    mode=delta_mode,
                    schema=schema,
                    partition_by=self._partition_by or None,
                )

            self._with_retry(
                _commit,
                description=f"commit to Delta table at '{self._path}'",
                refresh=self._refresh_driver_filesystem,
            )

        logger.info(
            "Committed %d file(s) to Delta table '%s' (mode=%s).",
            len(add_actions),
            self._path,
            delta_mode,
        )

    # ------------------------------------------------------------------
    # Driver-side retry / credential refresh.
    # ------------------------------------------------------------------
    def _refresh_driver_filesystem(self) -> bool:
        """Re-resolve credentials via the catalog on a driver-side auth error.

        Returns ``True`` if a refresh was attempted, ``False`` if there's
        nothing this datasink knows how to do beyond a plain retry.
        """
        catalog = self._catalog
        table_identifier = self._table_identifier
        # ``__init__`` rejects a catalog without an identifier, so this is
        # belt-and-braces -- but it keeps the precondition visible right where
        # the identifier is used rather than only at construction.
        if catalog is None or table_identifier is None:
            return False

        from ray.data.catalog import CatalogAccessMode, ReaderFormat

        resolved = catalog.resolve(
            table_identifier,
            reader=ReaderFormat.DELTA,
            mode=CatalogAccessMode.WRITE,
        )
        # `catalog.resolve()` also writes freshly-vended credentials into this
        # process's own environment as a side effect (e.g.
        # `DatabricksUnityCatalog._apply_env`) -- that's what the deltalake
        # calls in `on_write_complete` actually pick up, for any cloud the
        # catalog supports, since delta-rs resolves credentials from the
        # environment on each call rather than caching them at import time.
        # The explicit fields below are applied too, for completeness, but
        # aren't required for the driver's own commit to succeed.
        if resolved.storage_options:
            # Merge with the caller's *original* storage_options
            # (`_user_storage_options`), not the current (possibly now-stale)
            # `_storage_options` -- otherwise a stale value for a key the
            # fresh resolve() just updated (e.g. a session token) would keep
            # winning the merge on every subsequent refresh, making the
            # refresh a no-op for exactly the fields that needed to change.
            self._storage_options = {
                **resolved.storage_options,
                **self._user_storage_options,
            }
        if resolved.filesystem is not None:
            self._filesystem = resolved.filesystem
        logger.info(
            "Refreshed Delta write credentials for '%s' via catalog after an "
            "auth error.",
            self._path,
        )
        return True

    def _with_retry(
        self,
        func: Callable[[], Any],
        description: str,
        *,
        refresh: Optional[Callable[[], bool]] = None,
        retry_auth_errors: bool = True,
    ) -> Any:
        """Retry ``func``, refreshing credentials first on an auth error.

        Used from both sides of the write: the driver's Delta log reads and
        commit, and each worker's Parquet write.

        Args:
            func: The operation to run and retry.
            description: Passed to ``call_with_retry`` for its log messages.
            refresh: Called on an auth error before the next attempt. Each
                implementation decides for itself whether a refresh is possible
                and no-ops if not, so there's no need to pre-check here.
            retry_auth_errors: Whether an auth error is worth retrying at all.
                ``False`` when nothing about the next attempt could differ --
                otherwise it just fails identically ``commit_max_attempts``
                times over.

        Returns:
            Whatever ``func`` returns on the attempt that succeeds.
        """
        cfg = self._data_context.delta_config
        retried_errors = list(cfg.commit_retried_errors)
        if cfg.credential_refresh_enabled and retry_auth_errors:
            retried_errors = retried_errors + AUTH_ERROR_PATTERNS

        def wrapped() -> Any:
            try:
                return func()
            except Exception as e:
                if (
                    refresh is not None
                    and cfg.credential_refresh_enabled
                    and is_auth_error(e)
                ):
                    refresh()
                raise

        return call_with_retry(
            wrapped,
            description=description,
            match=retried_errors,
            max_attempts=cfg.commit_max_attempts,
            max_backoff_s=cfg.commit_retry_max_backoff_s,
        )

    # ------------------------------------------------------------------
    # Worker-side retry / credential refresh.
    # ------------------------------------------------------------------
    def _resolve_worker_filesystem(self) -> "pafs.FileSystem":
        """Build the filesystem a worker should use for its Parquet write."""
        import pyarrow.fs as pafs

        if self._filesystem is not None:
            return self._filesystem
        explicit_fs = _explicit_filesystem_from_storage_options(
            self._path, self._storage_options
        )
        if explicit_fs is not None:
            return explicit_fs
        fs, _ = pafs.FileSystem.from_uri(self._path)
        return fs

    def _can_refresh_worker_credentials(self) -> bool:
        """Whether a worker can safely re-resolve catalog credentials.

        Only when the catalog already handed this datasink an explicit,
        picklable filesystem -- the AWS-backed Delta write shape. Since
        ``write_delta`` rejects a user-supplied ``filesystem=`` alongside
        ``catalog=``, a non-``None`` filesystem here can only have come from
        the catalog.

        This must be decided *without* calling ``resolve()``, because
        ``resolve()`` itself has credential-delivery side effects: Unity
        Catalog's ``_apply_env`` writes the vended credentials into this
        process's environment and never restores them. For a shape a worker
        can't use anyway (Azure, which has no explicit-filesystem path
        today), calling ``resolve()`` just to discover that would leave live
        credentials in a worker process that Ray then reuses for unrelated
        tasks.
        """
        return self._catalog is not None and self._filesystem is not None

    def _worker_retry_can_change_credentials(self) -> bool:
        """Whether retrying a worker-side auth error could plausibly succeed.

        An auth failure repeated against the same credentials fails
        identically, so retrying one is only worth the backoff when the next
        attempt can present different credentials.
        """
        if self._can_refresh_worker_credentials():
            # The catalog vends a fresh credential on each resolve().
            return True
        if self._catalog is not None:
            # A catalog whose credential shape a worker can't rebuild from --
            # see ``_can_refresh_worker_credentials``.
            return False
        # No catalog: each attempt re-runs ``_resolve_worker_filesystem``,
        # which rebuilds the filesystem and so re-resolves the ambient cloud
        # SDK credential chain -- unless a filesystem object was handed in, in
        # which case every attempt reuses that same object and nothing can
        # change. (A ``storage_options`` dict carrying a full static credential
        # set is a middle ground: the rebuild re-reads the same fixed keys.
        # Left as retryable because partial options -- region or endpoint only
        # -- still leave the credential chain to be resolved at construction.)
        return self._filesystem is None

    def _refresh_worker_filesystem(self) -> bool:
        """Re-resolve credentials via the catalog on a worker-side auth error.

        Scoped to the shape ``_can_refresh_worker_credentials`` describes; for
        anything else this is a no-op returning ``False`` and the caller lets
        the auth error propagate, same as before this feature existed.
        """
        catalog = self._catalog
        table_identifier = self._table_identifier
        if (
            catalog is None
            or table_identifier is None
            or not self._can_refresh_worker_credentials()
        ):
            return False

        from ray.data.catalog import CatalogAccessMode, ReaderFormat

        # ``resolve()`` may also seed the vended credentials into this
        # process's environment without restoring them (Unity Catalog's
        # ``_apply_env``). A worker process outlives this task and gets reused
        # for unrelated ones, so undo whatever it changed -- the refreshed
        # filesystem below carries the credentials we actually write with, so
        # nothing downstream needs them in the environment.
        env_before = dict(os.environ)
        try:
            resolved = catalog.resolve(
                table_identifier,
                reader=ReaderFormat.DELTA,
                mode=CatalogAccessMode.WRITE,
            )
        finally:
            restore_environ(env_before)

        if resolved.filesystem is None:
            return False
        self._filesystem = resolved.filesystem
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _reconcile_schema_with_existing_table(
        self, schema: "pa.Schema", dt: "DeltaTable"
    ) -> "DeltaTable":
        """Reconcile ``schema`` (the schema being committed) against ``dt``'s
        current table schema, per ``self._schema_mode``.

        A field in ``schema`` that ``dt`` doesn't have is either added to the
        table (``schema_mode="merge"``) or rejected (``schema_mode="error"``).
        A field both schemas have with an incompatible type always raises,
        regardless of ``schema_mode`` -- only adding fields is ever safe to
        do automatically.

        Returns the ``DeltaTable`` to commit against: ``dt`` itself, or a
        reloaded one if the schema was evolved.
        """
        import pyarrow as pa

        existing_schema = pa.schema(dt.schema().to_arrow())

        # Check types before evolving, so a write that both adds a field and
        # conflicts with an existing one fails without having already committed
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
                "supports adding new fields, not changing an existing "
                "field's type."
            ) from e

        # Walk the incoming schema's field order, not a set difference: set
        # iteration order for strings is hash-randomized per process, which
        # would make the table's final column order vary between runs.
        existing_names = set(existing_schema.names)
        new_fields = []
        new_field_paths = []
        for name in schema.names:
            incoming_field = schema.field(name)
            if name not in existing_names:
                # Existing rows have no value for a new top-level field.
                new_fields.append(incoming_field.with_nullable(True))
                new_field_paths.append(name)
                continue

            nested_patch, nested_paths = _nested_schema_additions(
                existing_schema.field(name),
                incoming_field,
            )
            if nested_patch is not None:
                new_fields.append(nested_patch)
                new_field_paths.extend(nested_paths)

        if new_fields:
            if self._schema_mode == "error":
                raise ValueError(
                    f"Cannot write to Delta table '{self._path}': field(s) "
                    f"{new_field_paths} are not present in the "
                    "table's existing schema. Pass schema_mode='merge' "
                    "(the default) to add new fields automatically, or "
                    "remove them from the incoming data."
                )
            dt = self._evolve_schema_for_new_fields(dt, new_fields)

        return dt

    def _evolve_schema_for_new_fields(
        self, dt: "DeltaTable", new_fields: List["pa.Field"]
    ) -> "DeltaTable":
        """Add ``new_fields`` to the table's schema in their own transaction
        and return a reloaded ``DeltaTable``.

        ``new_fields`` are already prepared as nullable top-level fields or
        partial nested patches whose new descendants are nullable. This keeps
        rows already in the table valid. ``alter.add_columns`` is also the only
        API that actually evolves the schema -- passing a wider ``schema=`` to
        the data-commit calls is silently ignored.
        """
        import pyarrow as pa
        from deltalake import DeltaTable, Schema as DeltaSchema

        delta_schema = DeltaSchema.from_arrow(pa.schema(new_fields))
        dt.alter.add_columns(delta_schema.fields)
        return DeltaTable(self._path, storage_options=self._storage_options)

    def _write_parquet(
        self, table: "pa.Table", ctx: TaskContext, block_idx: int = 0
    ) -> List["AddAction"]:
        """Write a PyArrow table to Parquet under the table root, one file per
        partition, and return the corresponding ``AddAction`` metadata.

        ``block_idx`` distinguishes the blocks of one write task from each
        other in the output filenames -- see the ``write_uuid`` comment below.
        It defaults to 0 for direct single-block calls.
        """
        import pyarrow.dataset as pds
        import pyarrow.fs as pafs
        from deltalake.transaction import AddAction

        # Path only -- the filesystem comes from ``_resolve_worker_filesystem``
        # below. See ``_filesystem_root_from_uri`` for why ``FileSystem.from_uri`` can't be
        # used to derive it.
        root = _filesystem_root_from_uri(self._path)
        modification_time = int(time.time() * 1000)
        task_idx = getattr(ctx, "task_idx", 0)
        # Fixed once per write job at plan time, so a retry reuses the same
        # filenames instead of leaking orphans under a fresh random name. The
        # fallback only applies to direct/test calls.
        #
        # Because it's job-level rather than per-call, it can't distinguish the
        # blocks within one task on its own -- ``basename_template``'s ``{i}``
        # restarts at 0 on every ``write_dataset`` call, so two blocks would
        # both claim ``...-0.parquet`` and the second would overwrite the first
        # (``existing_data_behavior="overwrite_or_ignore"``). ``block_idx``
        # below is what keeps them apart; it's stable across retries because a
        # retried task re-iterates the same blocks in the same order.
        write_uuid = (
            getattr(ctx, "kwargs", {}).get(WRITE_UUID_KWARG_NAME) or uuid.uuid4().hex
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

        def _do_write() -> List["AddAction"]:
            written: List["AddAction"] = []

            def _visit(written_file: "pds.WrittenFile") -> None:
                # The Delta log stores paths relative to the table root. Strip
                # leading slashes from both sides first: S3 can report a root
                # without one but written-file paths with one, and that
                # mismatch makes relpath walk upward into ``../../bucket/...``.
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

            filesystem = self._resolve_worker_filesystem()
            # Object stores have no real directories, so create_dir=True just
            # makes every task PutObject the same marker key -- enough
            # concurrency against a cold prefix to trigger S3 SLOW_DOWN. Real
            # filesystems still need it.
            create_dir = not isinstance(
                filesystem,
                (pafs.S3FileSystem, pafs.GcsFileSystem, pafs.AzureFileSystem),
            )

            pds.write_dataset(
                table,
                base_dir=root,
                filesystem=filesystem,
                format="parquet",
                partitioning=partitioning,
                basename_template=(
                    f"part-{task_idx:05d}-{block_idx:05d}-{write_uuid}-{{i}}.parquet"
                ),
                existing_data_behavior="overwrite_or_ignore",
                file_visitor=_visit,
                create_dir=create_dir,
            )
            return written

        return self._with_retry(
            _do_write,
            description=f"write Parquet files to '{root}'",
            refresh=self._refresh_worker_filesystem,
            # A worker that can't rebuild its filesystem with different
            # credentials gains nothing from retrying an auth error.
            retry_auth_errors=self._worker_retry_can_change_credentials(),
        )


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
