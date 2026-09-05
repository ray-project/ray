import abc
import logging
import os
import posixpath
import sys
import time
from abc import abstractmethod
from typing import List, Optional, Tuple

import numpy as np
import pyarrow
import pyarrow.compute as pc
from pyarrow.fs import FileSelector, FileType

import ray
from ray._common.retry import call_with_retry
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_writer import PENDING_CHECKPOINT_SUFFIX
from ray.data.checkpoint.generated_id import (
    CHECKPOINTED_FILE_COLUMN_NAME,
    CHECKPOINTED_FILE_FRAGMENTS_TYPE,
    CHECKPOINTED_FRAGMENT_TYPE,
    CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA,
    FILE_NAME_FIELD,
    FRAGMENT_FIELD,
    NUM_FRAGMENTS_FIELD,
    NUM_ROWS_FIELD,
    PATH_PREFIX_FIELD,
    ROW_ID_FIELD,
    get_struct_field_index,
)
from ray.data.checkpoint.util import build_pending_checkpoint_trie
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Retry configuration for checkpoint recovery operations.
# These can be overridden via environment variables for testing or tuning.
CHECKPOINT_RECOVERY_MAX_ATTEMPTS = int(
    os.environ.get("RAY_DATA_CHECKPOINT_RECOVERY_MAX_ATTEMPTS", "3")
)
CHECKPOINT_RECOVERY_MAX_BACKOFF_S = int(
    os.environ.get("RAY_DATA_CHECKPOINT_RECOVERY_MAX_BACKOFF_S", "8")
)


def _numpy_size(array: np.ndarray) -> int:
    """Calculate the size of a numpy ndarray."""
    total_size = array.nbytes
    if array.dtype == object:
        sample_count = 10**4

        if len(array) <= sample_count:
            for item in array.flat:
                total_size += sys.getsizeof(item)
        else:
            sample_total_size = 0
            for item in array[:sample_count].flat:
                sample_total_size += sys.getsizeof(item)
            total_size += int(sample_total_size / sample_count * len(array))
    return total_size


@ray.remote(num_cpus=0)
def _clean_pending_checkpoints_task(
    checkpoint_path_unwrapped: str,
    checkpoint_filesystem: pyarrow.fs.FileSystem,
    data_file_dir_unwrapped: str,
    data_file_filesystem: pyarrow.fs.FileSystem,
) -> int:
    """Delete data files that have matching pending checkpoint files, then
    delete the pending checkpoints.

    This runs as a remote task to avoid blocking the driver during potentially
    slow filesystem operations (especially on cloud storage like S3).

    Algorithm:
    1. List all files in checkpoint dir, find those ending with .pending.parquet
    2. Build a PrefixTrie from their basenames (strip .pending.parquet)
    3. List all data files in data_file_dir (recursively for partitions)
    4. For each data file, if trie.has_prefix_of(basename) -> delete it
    5. Delete all the pending checkpoint files
    6. Return count of pending checkpoints cleaned

    Args:
        checkpoint_path_unwrapped: The unwrapped checkpoint path.
        checkpoint_filesystem: The filesystem for checkpoint files.
        data_file_dir_unwrapped: The unwrapped directory where data files are
            written (protocol prefix already stripped).
        data_file_filesystem: The filesystem for data files. May differ from
            checkpoint_filesystem (e.g., checkpoints on local disk, data on S3).

    Returns:
        Number of pending checkpoints cleaned.
    """

    def _clean() -> int:
        # 1. List all files in checkpoint dir, find pending ones
        ckpt_files = checkpoint_filesystem.get_file_info(
            FileSelector(
                checkpoint_path_unwrapped, recursive=False, allow_not_found=True
            )
        )
        pending_suffix = f"{PENDING_CHECKPOINT_SUFFIX}.parquet"
        pending_file_paths = [
            f
            for f in ckpt_files
            if f.type == FileType.File and f.path.endswith(pending_suffix)
        ]

        if not pending_file_paths:
            return 0

        # 2. Build prefix trie from pending checkpoint basenames
        trie = build_pending_checkpoint_trie(pending_file_paths, pending_suffix)

        # 3. List all data files (recursively for partitions)
        data_files = data_file_filesystem.get_file_info(
            FileSelector(data_file_dir_unwrapped, recursive=True, allow_not_found=True)
        )

        # 4. Delete data files matching a pending checkpoint prefix
        for f in data_files:
            if f.type != FileType.File:
                continue
            basename = posixpath.basename(f.path)
            if trie.has_prefix_of(basename):
                data_file_filesystem.delete_file(f.path)

        # 5. Delete all pending checkpoint files
        for f in pending_file_paths:
            checkpoint_filesystem.delete_file(f.path)

        return len(pending_file_paths)

    return call_with_retry(
        _clean,
        description="clean pending checkpoints",
        max_attempts=CHECKPOINT_RECOVERY_MAX_ATTEMPTS,
        max_backoff_s=CHECKPOINT_RECOVERY_MAX_BACKOFF_S,
    )


@ray.remote(num_returns=2)
def convert_and_sort_checkpointed_ids(
    checkpointed_ids_arrow: Block, id_column: str
) -> Tuple[np.ndarray, int]:
    """Convert checkpointed IDs from pyarrow.Table to sorted np.ndarray.

    Args:
        checkpointed_ids_arrow: A pyarrow.Table containing the checkpointed
            IDs, loaded from the checkpoint parquet files.
        id_column: The id column of `checkpoint_ids_array`.

    Returns:
        A tuple of:
        - The sorted checkpointed IDs of type numpy.ndarray, which can be
          passed directly to each checkpoint filter actor.
        - The size (bytes) of the ndarray, which can be used to determine
          the `ray_remote_args` of each checkpoint filter actor.
    """
    checkpointed_ids_ndarray = np.array([])

    try:
        if checkpointed_ids_arrow.num_rows != 0:
            checkpointed_ids_ndarray = np.sort(
                transform_pyarrow.to_numpy(
                    checkpointed_ids_arrow[id_column], zero_copy_only=False
                )
            )
    except Exception as e:
        raise RuntimeError(f"Failed to convert and sort checkpointed IDs: {e}")

    checkpoint_size = _numpy_size(checkpointed_ids_ndarray)
    return checkpointed_ids_ndarray, checkpoint_size


@DeveloperAPI
class CheckpointManager(abc.ABC):
    """Manage checkpoint data.

    Subclasses passed as ``CheckpointConfig.checkpoint_manager_cls`` must have
    a constructor accepting ``(checkpoint_config=..., data_context=...)``
    keyword arguments, and their ``load_checkpoint`` must return an
    ``(ObjectRef, int)`` tuple: the ref is passed opaquely to the configured
    ``CheckpointFilter`` class's constructor, and the int (size in bytes) is
    used for the per-actor memory reservation of the filter actors. Returning
    ``(None, 0)`` means there is no checkpoint data to restore from, and the
    checkpoint filter operator is not added to the plan.
    """

    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        data_context: DataContext,
    ):
        """Initialize the CheckpointManager.

        Args:
            checkpoint_config: the checkpoint config.
            data_context: the DataContext snapshot whose ``execution_options``
                should govern the Ray tasks fired during checkpoint loading
                and pending-checkpoint cleanup. Pass the dataset's
                ``_context`` (not ``DataContext.get_current()``) so the
                label_selector and other execution options stay consistent
                with the rest of materialize.
        """
        self.checkpoint_path = checkpoint_config.checkpoint_path
        self.filesystem = checkpoint_config.filesystem
        self.id_column = checkpoint_config.id_column
        self.checkpoint_path_partition_filter = (
            checkpoint_config.checkpoint_path_partition_filter
        )
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            checkpoint_config.checkpoint_path
        )
        self._data_context = data_context

    def load_checkpoint(
        self,
        data_file_dir: Optional[str] = None,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> Tuple[Optional[ObjectRef[np.ndarray]], int]:
        """Loading checkpoint data.

        This method first cleans up any pending checkpoints from incomplete
        2-phase commits, then loads the committed checkpoint data.

        Args:
            data_file_dir: Optional directory where data files are written.
                If provided, pending checkpoints will be used to find and
                delete matching data files before loading.
            data_file_filesystem: Optional filesystem for data files. If not
                provided, defaults to the checkpoint filesystem. Should be
                provided when data files are on a different filesystem than
                checkpoints.

        Returns:
            ObjectRef: The ref of checkpointed IDs array. None if no checkpoint was loaded.
            int: the size of the checkpointed IDs array.
        """
        start_t = time.time()

        loaded = self._load_checkpoint_block(data_file_dir, data_file_filesystem)
        if loaded is None:
            return None, 0
        block_ref, schema, _ = loaded

        # Convert arrow-typed ids to sorted numpy-typed ids.
        # Note: the convert is very time-consuming.
        # Get the object ref the checkpointed IDs, because we do not want the IDs
        # to occupy the memory of the head node.
        ctx_label_selector = self._data_context.execution_options.label_selector
        task = convert_and_sort_checkpointed_ids
        if ctx_label_selector:
            task = task.options(label_selector=ctx_label_selector)
        (
            checkpointed_ids_ref,
            checkpoint_size_ref,
        ) = task.remote(block_ref, self.id_column)

        checkpoint_size = ray.get(checkpoint_size_ref)

        logger.info(
            "Checkpoint loaded for %s in %.2f seconds. SizeBytes = %d, Schema = %s",
            type(self).__name__,
            time.time() - start_t,
            checkpoint_size,
            schema.to_string(),
        )
        return checkpointed_ids_ref, checkpoint_size

    def _load_checkpoint_block(
        self,
        data_file_dir: Optional[str] = None,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> Optional[Tuple[ObjectRef[Block], Schema, BlockMetadata]]:
        """Clean pending checkpoints, then load committed IDs as one block.

        Shared by :meth:`load_checkpoint` (which converts the block to a
        sorted numpy array for the actor-pool filter) and
        ``GeneratedIdColumnCheckpointManager.load_checkpoint_as_block``
        (which hands the compact block to ``ListFiles`` / the Parquet
        reader directly).

        Returns None when there is no committed checkpoint data.
        """
        logger.info(
            "Loading checkpoint from %s, this could take a while.", self.checkpoint_path
        )

        # Clean up pending checkpoints before loading (runs as a Ray task)
        if data_file_dir is not None:
            self._clean_pending_checkpoints(data_file_dir, data_file_filesystem)

        # If the checkpoint directory has no remaining data files (e.g., all
        # entries were pending checkpoints that were just cleaned up), skip
        # the inner ``read_parquet``. V2's ``read_parquet`` raises on empty
        # directories while V1 returned a zero-row dataset; this pre-check
        # keeps ``load_checkpoint`` behaving the same under both.
        # Recurse when a partition filter is configured because committed
        # files live under Hive-partitioned subdirectories rather than at
        # the top level.
        entries = self.filesystem.get_file_info(
            FileSelector(
                self.checkpoint_path_unwrapped,
                recursive=self.checkpoint_path_partition_filter is not None,
                allow_not_found=True,
            )
        )
        if not any(f.type == FileType.File for f in entries):
            return None

        # Load the checkpoint data
        checkpoint_ds: ray.data.Dataset = ray.data.read_parquet(
            self.checkpoint_path,
            filesystem=self.filesystem,
            partition_filter=self.checkpoint_path_partition_filter,
        )
        checkpoint_ds.set_name("checkpoint_dataset")

        # Manually disable checkpointing for loading the checkpoint metadata
        # to avoid recursively restoring checkpoints.
        # TODO: Clean way to do this would be to introduce per Op config
        # [https://github.com/ray-project/ray/issues/54520]
        checkpoint_ds.context.checkpoint_config = None

        # Pre-process data pipeline
        checkpoint_ds: ray.data.Dataset = self._preprocess_data_pipeline(checkpoint_ds)

        # Repartition to 1 block.
        checkpoint_ds = checkpoint_ds.repartition(num_blocks=1)

        # Get the block reference
        ref_bundles: List[RefBundle] = list(checkpoint_ds.iter_internal_ref_bundles())

        assert len(ref_bundles) == 1

        # If there are no valid files under the checkpoint_path, return None.
        if ref_bundles[0].num_rows() == 0:
            return None

        ref_bundle: RefBundle = ref_bundles[0]
        schema: Schema = ref_bundle.schema
        assert len(ref_bundle.blocks) == 1
        block_ref: ObjectRef[Block] = ref_bundle.blocks[0].ref
        metadata: BlockMetadata = ref_bundle.blocks[0].metadata
        # Validate the loaded checkpoint
        self._validate_loaded_checkpoint(schema, metadata)
        return block_ref, schema, metadata

    def _clean_pending_checkpoints(
        self,
        data_file_dir: Optional[str],
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> None:
        """Clean up pending checkpoints from incomplete 2-phase commits.

        Finds pending checkpoint files, builds a prefix trie from their basenames,
        deletes matching data files, then deletes the pending checkpoints.

        Runs as a Ray task to avoid blocking the driver during potentially
        slow filesystem operations (especially on cloud storage like S3).

        Args:
            data_file_dir: The directory where data files are written.
            data_file_filesystem: The filesystem for data files. If not
                provided, defaults to the checkpoint filesystem.
        """
        if not data_file_dir:
            return
        if data_file_filesystem is None:
            data_file_filesystem = self.filesystem
        ctx_label_selector = self._data_context.execution_options.label_selector
        task = _clean_pending_checkpoints_task
        if ctx_label_selector:
            task = task.options(label_selector=ctx_label_selector)
        try:
            cleaned_count = ray.get(
                task.remote(
                    self.checkpoint_path_unwrapped,
                    self.filesystem,
                    _unwrap_protocol(data_file_dir),
                    data_file_filesystem,
                )
            )
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} pending checkpoint(s)")
        except ray.exceptions.RayTaskError:
            logger.exception("Failed to clean up pending checkpoints")
            raise

    def _preprocess_data_pipeline(
        self, checkpoint_ds: ray.data.Dataset
    ) -> ray.data.Dataset:
        """Pre-process the checkpoint dataset.

        Subclasses can override this method for custom processing.
        """
        return checkpoint_ds

    def _validate_loaded_checkpoint(
        self, schema: Schema, metadata: BlockMetadata
    ) -> None:
        """Validate the loaded checkpoint. Subclasses can override for custom validation."""
        pass


@DeveloperAPI
class IdColumnCheckpointManager(CheckpointManager):
    """Manager for regular ID columns."""


@DeveloperAPI
class GeneratedIdColumnCheckpointManager(CheckpointManager):
    """Manager for auto-generated row-ID columns.

    Committed checkpoint files store one struct ID per output row. At load
    time this manager compacts them into one row per input file
    (``CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA``): the raw IDs are
    grouped by file, then per row group either marked fully checkpointed
    (empty ``checkpointed_row_ids`` list) or given a dense boolean mask of
    committed positions. ``ListFiles`` uses the compact block to drop
    fully-done files, and the Parquet reader uses it to skip fully-done row
    groups and filter partially-done ones — the actor-pool
    ``CheckpointFilter`` path is not used for generated IDs.
    """

    def _extract_grouping_fields(self, batch: pyarrow.Table) -> pyarrow.Table:
        """Project the struct ID column into groupable path columns."""
        id_col: pyarrow.ChunkedArray = batch[self.id_column]

        path_prefix_idx = get_struct_field_index(id_col, PATH_PREFIX_FIELD)
        path_prefix = pc.struct_field(id_col, [path_prefix_idx])

        file_name_idx = get_struct_field_index(id_col, FILE_NAME_FIELD)
        file_name = pc.struct_field(id_col, [file_name_idx])

        return pyarrow.Table.from_arrays(
            [
                path_prefix.cast(pyarrow.large_string()),
                file_name.cast(pyarrow.large_string()),
                batch[self.id_column],
            ],
            names=[PATH_PREFIX_FIELD, FILE_NAME_FIELD, self.id_column],
        )

    def _process_file_group(self, file_group_batch: pyarrow.Table) -> pyarrow.Table:
        """Compact one file's committed IDs into a single checkpoint row.

        Args:
            file_group_batch: Rows of a single ``(path_prefix, file_name)``
                group.

        Returns:
            One-row table with ``CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA``.
        """
        path_prefix = file_group_batch[PATH_PREFIX_FIELD][0].as_py()
        file_name = file_group_batch[FILE_NAME_FIELD][0].as_py()
        file_path = f"{path_prefix}/{file_name}"

        id_columns = file_group_batch[self.id_column]

        # NUM_FRAGMENTS is the file's total row-group count — identical for
        # every row of the file.
        num_fragments_field_idx = get_struct_field_index(
            id_columns[0], NUM_FRAGMENTS_FIELD
        )
        num_fragments = pc.struct_field(id_columns, [num_fragments_field_idx])[
            0
        ].as_py()

        fragment_field_idx = get_struct_field_index(id_columns[0], FRAGMENT_FIELD)
        fragments_array = pc.struct_field(id_columns, [fragment_field_idx])

        num_rows_field_idx = get_struct_field_index(id_columns[0], NUM_ROWS_FIELD)
        num_rows_array = pc.struct_field(id_columns, [num_rows_field_idx])

        row_id_field_idx = get_struct_field_index(id_columns[0], ROW_ID_FIELD)
        row_ids_array = pc.struct_field(id_columns, [row_id_field_idx])

        # Group committed row IDs by row group.
        fragment_table = pyarrow.table(
            {
                "fragment": fragments_array,
                "row_id": row_ids_array,
                "num_rows": num_rows_array,
            }
        )
        grouped = fragment_table.group_by("fragment").aggregate(
            [
                ("row_id", "list"),
                # num_rows is the same for all rows in a row group; min is
                # just a way to pick it.
                ("num_rows", "min"),
            ]
        )

        fragments_array = grouped["fragment"]
        row_ids_lists = grouped["row_id_list"]
        num_rows_array = grouped["num_rows_min"]

        checkpointed_row_counts = pc.cast(
            pc.list_value_length(row_ids_lists), pyarrow.int32()
        )
        fully_checkpointed_mask = pc.equal(checkpointed_row_counts, num_rows_array)
        num_fragments_fully_checkpointed = pc.sum(fully_checkpointed_mask).as_py() or 0

        # Per row group: empty list when fully committed, else a dense
        # boolean mask over the row group.
        checkpointed_row_ids_arrays = []
        for i in range(len(grouped)):
            num_rows = num_rows_array[i].as_py()
            checkpointed_row_count = checkpointed_row_counts[i].as_py()
            row_ids_list = row_ids_lists[i]

            if checkpointed_row_count == num_rows:
                checkpointed_row_ids_col = pyarrow.array(
                    [[]], type=pyarrow.large_list(pyarrow.bool_())
                )
            else:
                row_indices = pyarrow.array(np.arange(num_rows), type=pyarrow.int32())
                # ``pc.is_in`` requires a sorted value set.
                row_ids_values = row_ids_list.values
                sorted_row_ids = row_ids_values.take(pc.sort_indices(row_ids_values))
                boolean_array = pc.is_in(row_indices, sorted_row_ids)
                offsets = pyarrow.array([0, len(boolean_array)], type=pyarrow.int64())
                checkpointed_row_ids_col = pyarrow.LargeListArray.from_arrays(
                    offsets, boolean_array
                )

            checkpointed_row_ids_arrays.append(checkpointed_row_ids_col)

        if checkpointed_row_ids_arrays:
            checkpointed_row_ids_array = pyarrow.concat_arrays(
                checkpointed_row_ids_arrays
            )
        else:
            checkpointed_row_ids_array = pyarrow.array(
                [[]], type=pyarrow.large_list(pyarrow.bool_())
            )

        fragment_structs = pyarrow.StructArray.from_arrays(
            [
                pc.cast(fragments_array.combine_chunks(), pyarrow.int32()),
                pc.cast(num_rows_array.combine_chunks(), pyarrow.int32()),
                checkpointed_row_counts.combine_chunks(),
                checkpointed_row_ids_array,
            ],
            fields=list(CHECKPOINTED_FRAGMENT_TYPE),
        )

        if len(fragment_structs) > 0:
            offsets = pyarrow.array([0, len(fragment_structs)], type=pyarrow.int64())
            fragments_list = pyarrow.LargeListArray.from_arrays(
                offsets, fragment_structs
            )
        else:
            fragments_list = pyarrow.array(
                [[]], type=pyarrow.large_list(CHECKPOINTED_FRAGMENT_TYPE)
            )

        # The file is fully checkpointed only when every one of its row
        # groups was seen and fully committed.
        fully_checkpointed = num_fragments_fully_checkpointed == num_fragments
        checkpointed_fragment_col = pyarrow.StructArray.from_arrays(
            [
                pyarrow.array([len(fragment_structs)], type=pyarrow.int32()),
                pyarrow.array([fully_checkpointed], type=pyarrow.bool_()),
                fragments_list,
            ],
            fields=list(CHECKPOINTED_FILE_FRAGMENTS_TYPE),
        )

        logger.debug(
            "Compacted checkpoint for file %s: %d/%d row groups fully "
            "checkpointed, fully_checkpointed=%s",
            file_path,
            num_fragments_fully_checkpointed,
            num_fragments,
            fully_checkpointed,
        )

        return pyarrow.Table.from_arrays(
            [pyarrow.array([file_path]), checkpointed_fragment_col],
            schema=CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA,
        )

    def _preprocess_data_pipeline(
        self, checkpoint_ds: "ray.data.Dataset"
    ) -> "ray.data.Dataset":
        """Compact raw committed IDs into one row per file, sorted by path."""
        checkpoint_ds = checkpoint_ds.map_batches(
            self._extract_grouping_fields,
            batch_format="pyarrow",
            batch_size=None,
        )
        # The sort-based shuffle materializes every group on one node;
        # hash shuffle keeps the groupby streaming.
        checkpoint_ds.context._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

        checkpoint_ds = checkpoint_ds.groupby([PATH_PREFIX_FIELD, FILE_NAME_FIELD])
        checkpoint_ds = checkpoint_ds.map_groups(
            self._process_file_group, batch_format="pyarrow"
        )
        return checkpoint_ds.sort(CHECKPOINTED_FILE_COLUMN_NAME)

    def _validate_loaded_checkpoint(
        self, schema: Schema, metadata: BlockMetadata
    ) -> None:
        if metadata.num_rows > 0:
            assert schema == CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA, (
                f"Schema mismatch: {schema} != "
                f"{CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA}"
            )

    def load_checkpoint_as_block(
        self,
        data_file_dir: Optional[str] = None,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> ObjectRef[Block]:
        """Load the compact checkpoint as an ``ObjectRef[Block]``.

        Unlike :meth:`load_checkpoint`, the block is not converted to a
        numpy array — the struct-typed compact table is passed as a task
        kwarg to ``ListFiles`` and consumed as a pyarrow table. An empty
        (schema-less) table means there is no checkpoint data.
        """
        start_t = time.time()
        loaded = self._load_checkpoint_block(data_file_dir, data_file_filesystem)
        if loaded is None:
            return ray.put(pyarrow.table({}))
        block_ref, schema, _ = loaded
        logger.info(
            "Checkpoint loaded for %s in %.2f seconds. Schema = %s",
            type(self).__name__,
            time.time() - start_t,
            schema.to_string(),
        )
        return block_ref


def load_generated_id_checkpoint_as_block(
    config: CheckpointConfig,
    data_file_dir: Optional[str] = None,
    data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    data_context: Optional[DataContext] = None,
) -> ObjectRef[Block]:
    """Load the generated-ID checkpoint as a compact per-file Block.

    The returned block has ``CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA``
    and is consumed by ``ListFiles`` (file-level skipping) and the V2
    Parquet reader (row-group and row-level skipping). Regular ``id_column``
    checkpointing does not go through this path — it uses the actor-pool
    ``CheckpointFilter`` wired up by
    ``ray.data._internal.planner.checkpoint.create_checkpoint_filter_op``.
    """
    assert getattr(config, "generated_id_column", None), (
        "load_generated_id_checkpoint_as_block() is only for "
        "generated_id_column configs; id_column uses the actor-pool pattern."
    )
    manager = GeneratedIdColumnCheckpointManager(
        checkpoint_config=config,
        data_context=data_context or DataContext.get_current(),
    )
    return manager.load_checkpoint_as_block(data_file_dir, data_file_filesystem)


@DeveloperAPI
class CheckpointFilter(abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.

    Subclasses passed as ``CheckpointConfig.checkpoint_filter_cls`` are
    constructed with ``(checkpoint_config, checkpoint_ref)``, where
    ``checkpoint_ref`` is the ``ObjectRef`` returned by the checkpoint
    manager's ``load_checkpoint`` (by default, a sorted NumPy array of
    checkpointed IDs). Subclasses that define their own constructor must
    accept the same two arguments. The class is instantiated once per
    checkpoint filter actor on a remote worker, so it must be serializable
    (or importable) there.
    """

    def __init__(
        self,
        config: CheckpointConfig,
        checkpoint_ref: Optional[ObjectRef] = None,
    ):
        self.ckpt_config = config
        self.id_column = self.ckpt_config.id_column
        self.checkpoint_ref = checkpoint_ref

    @abstractmethod
    def filter_rows_for_block(self, block: Block) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Args:
            block: The input block to filter.
        Returns:
            A new block with rows that have not been checkpointed.
        """
        raise NotImplementedError


@DeveloperAPI
class NumpyArrayBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends.

    This filter will first fetch the checkpointed IDs (as NumPy arrays) from the object store.
    For each input block, it filters the block and returns the filtered block.
    """

    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        checkpoint_ref: ObjectRef[np.ndarray],
    ):
        super().__init__(checkpoint_config, checkpoint_ref)
        self.checkpointed_ids = ray.get(checkpoint_ref)
        assert isinstance(self.checkpointed_ids, np.ndarray)

    def filter_rows_for_block(
        self,
        block: Block,
    ) -> Block:
        """Filter IDs in memory using NumPy's binary search."""

        if self.checkpointed_ids.shape[0] == 0 or len(block) == 0:
            return block

        assert isinstance(block, pyarrow.Table)

        # The checkpointed_ids block is sorted (see load_checkpoint).
        # We'll use binary search to filter out processed rows.

        # Convert the block's ID column to a numpy array for fast processing.
        block_ids = transform_pyarrow.to_numpy(
            block[self.id_column], zero_copy_only=False
        )

        # Start with a mask of all True (keep all rows).
        mask = np.ones(len(block_ids), dtype=bool)
        # Use binary search to find where block_ids would be in ckpt_ids.
        sorted_indices = np.searchsorted(self.checkpointed_ids, block_ids)
        # Only consider indices that are within bounds.
        valid_indices = sorted_indices < len(self.checkpointed_ids)
        # For valid indices, check for exact matches.
        potential_matches = sorted_indices[valid_indices]
        matched = self.checkpointed_ids[potential_matches] == block_ids[valid_indices]
        # Mark matched IDs as False (filter out these rows).
        mask[valid_indices] = ~matched

        # Convert the final mask to a PyArrow array and filter the block.
        mask_array = pyarrow.array(mask)
        filtered_block = block.filter(mask_array)
        return filtered_block
