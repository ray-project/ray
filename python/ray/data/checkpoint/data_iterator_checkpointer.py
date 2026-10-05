import abc
import logging
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, fields
from queue import Queue
from typing import Any, ClassVar, Dict, List, Optional

import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq

import ray
from ray.data._internal.arrow_ops.transform_pyarrow import deepcopy_array
from ray.data._internal.block_batching.interfaces import Batch
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.interfaces import DatasetCheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource import PartitionStyle, PathPartitionFilter

logger = logging.getLogger(__name__)


class DataIteratorCheckpointer(abc.ABC):
    """Abstract base class for data iterator checkpointers.

    Here's how this class should hook into an example training loop:

    load_checkpoint(...)             # (1)
    for epoch in range(num_epochs):  # (2)
        for batch in iterator:       # (3)
            train_step(model, batch)
            save_checkpoint(model)   # (4)

    (1) Load the checkpoint. Here is where the data iterator state should be loaded.
    (2) End the previous epoch and start a new one. Notify the checkpointer of these events.
    (3) Train on a new batch. The checkpointer should record the yielded batch.
    (4) Save the checkpoint. Here is where the data iterator state should be saved
        (e.g. by calling `state_dict`) along with the user's model state.

    Args:
        world_rank: The world rank of the current worker if running in a distributed
            setting (e.g. with Ray Train).
        world_size: The world size of the current distributed setting if running in a
            distributed setting (e.g. with Ray Train).
    """

    def __init__(
        self,
        world_rank: int = 0,
        world_size: int = 1,
    ):
        self._world_rank = world_rank
        self._world_size = world_size

    @abc.abstractmethod
    def state_dict(self) -> Dict[str, Any]:
        """Returns the current data iterator state."""
        ...

    def record_yielded_batch(self, batch: Batch) -> None:
        """Record that the iterator yielded a batch.

        This should have as little overhead as possible, since the user iteration
        loop calls this method repeatedly. Any significant overhead here will
        impact the performance of the user iteration loop.

        NOTE: The user is responsible for ensuring that the batch is consumed
        before calling `state_dict`, since we assume that all yielded batches
        are consumed at that point. For example, the user should pass all yielded
        batches to the model before checkpointing model and data iterator state.

        Args:
            batch: The batch yielded by the iterator.
        """
        ...

    def start_epoch(self) -> None:
        """Records that a new epoch has started.

        Performs any setup needed for the new epoch.
        """
        ...

    def end_epoch(self) -> None:
        """Records that the current epoch has ended.

        Performs any cleanup needed for the current epoch.
        """
        ...

    @property
    def world_rank(self) -> int:
        """The world rank of the current worker.
        Defaults to 0 if not running in a distributed setting."""
        return self._world_rank

    @property
    def world_size(self) -> int:
        """The world size of the current distributed setting.
        Defaults to 1 if not running in a distributed setting."""
        return self._world_size


@dataclass
class RowIDBasedStateDict:
    """State dict for a row-based data iterator checkpointer.

    Attributes:
        epoch_idx: The index of the current epoch.
        checkpoint_idx: The index of the current checkpoint.
        root_checkpoint_path: The path to the root checkpoint directory.
        id_column: The ID column name.
    """

    epoch_idx: int
    checkpoint_idx: int
    root_checkpoint_path: str
    id_column: str

    # Hive-style partition keys.
    EPOCH_PATH_KEY: ClassVar[str] = "epoch"
    CHECKPOINT_IDX_PATH_KEY: ClassVar[str] = "checkpoint"
    RANK_PATH_KEY: ClassVar[str] = "rank"

    STATE_DICT_TYPE: ClassVar[str] = "row_id_based"
    STATE_DICT_VERSION: ClassVar[int] = 0

    def should_restore(self) -> bool:
        """Whether the state dict was captured mid-epoch and warrants restoring.
        `checkpoint_idx=-1` indicates that the state dict was captured at
        an epoch boundary, in which case we do not load any checkpoint files
        and start a new epoch.
        """
        return self.checkpoint_idx >= 0

    @property
    def restoration_checkpoint_path_filter(self) -> PathPartitionFilter:
        """Filter for checkpoint files to load during mid-epoch restoration.
        Returns a filter that matches all checkpoint files for this state dict.
        """
        return PathPartitionFilter.of(
            filter_fn=lambda partitioned_path: (
                int(partitioned_path[self.EPOCH_PATH_KEY]) == self.epoch_idx
                and int(partitioned_path[self.CHECKPOINT_IDX_PATH_KEY])
                <= self.checkpoint_idx
            ),
            style=PartitionStyle.HIVE,
        )

    @classmethod
    def from_dict(cls, state_dict: Dict[str, Any]) -> "RowIDBasedStateDict":
        required_keys = {f.name for f in fields(cls)}
        missing_keys = required_keys - state_dict.keys()
        if missing_keys:
            raise ValueError(
                f"Invalid state dict: {state_dict}. Missing keys: {missing_keys}"
            )

        return cls(**{k: state_dict[k] for k in required_keys})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "epoch_idx": self.epoch_idx,
            "checkpoint_idx": self.checkpoint_idx,
            "root_checkpoint_path": self.root_checkpoint_path,
            "id_column": self.id_column,
            # Also include some checkpoint metadata for backwards compatibility.
            "metadata": {
                "state_dict_type": self.STATE_DICT_TYPE,
                "state_dict_version": self.STATE_DICT_VERSION,
                "ray_commit": ray.__commit__,
                "ray_version": ray.__version__,
            },
        }


class RowIDBasedDataIteratorCheckpointer(DataIteratorCheckpointer):
    """Data iterator checkpointer that records yielded row IDs as checkpoint files.

    This checkpointer supports data iterators that yield batches of data, where
    the data has a column with a unique ID for each row. The checkpointer records
    the row IDs of the yielded batches and flushes them asynchronously to a parquet file.

    Upon calling `state_dict`, the checkpointer performs a blocking flush of any
    outstanding row IDs to a checkpoint file.

    Checkpoint files are stored in a hive-style partitioning format:
    {checkpoint_path}/rank={r}/epoch={x}/checkpoint={y}/chunk_{z}.parquet

    The epoch tracks the number of times iterator has reset (and called `start_epoch`).
    The checkpoint index tracks the number of committed data checkpoints so far,
    where each committed checkpoint corresponds to a call to `state_dict`.
    The chunk index tracks the number of chunks written to the current checkpoint directory.

    The rank index tracks the world rank of the current worker to avoid directory collisions.
    Setting the top-level directory to the rank allows each worker to manage a separate
    set of checkpoint files, which simplifies directory management.

    Here's an example of the checkpoint directory structure:

    /tmp/iterator_checkpoint_test/
    └── rank=0
        ├── epoch=0
        │   ├── checkpoint=0
        │   │   ├── chunk_0.parquet
        │   │   └── chunk_1.parquet
        │   ├── checkpoint=1
        │   │   └── chunk_0.parquet
        │   └── checkpoint=2
        │       └── chunk_0.parquet
        └── epoch=1
            ├── checkpoint=0
            │   └── chunk_0.parquet
            └── checkpoint=1
                └── chunk_0.parquet
    └── rank=1
        ├── epoch=0
        │   ├── checkpoint=0
        │   │   ├── chunk_0.parquet
        │   │   └── chunk_1.parquet
        │   ├── checkpoint=1
        │   │   └── chunk_0.parquet
        │   └── checkpoint=2
        │       └── chunk_0.parquet
        └── epoch=1
            ├── checkpoint=0
            │   └── chunk_0.parquet
            └── checkpoint=1
                └── chunk_0.parquet

    Calling `state_dict` triggers an on-demand snapshot of the data iterator state.
    For example: {"epoch": 1, "checkpoint_idx": 2, "epoch_running": True}.

    Upon restoring from this state dict, configure the base dataset to restore
    from a subset of the checkpoint files by using the ordering of the directory
    structure. For example, only read from directories matching this filter:
    `epoch=1/checkpoint<=2/`. Note that this dataset configuration based on a state
    dict is not in scope for this class.

    Args:
        checkpoint_config: The checkpoint configuration for the data iterator.
        world_rank: The world rank of the current worker if running in a distributed
            setting (e.g. with Ray Train).
        world_size: The world size of the current distributed setting if running in a
            distributed setting (e.g. with Ray Train).
        state_dict: The state dict to initialize the checkpointer from.
    """

    TARGET_CHECKPOINT_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB

    def __init__(
        self,
        checkpoint_config: DatasetCheckpointConfig,
        world_rank: int = 0,
        world_size: int = 1,
        state_dict: Optional[RowIDBasedStateDict] = None,
    ):
        super().__init__(world_rank=world_rank, world_size=world_size)

        if not checkpoint_config.checkpoint_path:
            raise ValueError(
                "`checkpoint_path` must be set to enable data iterator "
                "checkpointing. Ray Train sets it by default to "
                "`{RunConfig.storage_path}/{RunConfig.name}/ray_data_checkpoints`."
            )

        self._checkpoint_config = checkpoint_config
        self._id_column = checkpoint_config.id_column
        if checkpoint_config.override_filesystem:
            self._fs = checkpoint_config.override_filesystem
            self._checkpoint_path_unwrapped = checkpoint_config.checkpoint_path
        else:
            self._fs, self._checkpoint_path_unwrapped = pyarrow.fs.FileSystem.from_uri(
                checkpoint_config.checkpoint_path
            )

        # TODO(xgui): _epoch_idx should not be tracked here, instead it should be retrieved from the base dataset.
        self._epoch_idx = 0
        self._latest_committed_checkpoint_idx = -1
        self._epoch_running = False

        # Index for tracking the current checkpoint file to write.
        self._chunk_idx = -1

        # Whether the state dict should be updated on the next `state_dict` call.
        self._should_update_state_dict = False

        # Queue for staging row IDs.
        self._row_ids_staging_queue: Optional[Queue[Optional[Block]]] = None
        # Background thread for flushing row IDs to a checkpoint file.
        self._flush_thread: Optional[threading.Thread] = None
        # Event for signaling that a forced flush has completed.
        self._flush_completed_event: Optional[threading.Event] = None
        # Exception raised during a flush operation.
        self._flush_exception: Optional[Exception] = None

        # Deletion threadpool.
        self._delete_threadpool: Optional[ThreadPoolExecutor] = None
        # Task for deleting completed epoch checkpoints.
        self._delete_task: Optional[Future] = None

        # Whether the thread attributes have been initialized.
        self._threads_initialized = False

        # Load state dict
        if state_dict:
            self._epoch_idx = state_dict.epoch_idx
            self._latest_committed_checkpoint_idx = state_dict.checkpoint_idx

    def _init_threads(self):
        """Initialize background threads for flushing row IDs to checkpoint files
        and deleting completed epoch checkpoints.

        Delay the thread initialization until the start of iteration
        in order to keep this class serializable.
        """
        if self._threads_initialized:
            return

        self._row_ids_staging_queue: Queue[Optional[Block]] = Queue()
        self._flush_thread = threading.Thread(
            target=self._process_row_ids_from_queue,
            daemon=True,
            name="DataIteratorCheckpointer-flush",
        )
        self._flush_completed_event = threading.Event()
        self._flush_exception: Optional[Exception] = None
        self._flush_thread.start()

        self._delete_threadpool = ThreadPoolExecutor(max_workers=1)

        self._threads_initialized = True

    def _get_current_checkpoint_directory(self) -> str:
        """Get the current checkpoint directory where files are written.

        Example: {checkpoint_path}/rank={r}/epoch={x}/checkpoint={y}
        """
        # TODO: handle multiple datasets (add the dataset name to the path)
        return os.path.join(
            self._checkpoint_path_unwrapped,
            f"{RowIDBasedStateDict.RANK_PATH_KEY}={self.world_rank}",
            f"{RowIDBasedStateDict.EPOCH_PATH_KEY}={self._epoch_idx}",
            f"{RowIDBasedStateDict.CHECKPOINT_IDX_PATH_KEY}={self._current_checkpoint_idx}",
        )

    @property
    def _current_checkpoint_idx(self) -> int:
        """The current checkpoint index.

        This is one greater than the latest committed checkpoint index
        which points to a finalized directory, compared to the current directory
        where we still write files."""
        return self._latest_committed_checkpoint_idx + 1

    def _get_current_checkpoint_path(self) -> str:
        """Get the current checkpoint file to store row IDs.

        Example: {checkpoint_path}/rank={r}/epoch={x}/checkpoint={y}/chunk_{z}.parquet
        """
        return os.path.join(
            self._get_current_checkpoint_directory(),
            f"chunk_{self._chunk_idx}.parquet",
        )

    def _flush_row_ids(self, row_ids_batches: List[pa.Array]):
        """Flush staged row IDs to a checkpoint file."""
        assert self._threads_initialized, "Threads not initialized."

        if not row_ids_batches:
            raise ValueError(
                "Got an empty list of row IDs batches. "
                "This method must be called with at least one batch of row IDs."
            )

        self._chunk_idx += 1
        checkpoint_path = self._get_current_checkpoint_path()

        row_ids = pa.chunked_array(row_ids_batches)
        row_ids_table = pa.table({self._id_column: row_ids})

        def _write():
            pq.write_table(row_ids_table, checkpoint_path, filesystem=self._fs)

        try:
            call_with_retry(
                _write,
                description=f"Write checkpoint file: {checkpoint_path}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception as e:
            logger.exception(f"Failed to write checkpoint file: {checkpoint_path}")
            self._flush_exception = e

    def _process_row_ids_from_queue(self):
        """Process recorded row IDs from the staging queue.

        This method should run in a background thread and is responsible for
        flushing staged row IDs to a checkpoint file when reaching the target
        file-size or when the main thread requests a forced flush.
        """
        staged_row_ids_size_bytes = 0
        staged_row_id_batches: List[pa.Array] = []

        try:
            while True:
                row_ids = self._row_ids_staging_queue.get()
                if row_ids is None:
                    # Sentinel value `None` indicates that we should force flush any
                    # staged row IDs.
                    if staged_row_id_batches:
                        self._flush_row_ids(staged_row_id_batches)
                    staged_row_ids_size_bytes = 0
                    staged_row_id_batches = []
                    # Notify the main thread that the flush is complete.
                    self._flush_completed_event.set()
                    continue

                row_ids_accessor = BlockAccessor.for_block(row_ids)
                row_ids_array: pa.ChunkedArray = row_ids_accessor.to_arrow().column(
                    self._id_column
                )
                # Create an independent copy of the row IDs array to allow
                # garbage collection of the original table.
                # Without this copy, the original table would be kept
                # in memory until all batches of row IDs pointing to it are flushed.
                row_ids_array: pa.Array = deepcopy_array(row_ids_array)
                size_bytes = row_ids_accessor.size_bytes()

                staged_row_ids_size_bytes += size_bytes
                staged_row_id_batches.append(row_ids_array)
                if staged_row_ids_size_bytes >= self.TARGET_CHECKPOINT_SIZE_BYTES:
                    self._flush_row_ids(staged_row_id_batches)
                    staged_row_ids_size_bytes = 0
                    staged_row_id_batches = []
        except Exception as e:
            logger.exception("Internal error in background flush thread:")
            self._flush_exception = e
            self._flush_completed_event.set()
            raise

    def _raise_if_flush_failed(self):
        if self._flush_exception:
            raise RuntimeError(
                "Failed to flush one or more checkpoint files. "
                "If this error is retryable, add the error message prefix to: "
                "`ray.data.DataContext.get_current().retried_io_errors`"
            ) from self._flush_exception

    def _flush_all_staged_row_ids(self):
        """Force flush all staged row IDs to a checkpoint file.

        Raises an exception if a previous flush operation failed.
        """
        assert self._threads_initialized, "Threads not initialized."

        # The flush thread may have failed from a periodic flush.
        self._raise_if_flush_failed()

        self._flush_completed_event.clear()
        self._row_ids_staging_queue.put(None)
        self._flush_completed_event.wait()

        # The flush thread may have failed from the latest forced flush.
        self._raise_if_flush_failed()

    def record_yielded_batch(self, batch: Batch):
        assert (
            self._epoch_running
        ), "Must call `start_epoch` before recording yielded batches."
        assert self._threads_initialized, "Threads not initialized."

        self._raise_if_flush_failed()

        assert batch.metadata.row_ids is not None, batch.metadata

        self._should_update_state_dict = True
        self._row_ids_staging_queue.put(batch.metadata.row_ids)

    def start_epoch(self):
        if self._epoch_running:
            # The previous epoch did not complete, so we need to end it
            # before starting a new one.
            logger.warning(
                "[DataIteratorCheckpointer] Detected that a new epoch was "
                "started before the previous one completed. "
                "This usually occurs when the dataset iterator is re-created "
                "before the previous epoch has completed. "
                "As a result, the checkpointer is moving to a new epoch. "
                "To avoid this warning, ensure the dataset is fully consumed "
                "before starting a new iterator."
            )
            self.end_epoch()

        # Create the checkpoint directory.
        checkpoint_dir = self._get_current_checkpoint_directory()
        self._setup_new_checkpoint_directory(checkpoint_dir)

        self._init_threads()

        self._epoch_running = True

    def end_epoch(self):
        assert self._epoch_running, "Must call `start_epoch` before ending an epoch."

        self._epoch_running = False

        # Make sure any staged row IDs are flushed before ending the epoch.
        self._flush_all_staged_row_ids()

        if self._checkpoint_config.delete_checkpoints_after_epoch:
            self._delete_completed_epoch_checkpoints(epoch_idx=self._epoch_idx)

        self._epoch_idx += 1
        self._latest_committed_checkpoint_idx = -1
        self._should_update_state_dict = False

    def state_dict(self) -> Dict[str, Any]:
        # TODO: make this a barrier across train workers

        if self._should_update_state_dict:
            # Force flush and wait for completion
            self._flush_all_staged_row_ids()

            self._latest_committed_checkpoint_idx += 1
            self._chunk_idx = -1

            # Create the next checkpoint directory.
            new_checkpoint_dir = self._get_current_checkpoint_directory()
            self._setup_new_checkpoint_directory(new_checkpoint_dir)

            self._should_update_state_dict = False

        state_dict = RowIDBasedStateDict(
            epoch_idx=self._epoch_idx,
            checkpoint_idx=self._latest_committed_checkpoint_idx,
            root_checkpoint_path=self._checkpoint_path_unwrapped,
            id_column=self._id_column,
        ).to_dict()
        return state_dict

    def _setup_new_checkpoint_directory(self, new_checkpoint_dir: str):
        """Setup a new checkpoint directory.

        Deletes a partially written checkpoint directory from a previous run.
        """
        from ray.train.v2._internal.execution.storage import (
            _create_directory,
            _exists_at_fs_path,
            delete_fs_path,
        )

        if _exists_at_fs_path(self._fs, new_checkpoint_dir):
            logger.info(
                f"Found an existing directory at {new_checkpoint_dir}. "
                "Deleting this directory before writing new checkpoint files."
            )
            delete_fs_path(self._fs, new_checkpoint_dir)

        _create_directory(self._fs, new_checkpoint_dir)

    def _delete_completed_epoch_checkpoints(self, epoch_idx: int):
        """Delete checkpoint files for the completed epoch.

        Every worker is responsible for deleting its own checkpoint files."""
        from ray.train.v2._internal.execution.storage import delete_fs_path

        fs = self._fs
        checkpoint_path = os.path.join(
            self._checkpoint_path_unwrapped,
            f"rank={self.world_rank}",
            f"epoch={epoch_idx}",
        )

        def _delete_checkpoint_files():
            try:
                delete_fs_path(fs, checkpoint_path)
            except Exception:
                logger.exception(
                    f"Failed to delete checkpoint files: {checkpoint_path}"
                )
                raise

        # Wait for the previous delete task to complete.
        if self._delete_task:
            self._delete_task.result()

        self._delete_task = self._delete_threadpool.submit(_delete_checkpoint_files)

    def __del__(self):
        if self._delete_threadpool:
            self._delete_threadpool.shutdown(wait=True)
