import logging
import os
import uuid
import hashlib
import json
from abc import abstractmethod

from pyarrow import parquet as pq

import ray

from ray.data._internal.util import call_with_retry
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


def _get_iceberg_checkpoint_committer_name(
    table_identifier: str, catalog_kwargs: dict
) -> str:
    catalog_key = json.dumps(catalog_kwargs or {}, sort_keys=True, default=str)
    digest = hashlib.sha1(f"{table_identifier}|{catalog_key}".encode("utf-8")).hexdigest()
    return f"ray_data_iceberg_ckpt_committer_{digest}"


@ray.remote
class _IcebergCheckpointCommitter:
    def __init__(self, table_identifier: str, catalog_kwargs: dict):
        from ray.data._internal.datasource.iceberg_datasink import IcebergDatasink

        self._table_identifier = table_identifier
        self._datasink = IcebergDatasink(
            table_identifier=table_identifier, catalog_kwargs=catalog_kwargs
        )
        self._initialized = False

    def ensure_table(self, schema):
        if self._initialized:
            return

        from pyiceberg.exceptions import TableAlreadyExistsError
        from pyiceberg.io import pyarrow as pyi_pa_io

        cat = self._datasink._get_catalog()
        exists = self._datasink._with_retry(
            lambda: cat.table_exists(self._table_identifier),
            description=f"check existence of Iceberg table '{self._table_identifier}'",
        )
        if not exists:
            iceberg_schema = pyi_pa_io.visit_pyarrow(
                schema, pyi_pa_io._ConvertToIcebergWithoutIDs()
            )
            try:
                self._datasink._with_retry(
                    lambda: cat.create_table(
                        self._table_identifier, schema=iceberg_schema
                    ),
                    description=f"create Iceberg table '{self._table_identifier}'",
                )
            except TableAlreadyExistsError:
                pass

        self._datasink.on_write_start(schema)
        self._initialized = True

    def commit(self, write_return, num_rows: int, size_bytes: int) -> None:
        from ray.data.datasource.datasink import WriteResult

        self._datasink._reload_table()
        self._datasink.on_write_complete(
            WriteResult(
                num_rows=num_rows,
                size_bytes=size_bytes,
                write_returns=[write_return],
            )
        )


class CheckpointWriter:
    """Abstract class which defines the interface for writing row-level
    checkpoints based on varying backends.

    Subclasses must implement `.write_block_checkpoint()`."""

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            self.ckpt_config.checkpoint_path
        )
        self.id_col = self.ckpt_config.id_column
        self.filesystem = self.ckpt_config.filesystem
        self.write_num_threads = self.ckpt_config.write_num_threads

    @abstractmethod
    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        ...

    @staticmethod
    def create(config: CheckpointConfig) -> "CheckpointWriter":
        """Factory method to create a `CheckpointWriter` based on the
        provided `CheckpointConfig`."""
        backend = config.backend

        if backend in [
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            CheckpointBackend.FILE_STORAGE,
        ]:
            return BatchBasedCheckpointWriter(config)
        if backend == CheckpointBackend.ICEBERG:
            return IcebergCheckpointWriter(config)
        raise NotImplementedError(f"Backend {backend} not implemented")


class IcebergCheckpointWriter(CheckpointWriter):
    """CheckpointWriter that writes to an Iceberg table."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)
        from ray.data._internal.datasource.iceberg_datasink import IcebergDatasink

        table_identifier = self.ckpt_config.checkpoint_path

        self.datasink = IcebergDatasink(
            table_identifier=table_identifier,
            catalog_kwargs=self.ckpt_config.catalog_kwargs,
        )
        self._table_identifier = table_identifier
        self._committer_name = _get_iceberg_checkpoint_committer_name(
            table_identifier, self.ckpt_config.catalog_kwargs
        )
        self._committer = None
        self._worker_initialized = False

    def _get_committer(self):
        if self._committer is not None:
            return self._committer

        try:
            self._committer = ray.get_actor(self._committer_name)
        except ValueError:
            try:
                self._committer = _IcebergCheckpointCommitter.options(
                    name=self._committer_name
                ).remote(self._table_identifier, self.ckpt_config.catalog_kwargs)
            except Exception:
                self._committer = ray.get_actor(self._committer_name)

        return self._committer

    def write_block_checkpoint(self, block: BlockAccessor):
        if block.num_rows() == 0:
            return

        checkpoint_ids_block = block.select(columns=[self.id_col])
        if not self._worker_initialized:
            schema = BlockAccessor.for_block(checkpoint_ids_block).to_arrow().schema
            committer = self._get_committer()
            ray.get(committer.ensure_table.remote(schema))
            self.datasink._reload_table()
            self._worker_initialized = True

        from ray.data._internal.execution.interfaces.task_context import TaskContext

        ctx = TaskContext(task_idx=0, op_name="checkpoint_write")
        write_result = self.datasink.write([checkpoint_ids_block], ctx)

        committer = self._get_committer()
        ckpt_accessor = BlockAccessor.for_block(checkpoint_ids_block)
        ray.get(
            committer.commit.remote(
                write_result, ckpt_accessor.num_rows(), ckpt_accessor.size_bytes()
            )
        )


class BatchBasedCheckpointWriter(CheckpointWriter):
    """CheckpointWriter for batch-based backends."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        self.filesystem.create_dir(self.checkpoint_path_unwrapped, recursive=True)

    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        if block.num_rows() == 0:
            return

        file_name = f"{uuid.uuid4()}.parquet"
        ckpt_file_path = os.path.join(self.checkpoint_path_unwrapped, file_name)

        checkpoint_ids_block = block.select(columns=[self.id_col])
        # `pyarrow.parquet.write_parquet` requires a PyArrow table. It errors if the block is
        # a pandas DataFrame.
        checkpoint_ids_table = BlockAccessor.for_block(checkpoint_ids_block).to_arrow()

        def _write():
            pq.write_table(
                checkpoint_ids_table,
                ckpt_file_path,
                filesystem=self.filesystem,
            )

        try:
            return call_with_retry(
                _write,
                description=f"Write checkpoint file: {file_name}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed: {file_name}")
            raise
