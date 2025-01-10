import glob
import logging
import os
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.csv as pcsv

from ray.anyscale.data.checkpoint.interfaces import (
    BatchBasedCheckpointFilter,
    CheckpointConfig,
    CheckpointWriter,
    DiskCheckpointIO,
)
from ray.data import DataContext
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)


class DiskCheckpointFilter(BatchBasedCheckpointFilter, DiskCheckpointIO):
    """CheckpointFilter implementation for disk backend, reading all
    checkpoint files into object store prior to filtering rows."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        if not os.path.exists(config.output_path):
            logger.warning(
                f"Checkpoint directory {config.output_path} does not exist. "
                f"No rows will be skipped from checkpoint filtering. "
                f"Ensure that the correct checkpoint directory was configured."
            )

    def load_checkpoint(self) -> Block:
        checkpoint_fpaths = glob.glob(f"{self.output_path}/*.csv")
        if not checkpoint_fpaths:
            empty_block = pa.Table.from_pylist([])
            return empty_block

        def read_file(fpath) -> Block:
            return pcsv.read_csv(fpath)

        with ThreadPoolExecutor() as executor:
            id_blocks = list(executor.map(read_file, checkpoint_fpaths))

        builder = DelegatingBlockBuilder()
        for id_block in id_blocks:
            builder.add_block(id_block)
        return builder.build()

    def delete_checkpoint(self):
        shutil.rmtree(self.output_path)


class DiskCheckpointWriter(CheckpointWriter, DiskCheckpointIO):
    """CheckpointWriter implementation for disk backend, writing one
    checkpoint file per output block written."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        # If the checkpoint output directory does not exist, create it.
        if not os.path.exists(config.output_path):
            os.makedirs(config.output_path)

    def write_block_checkpoint(self, block: BlockAccessor):
        # Write a single checkpoint file for the block being written.
        # The file is a CSV file with name `f"{uuid.uuid4()}.csv"`
        # with a single column of row IDs in the block.
        if block.num_rows() == 0:
            return

        file_name = f"{uuid.uuid4()}.csv"
        ckpt_file_path = os.path.join(self.output_path, file_name)

        checkpoint_ids_block = block.select(columns=[self.id_col])

        def _write():
            pcsv.write_csv(checkpoint_ids_block, ckpt_file_path)

        try:
            return call_with_retry(
                _write,
                description=f"Write checkpoint file: {file_name}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed: {file_name}")
            raise
