import logging
import uuid
from concurrent.futures import ThreadPoolExecutor

import pyarrow
import pyarrow.csv as pcsv
from pyarrow.fs import FileSelector, S3FileSystem

from ray.anyscale.data.checkpoint.interfaces import (
    BatchBasedCheckpointFilter,
    CheckpointConfig,
    CheckpointWriter,
    S3CheckpointIO,
)
from ray.data import DataContext
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)


class S3CheckpointFilter(BatchBasedCheckpointFilter, S3CheckpointIO):
    """CheckpointFilter implementation for S3 backend, reading all
    checkpoint files into object store prior to filtering rows."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        if self.fs is None:
            self.fs = S3FileSystem()

    def load_checkpoint(self) -> Block:
        def read_checkpoint_file(file) -> Block:
            if file.path.endswith(".csv"):
                with self.fs.open_input_file(file.path) as f:
                    try:
                        return pcsv.read_csv(f)
                    except pyarrow.lib.ArrowInvalid as e:
                        if "Empty CSV file" in str(e):
                            return None
                        raise e
            return None

        file_paths = FileSelector(
            f"{self.output_path}",
            recursive=True,
            allow_not_found=True,
        )
        file_info = self.fs.get_file_info(file_paths)

        id_blocks = []
        with ThreadPoolExecutor(max_workers=self.filter_num_threads) as executor:
            id_blocks = [
                block
                for block in executor.map(read_checkpoint_file, file_info)
                if block is not None
            ]

        builder = DelegatingBlockBuilder()
        for id_block in id_blocks:
            builder.add_block(id_block)
        return builder.build()

    def delete_checkpoint(self):
        self.fs.delete_dir(self.output_path)


class S3CheckpointWriter(CheckpointWriter, S3CheckpointIO):
    """CheckpointWriter implementation for S3 backend, writing one
    checkpoint file per output block written."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        if self.fs is None:
            self.fs = S3FileSystem()

    def write_block_checkpoint(self, block: BlockAccessor):
        # Write a single checkpoint file for the block being written.
        # The file is a CSV file with name `f"{uuid.uuid4()}.csv"`
        # with a single column of row IDs in the block.

        if block.num_rows() == 0:
            return

        split_bucket = self.output_path.split("/")
        bucket, key_prefix = split_bucket[0], "/".join(split_bucket[1:])
        file_id = uuid.uuid4()
        file_key = f"{key_prefix}/{file_id}.csv"

        # The checkpoint file contains a single column of the row IDs.
        checkpoint_ids_block = block.select(columns=[self.id_col])

        def _write():
            # TODO: add some checkpoint metadata, like timestamp, etc. in Body
            with self.fs.open_output_stream(f"{bucket}/{file_key}") as out_stream:
                pcsv.write_csv(checkpoint_ids_block, out_stream)

        try:
            return call_with_retry(
                _write,
                description=f"Write checkpoint file with UUID: {file_id}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed for UUID: {file_id}")
            raise
