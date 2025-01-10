import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

from pyarrow.fs import FileSelector, S3FileSystem

from ray.anyscale.data.checkpoint.interfaces import (
    CheckpointConfig,
    CheckpointWriter,
    RowBasedCheckpointFilter,
    S3CheckpointIO,
)
from ray.data import DataContext
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)


class RowBasedS3CheckpointFilter(RowBasedCheckpointFilter, S3CheckpointIO):
    """CheckpointFilter implementation for S3 backend, reading
    one checkpoint file per input row.

    For a more efficient implementation, see `S3CheckpointFilter`."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        if self.fs is None:
            self.fs = S3FileSystem()

    def filter_rows_for_block(self, block: Block) -> Block:
        block_accessor = BlockAccessor.for_block(block)
        files = []
        for row in block_accessor.iter_rows(False):
            _id = row[self.id_col]
            files.append(f"{_id}.jsonl")

        mask_file_exists = self.check_files_exist(files)
        builder = DelegatingBlockBuilder()
        for idx, row in enumerate(block_accessor.iter_rows(False)):
            ckpt_file_key = files[idx]
            if not mask_file_exists[ckpt_file_key]:
                builder.add(row)
        filtered_block = builder.build()
        return filtered_block

    def check_files_exist(self, files) -> Dict[str, bool]:
        # Mapping of {file_key -> whether checkpoint file exists or not}
        mask_file_exists = {f_name: False for f_name in files}

        def _get_file_info():
            return self.fs.get_file_info(
                FileSelector(self.output_path, allow_not_found=True)
            )

        files = call_with_retry(
            _get_file_info,
            description=f"Get file info: {files}",
            match=DataContext.get_current().retried_io_errors,
        )

        def _update_mask(file_info):
            if file_info.is_file and file_info.base_name in mask_file_exists:
                mask_file_exists[file_info.base_name] = True

        with ThreadPoolExecutor(max_workers=self.filter_num_threads) as executor:
            list(executor.map(_update_mask, files))

        return mask_file_exists


class RowBasedS3CheckpointWriter(CheckpointWriter, S3CheckpointIO):
    """CheckpointWriter implementation for S3 backend, writing
    one checkpoint file per input row.

    For a more efficient implementation, see `S3CheckpointWriter`."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        if self.fs is None:
            self.fs = S3FileSystem()

    def write_row_checkpoint(self, row: Dict[str, Any]):
        """Write a checkpoint for a single row to the checkpoint
        output directory given by `self.output_path`.

        The name of the checkpoint file is `f"{row[self.id_col]}.jsonl"`."""

        split_bucket = self.output_path.split("/")
        bucket, key_prefix = split_bucket[0], "/".join(split_bucket[1:])
        row_id = row[self.id_col]
        file_key = f"{key_prefix}/{row_id}.jsonl"

        def _write():
            # TODO: add some checkpoint metadata, like timestamp, etc. in Body
            with self.fs.open_output_stream(f"{bucket}/{file_key}"):
                pass
            return row_id

        try:
            return call_with_retry(
                _write,
                description=f"Write checkpoint file for ID: {row_id}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed for ID {row_id}")
            raise

    def write_block_checkpoint(self, block: BlockAccessor):
        with ThreadPoolExecutor(max_workers=self.write_num_threads) as executor:
            futures = []
            future_ids = []
            for row in block.iter_rows(public_row_format=False):
                futures.append(executor.submit(self.write_row_checkpoint, row))
                future_ids.append(row[self.id_col])

            completed_ids = []
            for result in as_completed(futures):
                completed_ids.append(result.result())

            # Verify that all checkpoints were written successfully.
            assert set(future_ids) == set(completed_ids), (
                f"Checkpoint writes failed for rows with IDs: "
                f"{set(future_ids) - set(completed_ids)}"
            )
