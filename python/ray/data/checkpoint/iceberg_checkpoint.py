"""Iceberg-specific checkpoint logic.

This module contains checkpoint utilities that are specific to Iceberg datasink.
It is separate from the generic checkpoint modules (checkpoint_writer.py,
checkpoint_filter.py) to maintain clean separation of concerns:
- Generic checkpoint modules handle ID-column-based checkpoint read/write
- This module handles Iceberg WriteResult metadata persistence and recovery

The planner layer (plan_write_op.py) orchestrates calling these functions
at the right points in the write pipeline.
"""

import logging
import os
import pickle
from typing import Any, List, Optional, Set

import pyarrow.parquet as pq
from pyarrow.fs import FileSelector, FileType

from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


class IcebergCheckpointLoader:
    """Loads IcebergWriteResult metadata from checkpoint .meta.pkl files.

    Checkpoint files are paired: each checkpoint_id has a .parquet file
    (containing checkpointed row IDs) and a .meta.pkl file (containing
    the serialized IcebergWriteResult). The loader reads .meta.pkl files
    to recover write results for Iceberg commit retry.

    The pairing scheme:
        {checkpoint_id}.parquet   -- row ID checkpoint (generic)
        {checkpoint_id}.meta.pkl  -- IcebergWriteResult (Iceberg-specific)
    """

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path_unwrapped = _unwrap_protocol(config.checkpoint_path)
        self.filesystem = config.filesystem

    def load_write_results(self) -> List[Any]:
        """Load all IcebergWriteResult objects from checkpoint .meta.pkl files.

        Only loads .meta.pkl files that have a matching .parquet file,
        ensuring the checkpoint was fully written (both ID data and metadata).

        Returns:
            List of deserialized IcebergWriteResult objects.
        """
        file_infos = self.filesystem.get_file_info(
            FileSelector(self.checkpoint_path_unwrapped, recursive=False)
        )

        parquet_ids: Set[str] = set()
        meta_paths: List[str] = []

        for f in file_infos:
            if f.type != FileType.File:
                continue
            basename = os.path.basename(f.path)
            if basename.endswith(".parquet") and not basename.endswith(
                ".pending.parquet"
            ):
                checkpoint_id = basename[: -len(".parquet")]
                parquet_ids.add(checkpoint_id)
            elif basename.endswith(".meta.pkl"):
                meta_paths.append(f.path)

        results = []
        for meta_path in meta_paths:
            basename = os.path.basename(meta_path)
            checkpoint_id = basename[: -len(".meta.pkl")]

            if checkpoint_id not in parquet_ids:
                logger.warning(
                    "Skipping orphaned .meta.pkl without matching .parquet: %s",
                    meta_path,
                )
                continue

            try:
                with self.filesystem.open_input_stream(meta_path) as f:
                    result = pickle.loads(f.readall())
                results.append(result)
            except Exception:
                logger.exception(
                    "Failed to load checkpoint metadata: %s", meta_path
                )
                raise

        logger.info(
            "Loaded %d IcebergWriteResult(s) from checkpoint: %s",
            len(results),
            self.checkpoint_path_unwrapped,
        )
        return results

    def get_checkpoint_ids(self, id_col: str) -> Set:
        """Load all checkpointed row IDs from .parquet files.

        Args:
            id_col: The name of the ID column in checkpoint parquet files.

        Returns:
            Set of all checkpointed row IDs.
        """
        file_infos = self.filesystem.get_file_info(
            FileSelector(self.checkpoint_path_unwrapped, recursive=False)
        )

        all_ids = set()
        for f in file_infos:
            if f.type != FileType.File:
                continue
            basename = os.path.basename(f.path)
            if basename.endswith(".parquet") and not basename.endswith(
                ".pending.parquet"
            ):
                table = pq.read_table(
                    f.path, filesystem=self.filesystem, columns=[id_col]
                )
                all_ids.update(table[id_col].to_pylist())

        return all_ids


def write_iceberg_checkpoint_metadata(
    filesystem,
    checkpoint_path_unwrapped: str,
    file_id: str,
    write_result: Any,
) -> None:
    """Atomically write an IcebergWriteResult as a .meta.pkl checkpoint file.

    Uses a tmp file + move pattern to ensure atomicity.

    Args:
        filesystem: PyArrow filesystem to write to.
        checkpoint_path_unwrapped: Unwrapped checkpoint directory path.
        file_id: The checkpoint file ID (shared with the .parquet file).
        write_result: The IcebergWriteResult object to serialize.
    """
    meta_file_name = f"{file_id}.meta.pkl"
    meta_path = os.path.join(checkpoint_path_unwrapped, meta_file_name)
    tmp_meta_path = os.path.join(
        checkpoint_path_unwrapped, f".tmp_{file_id}_{os.getpid()}.meta.pkl"
    )

    try:
        serialized = pickle.dumps(write_result)
        with filesystem.open_output_stream(tmp_meta_path) as f:
            f.write(serialized)
        filesystem.move(tmp_meta_path, meta_path)
    except Exception:
        try:
            info = filesystem.get_file_info(tmp_meta_path)
            if info.type != FileType.NotFound:
                filesystem.delete_file(tmp_meta_path)
        except Exception:
            pass
        raise


def merge_recovered_iceberg_write_results(
    write_returns: list,
    checkpoint_config: Optional[CheckpointConfig],
) -> list:
    """Load previously checkpointed IcebergWriteResults and merge with current results.

    Deduplicates by data file path to avoid committing the same file twice.

    Args:
        write_returns: Current write results from this attempt.
        checkpoint_config: The checkpoint configuration. If None, returns
            write_returns unchanged.

    Returns:
        Merged list of IcebergWriteResults (previous + current, deduplicated).
    """
    if checkpoint_config is None:
        return write_returns

    try:
        loader = IcebergCheckpointLoader(checkpoint_config)
        previous_results = loader.load_write_results()
    except Exception:
        logger.exception(
            "Failed to load previous IcebergWriteResults from checkpoint; "
            "proceeding with current results only"
        )
        return write_returns

    if not previous_results:
        return write_returns

    seen_paths: Set[str] = set()
    for r in write_returns:
        if r and hasattr(r, "data_files") and r.data_files:
            for df in r.data_files:
                file_path = getattr(df, "file_path", None)
                if file_path:
                    seen_paths.add(file_path)

    deduplicated_previous = []
    for prev_result in previous_results:
        if not prev_result or not hasattr(prev_result, "data_files"):
            continue
        new_data_files = []
        for df in prev_result.data_files or []:
            file_path = getattr(df, "file_path", None)
            if file_path and file_path not in seen_paths:
                new_data_files.append(df)
                seen_paths.add(file_path)

        if new_data_files:
            from ray.data._internal.datasource.iceberg_datasink import (
                IcebergWriteResult,
            )

            deduplicated_result = IcebergWriteResult(
                data_files=new_data_files,
                upsert_keys=prev_result.upsert_keys
                if hasattr(prev_result, "upsert_keys")
                else None,
                schemas=prev_result.schemas
                if hasattr(prev_result, "schemas")
                else [],
            )
            deduplicated_previous.append(deduplicated_result)

    if deduplicated_previous:
        total_recovered = sum(
            len(r.data_files) for r in deduplicated_previous
        )
        logger.info(
            "Recovered %d IcebergWriteResult(s) with %d data files "
            "from checkpoint",
            len(deduplicated_previous),
            total_recovered,
        )

    return deduplicated_previous + write_returns
