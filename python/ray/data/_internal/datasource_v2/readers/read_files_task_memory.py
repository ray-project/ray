"""Heap memory hints for DataSource V2 ``ReadFiles`` Ray tasks.

For integration tests, a local probe script, and manual OOM / utilization
tuning of :data:`READ_FILES_TASK_MEMORY_EPS_BYTES`, see
``python/ray/data/tests/datasource_v2/README_read_files_task_memory.md``.

Override padding per process with the environment variable
``RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES`` (integer bytes; unset or invalid
values fall back to the built-in default). Set it in the environment before
``ray.init`` so workers inherit it when they import this module.
"""

from __future__ import annotations

import os
from typing import Optional

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.util.annotations import DeveloperAPI

_READ_FILES_TASK_MEMORY_EPS_BYTES_DEFAULT = 64 * 1024 * 1024
READ_FILES_TASK_MEMORY_EPS_BYTES_ENV = "RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES"


def _parse_read_files_task_memory_eps_bytes_env(raw: Optional[str]) -> int:
    """Parse ``RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES``; invalid → default."""
    if raw is None or not raw.strip():
        return _READ_FILES_TASK_MEMORY_EPS_BYTES_DEFAULT
    try:
        v = int(raw.strip(), 10)
    except ValueError:
        return _READ_FILES_TASK_MEMORY_EPS_BYTES_DEFAULT
    if v < 0:
        return _READ_FILES_TASK_MEMORY_EPS_BYTES_DEFAULT
    return v


def _read_files_task_memory_eps_bytes_from_env() -> int:
    return _parse_read_files_task_memory_eps_bytes_env(
        os.environ.get(READ_FILES_TASK_MEMORY_EPS_BYTES_ENV)
    )


# Padding on top of decoded-footprint estimates; tune via experimentation or
# ``RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES``.
READ_FILES_TASK_MEMORY_EPS_BYTES = _read_files_task_memory_eps_bytes_from_env()

# Passed via ``MapOperator`` → ``map_task_kwargs`` so only V2 ListFiles / ReadFiles
# planners enable manifest enrichment and Ray ``memory`` merging (legacy reads
# never set these keys).
MAP_TASK_KWARG_ENRICH_MANIFEST_TASK_MEMORY = (
    "_ray_data_datasource_v2_enrich_manifest_task_memory"
)
MAP_TASK_KWARG_MERGE_READ_TASK_MEMORY = "_ray_data_datasource_v2_merge_read_task_memory"


@DeveloperAPI
def estimate_read_files_task_memory_bytes(
    manifest: FileManifest,
    target_max_block_size: Optional[int],
    *,
    eps: int = READ_FILES_TASK_MEMORY_EPS_BYTES,
) -> int:
    """Upper bound on heap memory for one ``ReadFiles`` task for ``manifest``.

    Uses ``max(uncompressed_row_group_size, 2 * target_max_block_size) + eps``
    where:

    - ``uncompressed_row_group_size`` is the largest per-file maximum raw Parquet
      row-group uncompressed footprint when ``FileManifest`` carries the optional
      ``__max_uncompressed_row_group_size`` column; otherwise it is treated as 0.
    - ``target_max_block_size`` comes from :class:`~ray.data.context.DataContext`
      when set (the reader targets that decoded block footprint). ``None`` is
      treated as 0 for the ``2 * target_max_block_size`` term.
    """
    uncompressed_rg = manifest.max_max_uncompressed_row_group_size
    tmax = int(target_max_block_size or 0)
    decoded_blocks_budget = 2 * tmax
    return max(uncompressed_rg, decoded_blocks_budget) + int(eps)


@DeveloperAPI
def enrich_file_manifest_block_metadata_if_applicable(
    block: Block,
    meta: BlockMetadata,
    *,
    target_max_block_size: Optional[int],
    eps: int = READ_FILES_TASK_MEMORY_EPS_BYTES,
) -> BlockMetadata:
    """Attach manifest-derived estimates when ``block`` is a ``FileManifest`` table."""
    from dataclasses import replace

    from ray.data._internal.datasource_v2.listing.file_manifest import (
        FILE_SIZE_COLUMN_NAME,
        PATH_COLUMN_NAME,
        FileManifest,
    )

    columns = BlockAccessor.for_block(block).column_names()
    if PATH_COLUMN_NAME not in columns or FILE_SIZE_COLUMN_NAME not in columns:
        return meta

    manifest = FileManifest(block)
    task_mem = estimate_read_files_task_memory_bytes(
        manifest, target_max_block_size, eps=eps
    )
    return replace(
        meta,
        input_files_estimated_bytes=manifest.total_estimated_in_memory_size,
        task_memory_bytes=task_mem,
    )
