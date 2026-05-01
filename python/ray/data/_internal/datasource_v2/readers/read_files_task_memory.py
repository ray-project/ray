"""Heap memory hints for DataSource V2 ``ReadFiles`` Ray tasks.

For integration tests, a local probe script, and manual OOM / utilization
tuning of :data:`READ_FILES_TASK_MEMORY_EPS_BYTES`, see
``python/ray/data/tests/datasource_v2/README_read_files_task_memory.md``.
"""

from __future__ import annotations

from typing import Optional

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.util.annotations import DeveloperAPI

# Padding on top of decoded-footprint estimates; tune via experimentation.
READ_FILES_TASK_MEMORY_EPS_BYTES = 64 * 1024 * 1024

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

    Uses ``max(uncompressed_row_group_size, batch_size_in_bytes, block_size) + eps``
    where:

    - ``uncompressed_row_group_size`` is the largest per-file maximum raw Parquet
      row-group uncompressed footprint when ``FileManifest`` carries the optional
      ``__max_uncompressed_row_group_size`` column; otherwise it is treated as 0.
    - ``batch_size_in_bytes`` and ``block_size`` both use
      ``target_max_block_size`` from :class:`~ray.data.context.DataContext` when set
      (the reader targets that decoded batch footprint).
    """
    uncompressed_rg = manifest.max_max_uncompressed_row_group_size
    block_or_batch = int(target_max_block_size or 0)
    return max(uncompressed_rg, block_or_batch, block_or_batch) + int(eps)


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
