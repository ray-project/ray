"""Heap memory hints for DataSource V2 ``ReadFiles`` Ray tasks.

The formula is
    ``task_memory_bytes = max(2 * target_max_block_size, max(file_sizes)) + eps``,
where ``max(file_sizes)`` is the largest on-disk file size in the manifest.
``eps`` accounts for additional per-task scratch (pyarrow buffers, transforms,
generator state). ``eps`` defaults to
:attr:`~ray.data.context.DataContext.read_files_task_memory_eps_bytes` and can
be overridden per-context or per-call.

For tuning, see
``python/ray/data/tests/datasource_v2/read_files_task_memory_probe.py``.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Optional

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI


def _default_eps() -> int:
    """Resolve ``eps`` from the current ``DataContext`` (also callable inside workers)."""
    return int(DataContext.get_current().read_files_task_memory_eps_bytes)


@DeveloperAPI
def estimate_read_files_task_memory_bytes(
    manifest: FileManifest,
    target_max_block_size: Optional[int],
    *,
    eps: Optional[int] = None,
) -> int:
    """Upper bound on heap memory for one ``ReadFiles`` task for ``manifest``.

    Returns ``max(2 * target_max_block_size, max(file_sizes)) + eps``:

    - ``2 * target_max_block_size`` covers the input batch buffer plus the
      output block being assembled in the same task.
    - ``max(file_sizes)`` is the largest on-disk file size in the manifest,
      a fast proxy for per-task peak decode footprint. ``0`` for an empty
      manifest.
    - ``eps`` covers additional per-task scratch (defaults to
      ``DataContext.read_files_task_memory_eps_bytes``; override via the
      context knob or pass explicitly).
    """
    if eps is None:
        eps = _default_eps()
    max_file = int(manifest.file_sizes.max()) if len(manifest) else 0
    block_pair = 2 * int(target_max_block_size or 0)
    return max(block_pair, max_file) + int(eps)


@DeveloperAPI
def enrich_file_manifest_block_metadata_if_applicable(
    block: Block,
    meta: BlockMetadata,
    *,
    target_max_block_size: Optional[int],
    eps: Optional[int] = None,
) -> BlockMetadata:
    """Attach ``task_memory_bytes`` when ``block`` is a ``FileManifest`` table.

    Pass-through for any non-manifest block (returns ``meta`` unchanged), so
    this is safe to apply in a generic per-block postprocess hook.
    """
    columns = BlockAccessor.for_block(block).column_names()
    if PATH_COLUMN_NAME not in columns or FILE_SIZE_COLUMN_NAME not in columns:
        return meta

    manifest = FileManifest(block)
    task_mem = estimate_read_files_task_memory_bytes(
        manifest, target_max_block_size, eps=eps
    )
    return replace(meta, task_memory_bytes=task_mem)


@DeveloperAPI
def enrich_manifest_block_metadata_for_map_task(
    block: Block, meta: BlockMetadata, data_context: DataContext
) -> BlockMetadata:
    """Adapter for ``MAP_TASK_KWARG_BLOCK_METADATA_POSTPROCESS``.

    Module-level so it pickles across the Ray task boundary. Reads
    ``target_max_block_size`` and ``read_files_task_memory_eps_bytes`` from
    the serialized worker-side ``DataContext`` so driver-side overrides take
    effect.
    """
    return enrich_file_manifest_block_metadata_if_applicable(
        block,
        meta,
        target_max_block_size=data_context.target_max_block_size,
        eps=data_context.read_files_task_memory_eps_bytes,
    )
