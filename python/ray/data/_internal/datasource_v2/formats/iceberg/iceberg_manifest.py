"""The Iceberg per-file state that travels from listing to reading.

``FileManifest`` is the only payload the framework moves between ``ListFiles``
and ``ReadFiles``, and its three built-in columns (path, size, chunk metadata)
describe a plain file. An Iceberg read also needs each data file's delete
files, partition values and residual filter, which only the listing task (the
one that called ``plan_files()``) has seen.

Each row's chunk metadata is the whole file as one ``FileChunk``, which is what
``OnlineBinPacker`` budgets on, as it does Parquet's row groups. A Parquet
file's size is its decoded bytes for the columns read, and its decode peak,
both from its footer (``iceberg_footer_sizing``); any other file is sized at
its bytes on disk, with no peak.

Each manifest row also carries the planned PyIceberg ``FileScanTask``
itself, pickled with Ray's ``cloudpickle`` as V1's read tasks are, and the
reader hands the unpickled task to ``ArrowScan`` unchanged. Carrying the task
rather than rebuilding it from typed columns keeps this module off PyIceberg's
internal constructors (``DataFile``, ``Record``), which change between
releases. Listing and reading run in one cluster, so both sides use the same
PyIceberg version.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
)

import pyarrow as pa

from ray import cloudpickle
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)

if TYPE_CHECKING:
    from pyiceberg.table import FileScanTask

    from ray.data._internal.datasource_v2.formats.iceberg.iceberg_footer_sizing import (  # noqa: E501
        DecodedSize,
    )

# The planned ``FileScanTask``, pickled.
SCAN_TASK_COLUMN_NAME = "__ib_scan_task"


def manifest_from_scan_tasks(
    tasks: Sequence["FileScanTask"],
    *,
    decoded_sizes: Optional[Mapping[str, "DecodedSize"]] = None,
) -> FileManifest:
    """Encode PyIceberg scan tasks as one manifest block, one row per task.

    Args:
        tasks: The planned scan tasks, one per data file.
        decoded_sizes: Sizes read from Parquet footers, by file path
            (``iceberg_footer_sizing``). A file without one is sized at its
            bytes on disk and requests no memory.

    Returns:
        A manifest with one row per task, in order.
    """
    decoded_sizes = decoded_sizes or {}
    chunks: List[Optional[Dict[str, Any]]] = []
    for task in tasks:
        decoded = decoded_sizes.get(task.file.file_path)
        # One unit per file: an Iceberg read unit is a whole file.
        chunks.append(
            FileChunk(
                unit_ids=(),
                num_rows=task.file.record_count,
                size_bytes=(
                    task.file.file_size_in_bytes
                    if decoded is None
                    else decoded.size_bytes
                ),
                memory_bytes=None if decoded is None else decoded.memory_bytes,
                fully_matched=False,
            ).to_metadata()
        )
    return FileManifest.construct_manifest(
        paths=[task.file.file_path for task in tasks],
        sizes=[task.file.file_size_in_bytes for task in tasks],
        chunk_metadatas=chunks,
        extra_columns=pa.table(
            {
                # 64-bit offsets, since a packed bin's tasks can pass binary's
                # 2 GiB limit.
                SCAN_TASK_COLUMN_NAME: pa.array(
                    [cloudpickle.dumps(task) for task in tasks],
                    type=pa.large_binary(),
                ),
            }
        ),
    )


def scan_tasks_from_manifest(manifest: FileManifest) -> Iterator["FileScanTask"]:
    """Unpickle each manifest row's scan task, in row order."""
    for data in manifest.as_block()[SCAN_TASK_COLUMN_NAME].to_pylist():
        yield cloudpickle.loads(data)
