"""The Iceberg per-file state that travels from listing to reading.

``FileManifest`` is the only payload the framework moves between ``ListFiles``
and ``ReadFiles``, and its three built-in columns (path, size, chunk metadata)
describe a plain file. An Iceberg read also needs each data file's delete
files, partition values and residual filter, which only the listing task (the
one that called ``plan_files()``) has seen.

Each row's chunk metadata is the whole file as one ``FileChunk``, sized as the
file's decoded bytes for the columns read, which is what ``OnlineBinPacker``
budgets on, as it does Parquet's row groups. A Parquet file's size and decode
peak come from its footer (``iceberg_footer_sizing``); any other file is sized
from ``column_sizes`` (``_estimate_inmemory_file_size``) with no peak. Its
``fully_matched`` flag says the file's ``record_count`` is exact after the
filter (``iceberg_row_match``).

Each manifest row also carries the planned PyIceberg ``FileScanTask``
itself, pickled with Ray's ``cloudpickle`` as V1's read tasks are, and the
reader hands the unpickled task to ``ArrowScan`` unchanged. Carrying the task
rather than rebuilding it from typed columns keeps this module off PyIceberg's
internal constructors (``DataFile``, ``Record``), which change between
releases. Listing and reading run in one cluster, so both sides use the same
PyIceberg version.

The task is pickled without its data and delete files' per-column statistics
(sizes, counts and bounds), which listing has already used and the reader
never reads. They are most of a task's bytes: a one-row file of a 1,000-column
table pickles to 85,746 B with them and 516 B without, and each attached
position delete file to 370 B with them and 144 B without.
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
    Set,
)

import pyarrow as pa

from ray import cloudpickle
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_utils import (
    _estimate_inmemory_file_size,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)

if TYPE_CHECKING:
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import FileScanTask

    from ray.data._internal.datasource_v2.formats.iceberg.iceberg_footer_sizing import (  # noqa: E501
        DecodedSize,
    )

# The planned ``FileScanTask``, pickled.
SCAN_TASK_COLUMN_NAME = "__ib_scan_task"


def manifest_from_scan_tasks(
    tasks: Sequence["FileScanTask"],
    *,
    projected_field_ids: Optional[Set[int]] = None,
    decoded_sizes: Optional[Mapping[str, "DecodedSize"]] = None,
    fully_matched: Optional[Sequence[bool]] = None,
) -> FileManifest:
    """Encode PyIceberg scan tasks as one manifest block, one row per task.

    Args:
        tasks: The planned scan tasks, one per data file.
        projected_field_ids: Top-level field ids the read decodes, ``None`` for
            every column. Sizes each row for the bin packer.
        decoded_sizes: Sizes read from Parquet footers, by file path
            (``iceberg_footer_sizing``). A file without one is sized from its
            ``column_sizes`` and requests no memory.
        fully_matched: Per task, whether every row passes the filter. ``None``
            marks no task.

    Returns:
        A manifest with one row per task, in order.
    """
    decoded_sizes = decoded_sizes or {}
    if fully_matched is None:
        fully_matched = [False] * len(tasks)
    chunks: List[Optional[Dict[str, Any]]] = []
    for task, matched in zip(tasks, fully_matched):
        decoded = decoded_sizes.get(task.file.file_path)
        # One unit per file: an Iceberg read unit is a whole file.
        chunks.append(
            FileChunk(
                unit_ids=(),
                num_rows=task.file.record_count,
                size_bytes=(
                    _estimate_inmemory_file_size(task.file, projected_field_ids)
                    if decoded is None
                    else decoded.size_bytes
                ),
                memory_bytes=None if decoded is None else decoded.memory_bytes,
                fully_matched=matched,
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
                    [pickle_scan_task(task) for task in tasks],
                    type=pa.large_binary(),
                ),
            }
        ),
    )


def pickle_scan_task(task: "FileScanTask") -> bytes:
    """``task`` as a manifest row carries it."""
    return cloudpickle.dumps(_without_column_stats(task))


def scan_tasks_from_manifest(manifest: FileManifest) -> Iterator["FileScanTask"]:
    """Unpickle each manifest row's scan task, in row order."""
    for task in scan_tasks_by_row(manifest):
        assert task is not None, "a row-group run without deletes has no scan task"
        yield task


def scan_tasks_by_row(manifest: FileManifest) -> Iterator[Optional["FileScanTask"]]:
    """Each row's scan task, ``None`` for a row-group run without deletes."""
    for data in manifest.as_block()[SCAN_TASK_COLUMN_NAME].to_pylist():
        yield None if data is None else cloudpickle.loads(data)


# The ``DataFile`` fields listing reads and the reader never does.
_COLUMN_STATS = (
    "column_sizes",
    "value_counts",
    "null_value_counts",
    "nan_value_counts",
    "lower_bounds",
    "upper_bounds",
)


def _without_column_stats(task: "FileScanTask") -> "FileScanTask":
    """A copy of ``task`` whose data and delete files carry no column stats."""
    from pyiceberg.table import FileScanTask

    return FileScanTask(
        _data_file_without_column_stats(task.file),
        delete_files={_data_file_without_column_stats(f) for f in task.delete_files},
        residual=task.residual,
    )


def _data_file_without_column_stats(data_file: "DataFile") -> "DataFile":
    from pyiceberg.manifest import DATA_FILE_TYPE, DEFAULT_READ_VERSION

    # ``DataFile`` stores its fields by position, in this struct's order.
    names = [field.name for field in DATA_FILE_TYPE[DEFAULT_READ_VERSION].fields]
    if not set(_COLUMN_STATS) <= set(names):
        return data_file
    copy = type(data_file)(*(data_file[i] for i in range(len(data_file))))
    for name in _COLUMN_STATS:
        copy[names.index(name)] = None
    spec_id = getattr(data_file, "spec_id", None)
    if spec_id is not None:
        copy.spec_id = spec_id
    kept = ("file_path", "file_format", "record_count", "file_size_in_bytes")
    if any(getattr(copy, name) is not None for name in _COLUMN_STATS) or any(
        getattr(copy, name) != getattr(data_file, name) for name in kept
    ):
        # Another PyIceberg layout; pickle the file as it is.
        return data_file
    return copy
