import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

import numpy as np
import pyarrow as pa

import ray
from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    ListFiles,
)
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlocksToRowsMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformer,
    Row,
    RowMapTransformFn,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.file_meta_provider import _handle_read_os_error
from ray.data.datasource.path_util import _has_file_extension

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)

# TODO(@bveeramani): 200 is arbitrary.
# This is the maximum total number of list files task that we launch. In practice, we'll
# usually only launch one list files task (i.e., in the case the user provides a single
# directory).
DEFAULT_MAX_NUM_LIST_FILES_TASKS = 200


def plan_list_files_op(
    op: ListFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 0
    input_data_buffer = create_input_data_buffer(op, data_context)
    map_operator = create_map_operator(input_data_buffer, op, data_context)
    return map_operator


def create_input_data_buffer(
    logical_op: ListFiles, data_context: DataContext
) -> InputDataBuffer:
    max_num_list_files_tasks = data_context.get_config(
        "max_num_list_files_tasks", DEFAULT_MAX_NUM_LIST_FILES_TASKS
    )
    path_splits = np.array_split(
        logical_op.paths, min(max_num_list_files_tasks, len(logical_op.paths))
    )

    input_data = []
    for path_split in path_splits:
        block = pa.Table.from_pydict({PATH_COLUMN_NAME: path_split})
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=None
        )
        ref_bundle = RefBundle(
            [(ray.put(block), metadata)],
            # `owns_blocks` is False, because these refs are the root of the
            # DAG. We shouldn't eagerly free them. Otherwise, the DAG cannot
            # be reconstructed.
            owns_blocks=False,
        )
        input_data.append(ref_bundle)
    return InputDataBuffer(data_context, input_data=input_data)


def create_map_operator(
    input_op: PhysicalOperator,
    logical_op: ListFiles,
    data_context: DataContext,
) -> MapOperator:
    def list_files(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
        for row in rows:
            for file_path, file_size in _get_file_infos(
                row[PATH_COLUMN_NAME],
                logical_op.filesystem,
                logical_op.ignore_missing_paths,
            ):
                if not _has_file_extension(file_path, logical_op.file_extensions):
                    logger.debug(
                        f"Skipping file '{file_path}' because it does not have one "
                        f"of the required extensions: {logical_op.file_extensions}"
                    )
                    continue

                if logical_op.partition_filter is not None:
                    if not logical_op.partition_filter([file_path]):
                        logger.debug(
                            f"Skipping file '{file_path}' because it does not "
                            "match the partition filter."
                        )
                        continue

                yield {
                    PATH_COLUMN_NAME: file_path,
                    FILE_SIZE_COLUMN_NAME: file_size,
                }

    transform_fns = [
        BlocksToRowsMapTransformFn.instance(),
        RowMapTransformFn(list_files, is_udf=False),
        BuildOutputBlocksMapTransformFn.for_rows(),
    ]

    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_op,
        data_context,
        name="ListFiles",
        # This will push the blocks to the next operator faster.
        target_max_block_size=data_context.target_min_block_size,
        ray_remote_args={
            # This is operator is extremely fast. If we don't unblock backpressure, this
            # operator gets bottlenecked by the Ray Data scheduler. This can prevent Ray
            # Data from launching enough read tasks.
            "_generator_backpressure_num_objects": -1,
        },
    )


def _get_file_infos(
    path: str,
    filesystem: "pyarrow.fs.FileSystem",
    ignore_missing_path: bool,
) -> Iterable[Tuple[str, Optional[int]]]:
    from pyarrow.fs import FileType

    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)

    if file_info.type == FileType.Directory:
        yield from _expand_directory(path, filesystem, ignore_missing_path)
    elif file_info.type == FileType.File:
        yield (path, file_info.size)
    elif file_info.type == FileType.NotFound and ignore_missing_path:
        pass
    else:
        raise FileNotFoundError(path)


def _expand_directory(
    base_path: str,
    filesystem: "pyarrow.fs.FileSystem",
    ignore_missing_path: bool,
) -> Iterable[Tuple[str, Optional[int]]]:
    from pyarrow.fs import FileSelector, FileType

    exclude_prefixes = [".", "_"]
    selector = FileSelector(
        base_path, recursive=False, allow_not_found=ignore_missing_path
    )
    files = filesystem.get_file_info(selector)

    for file_ in files:
        if not file_.path.startswith(base_path):
            continue

        relative = file_.path[len(base_path) :]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue

        if file_.type == FileType.File:
            yield (file_.path, file_.size)
        elif file_.type == FileType.Directory:
            yield from _expand_directory(file_.path, filesystem, ignore_missing_path)
        elif file_.type == FileType.UNKNOWN:
            logger.warning(f"Discovered file with unknown type: '{file_.path}'")
            continue
        else:
            assert file_.type == FileType.NotFound
            raise FileNotFoundError(file_.path)
