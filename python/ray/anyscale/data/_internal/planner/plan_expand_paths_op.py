import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

import numpy as np
import pyarrow as pa

import ray
from ray.anyscale.data._internal.logical.operators.expand_paths_operator import (
    ExpandPaths,
)
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_meta_provider import _handle_read_os_error
from ray.data.datasource.path_util import _has_file_extension

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)

# TODO(@bveeramani): 200 is arbitrary.
MAX_NUM_INPUT_BLOCKS = 200

# To amortize overheads associated with launching Ray tasks and using multi-threading,
# produce multiple blocks in each read task.
NUM_BLOCKS_PER_READ_TASK = 8


def plan_expand_paths_op(
    op: ExpandPaths, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 0
    input_data_buffer = create_input_data_buffer(op)
    expand_paths_operator = create_expand_paths_operator(input_data_buffer, op)
    return expand_paths_operator


def create_input_data_buffer(logical_op: ExpandPaths) -> InputDataBuffer:
    path_splits = np.array_split(
        logical_op.paths, min(MAX_NUM_INPUT_BLOCKS, len(logical_op.paths))
    )

    input_data = []
    for path_split in path_splits:
        block = pa.Table.from_pydict({"path": path_split})
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
    return InputDataBuffer(input_data=input_data)


def create_expand_paths_operator(
    input_op: PhysicalOperator,
    logical_op: ExpandPaths,
) -> PhysicalOperator:
    def expand_paths(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        file_paths = []
        running_in_memory_size = 0
        encoding_ratio = None  # Ratio of in-memory size to file size.
        for block in blocks:
            assert isinstance(block, pa.Table)
            assert "path" in block.column_names, block.column_names
            for path in map(str, list(block["path"])):
                for file_path, file_size in _get_file_infos(
                    path, logical_op.filesystem, logical_op.ignore_missing_paths
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

                    if encoding_ratio is None:
                        encoding_ratio = _estimate_encoding_ratio(
                            file_path, file_size, logical_op
                        )

                    file_paths.append(file_path)
                    # `HTTPFileSystem` returns `None` for `file_size`. In this case, we
                    # place all paths in the same block.
                    if file_size is not None:
                        in_memory_size = file_size * encoding_ratio
                        running_in_memory_size += in_memory_size

                    if (
                        running_in_memory_size
                        > ray.data.DataContext.get_current().target_max_block_size
                        * NUM_BLOCKS_PER_READ_TASK
                    ):
                        block = pa.Table.from_pydict({"path": file_paths})
                        yield block

                        file_paths = []
                        running_in_memory_size = 0

        if file_paths:
            block = pa.Table.from_pydict({"path": file_paths})
            yield block

    transform_fns: List[MapTransformFn] = [BlockMapTransformFn(expand_paths)]
    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_op,
        name="ExpandPaths",
        target_max_block_size=None,
        ray_remote_args={
            "num_cpus": 0.5,
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
    path: str,
    filesystem: "pyarrow.fs.FileSystem",
    ignore_missing_path: bool,
) -> Iterable[Tuple[str, Optional[int]]]:
    exclude_prefixes = [".", "_"]

    from pyarrow.fs import FileSelector

    selector = FileSelector(path, recursive=True, allow_not_found=ignore_missing_path)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    for file_ in files:
        if not file_.is_file:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path) :]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue
        yield (file_path, file_.size)


def _estimate_encoding_ratio(
    path: str, file_size: Optional[int], logical_op: ExpandPaths
) -> float:
    if file_size is not None and file_size > 0:
        in_memory_size = logical_op.reader.estimate_in_memory_size(
            path, file_size, filesystem=logical_op.filesystem
        )
        encoding_ratio = in_memory_size / file_size
    else:
        encoding_ratio = 1.0
    return encoding_ratio
