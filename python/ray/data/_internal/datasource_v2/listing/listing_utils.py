from typing import Iterable

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block


def partition_files(
    blocks: Iterable[Block],
    _: TaskContext,
    partitioner: FilePartitioner,
) -> Iterable[Block]:
    for block in blocks:
        partitioner.add_input(FileManifest(block))
        while partitioner.has_partition():
            yield partitioner.next_partition().as_block()

    partitioner.finalize()
    while partitioner.has_partition():
        yield partitioner.next_partition().as_block()
