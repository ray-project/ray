import functools
from typing import Any, Dict, Optional, Union

from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader


class Read(AbstractMap):
    """Logical operator for read."""

    def __init__(
        self,
        datasource: Datasource,
        datasource_or_legacy_reader: Union[Datasource, Reader],
        parallelism: int,
        mem_size: Optional[int],
        num_outputs: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            f"Read{datasource.get_name()}",
            None,
            num_outputs,
            ray_remote_args=ray_remote_args,
        )
        self._datasource = datasource
        self._datasource_or_legacy_reader = datasource_or_legacy_reader
        self._parallelism = parallelism
        self._mem_size = mem_size
        self._concurrency = concurrency
        self._detected_parallelism = None

    def set_detected_parallelism(self, parallelism: int):
        """
        Set the true parallelism that should be used during execution. This
        should be specified by the user or detected by the optimizer.
        """
        self._detected_parallelism = parallelism

    def get_detected_parallelism(self) -> int:
        """
        Get the true parallelism that should be used during execution.
        """
        return self._detected_parallelism

    def aggregate_output_metadata(self) -> BlockMetadata:
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method gets metadata from the read tasks. It doesn't trigger any actual
        execution.
        """
        return self._cached_output_metadata

    @functools.cached_property
    def _cached_output_metadata(self) -> BlockMetadata:
        # Legacy datasources might not implement `get_read_tasks`.
        if self._datasource.should_create_reader:
            return BlockMetadata(None, None, None, None, None)

        # HACK: Try to get a single read task to get the metadata.
        read_tasks = self._datasource.get_read_tasks(1)
        if len(read_tasks) == 0:
            # If there are no read tasks, the dataset is probably empty.
            return BlockMetadata(None, None, None, None, None)

        # `get_read_tasks` isn't guaranteed to return exactly one read task.
        metadata = [read_task.metadata for read_task in read_tasks]

        if all(meta.num_rows is not None for meta in metadata):
            num_rows = sum(meta.num_rows for meta in metadata)
        else:
            num_rows = None

        if all(meta.size_bytes is not None for meta in metadata):
            size_bytes = sum(meta.size_bytes for meta in metadata)
        else:
            size_bytes = None

        schema = unify_block_metadata_schema(metadata)

        input_files = []
        for meta in metadata:
            if meta.input_files is not None:
                input_files.extend(meta.input_files)

        return BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes,
            schema=schema,
            input_files=input_files,
            exec_stats=None,
        )
