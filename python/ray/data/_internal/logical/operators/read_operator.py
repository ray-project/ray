import functools
from typing import Any, Dict, Optional, Union

from ray.data._internal.logical.interfaces import SourceOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.block import (
    BlockMetadata,
    BlockMetadataWithSchema,
)
from ray.data.datasource.datasource import Datasource, Reader


class Read(AbstractMap, SourceOperator):
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

    def output_data(self):
        return None

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

    def infer_metadata(self) -> BlockMetadata:
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method gets metadata from the read tasks. It doesn't trigger any actual
        execution.
        """
        return self._cached_output_metadata.metadata

    def infer_schema(self):
        return self._cached_output_metadata.schema

    @functools.cached_property
    def _cached_output_metadata(self) -> "BlockMetadataWithSchema":
        # Legacy datasources might not implement `get_read_tasks`.
        if self._datasource.should_create_reader:
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

        # HACK: Try to get a single read task to get the metadata.
        read_tasks = self._datasource.get_read_tasks(1)
        if len(read_tasks) == 0:
            # If there are no read tasks, the dataset is probably empty.
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

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

        input_files = []
        for meta in metadata:
            if meta.input_files is not None:
                input_files.extend(meta.input_files)

        meta = BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes,
            input_files=input_files,
            exec_stats=None,
        )
        schemas = [
            read_task.schema for read_task in read_tasks if read_task.schema is not None
        ]
        from ray.data._internal.util import unify_schemas_with_validation

        schema = None
        if schemas:
            schema = unify_schemas_with_validation(schemas)
        return BlockMetadataWithSchema(metadata=meta, schema=schema)

    def can_modify_num_rows(self) -> bool:
        # NOTE: Returns true, since most of the readers expands its input
        #       and produce many rows for every single row of the input
        return True
