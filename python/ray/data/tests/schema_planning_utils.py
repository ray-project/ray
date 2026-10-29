"""Serializable filesystem instrumentation for schema-planning tests."""

import json
import os
from typing import List

import pyarrow as pa
from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import ListFiles
from ray.data.context import DataContext


def get_prelisted_blocks(read_op: LogicalOperator) -> List[pa.Table]:
    listing = read_op.input_dependencies[0]
    assert isinstance(listing, ListFiles)
    blocks = listing.prelisted_file_infos
    assert blocks is not None
    return blocks


class RecordingLocalFileSystem(LocalFileSystem):
    def __init__(
        self,
        trace_path=None,
        require_env=None,
        schema_config=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.trace_path = trace_path
        self.require_env = require_env
        self.schema_config = schema_config
        self.opens = []

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        if self.require_env is not None:
            assert os.environ.get(self.require_env) == "enabled"
        if self.schema_config is not None:
            context = DataContext.get_current()
            assert all(
                getattr(context, name) == value
                for name, value in self.schema_config.items()
            )
        self.opens.append(path)
        if self.trace_path is not None:
            with open(self.trace_path, "a") as output:
                output.write(
                    json.dumps(
                        {
                            "path": path,
                            "pid": os.getpid(),
                            "node": ray.get_runtime_context().get_node_id(),
                        }
                    )
                    + "\n"
                )
        return super()._open(path, mode, block_size=block_size, **kwargs)
