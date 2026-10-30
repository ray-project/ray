"""Serializable filesystem instrumentation for schema-planning tests."""

import json
import os

from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data._internal.logical.operators import ListFiles
from ray.data.context import DataContext


def get_prelisted_blocks(operator):
    blocks = next(
        op for op in operator.post_order_iter() if isinstance(op, ListFiles)
    ).prelisted_file_infos
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
        self.opens.append(path)
        if self.require_env is not None:
            assert os.environ.get(self.require_env) == "enabled"
        if self.schema_config is not None:
            context = DataContext.get_current()
            assert all(
                getattr(context, name) == value
                for name, value in self.schema_config.items()
            )
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
