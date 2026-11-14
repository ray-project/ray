"""Serializable filesystem instrumentation for schema-planning tests."""

import json
import os

from fsspec.implementations.local import LocalFileSystem

import ray


class RecordingLocalFileSystem(LocalFileSystem):
    def __init__(
        self,
        trace_path=None,
        require_env=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.trace_path = trace_path
        self.require_env = require_env

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        if self.require_env is not None:
            assert os.environ.get(self.require_env) == "enabled"
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
