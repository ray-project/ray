"""Serializable filesystem instrumentation for schema-planning tests."""

import json
import os
from typing import List

import pyarrow as pa
from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import ListFiles


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
        require_env_for_info=None,
        deny_open_path=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.trace_path = trace_path
        self.require_env = require_env
        self.require_env_for_info = require_env_for_info
        self.deny_open_path = deny_open_path
        self.opens = []
        self.fail_open = False

    def info(self, path, **kwargs):
        if self.require_env_for_info is not None:
            assert os.environ.get(self.require_env_for_info) == "enabled"
        return super().info(path, **kwargs)

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        assert not self.fail_open, f"Unexpected metadata read: {path}"
        if self.deny_open_path is not None and os.path.exists(self.deny_open_path):
            with open(self.deny_open_path) as marker:
                assert marker.read() != "deny", f"Unexpected metadata read: {path}"
        if self.require_env is not None:
            assert os.environ.get(self.require_env) == "enabled"
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
