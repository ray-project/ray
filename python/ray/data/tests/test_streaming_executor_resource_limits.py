import collections
import math
import time
from unittest.mock import patch

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions, ExecutionResources
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_from_block_fn,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.streaming_executor_state import (
    DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION,
    DownstreamMemoryInfo,
    TopologyResourceUsage,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.tests.conftest import *  # noqa

EMPTY_DOWNSTREAM_USAGE = collections.defaultdict(lambda: DownstreamMemoryInfo(0, 0))
NO_USAGE = TopologyResourceUsage(ExecutionResources(), EMPTY_DOWNSTREAM_USAGE)


@ray.remote
def sleep():
    time.sleep(999)


def make_map_transformer(block_fn):
    def map_fn(block_iter):
        for block in block_iter:
            yield block_fn(block)

    return create_map_transformer_from_block_fn(map_fn)


def make_ref_bundle(x):
    return make_ref_bundles([[x]])[0]


def test_resource_limits():
    cluster_resources = {"CPU": 10, "GPU": 5, "object_store_memory": 1000}
    default_object_store_memory_limit = math.ceil(
        cluster_resources["object_store_memory"]
        * DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
    )

    with patch("ray.cluster_resources", return_value=cluster_resources):
        # Test default resource limits.
        # When no resource limits are set, the resource limits should default to
        # the cluster resources for CPU/GPU, and
        # DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION of cluster object store memory.
        options = ExecutionOptions()
        executor = StreamingExecutor(options, "")
        expected = ExecutionResources(
            cpu=cluster_resources["CPU"],
            gpu=cluster_resources["GPU"],
            object_store_memory=default_object_store_memory_limit,
        )
        assert executor._get_or_refresh_resource_limits() == expected

        # Test setting resource_limits
        options = ExecutionOptions()
        options.resource_limits = ExecutionResources(
            cpu=1, gpu=2, object_store_memory=100
        )
        executor = StreamingExecutor(options, "")
        expected = ExecutionResources(
            cpu=1,
            gpu=2,
            object_store_memory=100,
        )
        assert executor._get_or_refresh_resource_limits() == expected

        # Test setting exclude_resources
        # The actual limit should be the default limit minus the excluded resources.
        options = ExecutionOptions()
        options.exclude_resources = ExecutionResources(
            cpu=1, gpu=2, object_store_memory=100
        )
        executor = StreamingExecutor(options, "")
        expected = ExecutionResources(
            cpu=cluster_resources["CPU"] - 1,
            gpu=cluster_resources["GPU"] - 2,
            object_store_memory=default_object_store_memory_limit - 100,
        )
        assert executor._get_or_refresh_resource_limits() == expected

        # Test that we don't support setting both resource_limits and exclude_resources.
        with pytest.raises(ValueError):
            options = ExecutionOptions()
            options.resource_limits = ExecutionResources(cpu=2)
            options.exclude_resources = ExecutionResources(cpu=1)
            options.validate()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
