import math
import unittest
from unittest.mock import MagicMock, patch

import pytest

from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.resource_manager import ResourceManager


class TestResourceManager(unittest.TestCase):
    def test_global_limits(self):
        cluster_resources = {"CPU": 10, "GPU": 5, "object_store_memory": 1000}
        default_object_store_memory_limit = math.ceil(
            cluster_resources["object_store_memory"]
            * ResourceManager.DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
        )

        with patch("ray.cluster_resources", return_value=cluster_resources):
            # Test default resource limits.
            # When no resource limits are set, the resource limits should default to
            # the cluster resources for CPU/GPU, and
            # DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION of cluster object store memory.
            options = ExecutionOptions()
            resource_manager = ResourceManager(MagicMock(), options)
            expected = ExecutionResources(
                cpu=cluster_resources["CPU"],
                gpu=cluster_resources["GPU"],
                object_store_memory=default_object_store_memory_limit,
            )
            assert resource_manager.get_global_limits() == expected

            # Test setting resource_limits
            options = ExecutionOptions()
            options.resource_limits = ExecutionResources(
                cpu=1, gpu=2, object_store_memory=100
            )
            resource_manager = ResourceManager(MagicMock(), options)
            expected = ExecutionResources(
                cpu=1,
                gpu=2,
                object_store_memory=100,
            )
            assert resource_manager.get_global_limits() == expected

            # Test setting exclude_resources
            # The actual limit should be the default limit minus the excluded resources.
            options = ExecutionOptions()
            options.exclude_resources = ExecutionResources(
                cpu=1, gpu=2, object_store_memory=100
            )
            resource_manager = ResourceManager(MagicMock(), options)
            expected = ExecutionResources(
                cpu=cluster_resources["CPU"] - 1,
                gpu=cluster_resources["GPU"] - 2,
                object_store_memory=default_object_store_memory_limit - 100,
            )
            assert resource_manager.get_global_limits() == expected

            # Test that we don't support setting both resource_limits and exclude_resources.
            with pytest.raises(ValueError):
                options = ExecutionOptions()
                options.resource_limits = ExecutionResources(cpu=2)
                options.exclude_resources = ExecutionResources(cpu=1)
                options.validate()

    def test_calculating_usage(self):
        pass
