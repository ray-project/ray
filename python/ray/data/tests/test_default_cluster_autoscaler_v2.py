from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    DefaultClusterAutoscalerV2,
    _get_node_resource_spec_and_count,
    _NodeResourceSpec,
)
from ray.data._internal.cluster_autoscaler.fake_autoscaling_coordinator import (
    FakeAutoscalingCoordinator,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    ResourceUtilizationGauge,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources


class StubUtilizationGauge(ResourceUtilizationGauge):
    def __init__(self, utilization: ExecutionResources):
        self._utilization = utilization

    def observe(self):
        pass

    def get(self):
        return self._utilization


class TestClusterAutoscaling:
    """Tests for cluster autoscaling functions in DefaultClusterAutoscalerV2."""

    def setup_class(self):
        self._node_type1 = {
            "CPU": 4,
            "memory": 1000,
            "object_store_memory": 500,
        }
        self._node_type2 = {
            "CPU": 8,
            "memory": 2000,
            "object_store_memory": 500,
        }
        self._node_type3 = {
            "CPU": 4,
            "GPU": 1,
            "memory": 1000,
            "object_store_memory": 500,
        }
        self._head_node = {
            "CPU": 4,
            "memory": 1000,
            "object_store_memory": 500,
            "node:__internal_head__": 1.0,
        }
        ray.init()

    def teardown_class(self):
        ray.shutdown()

    def test_get_node_resource_spec_and_count(self):
        # Test _get_node_resource_spec_and_count

        node_table = [
            {
                "Resources": self._head_node,
                "Alive": True,
            },
            {
                "Resources": self._node_type1,
                "Alive": True,
            },
            {
                "Resources": self._node_type2,
                "Alive": True,
            },
            {
                "Resources": self._node_type3,
                "Alive": True,
            },
            {
                "Resources": self._node_type1,
                "Alive": True,
            },
            {
                "Resources": self._node_type2,
                "Alive": False,
            },
        ]

        expected = {
            _NodeResourceSpec.of(
                cpu=self._node_type1["CPU"],
                gpu=self._node_type1.get("GPU", 0),
                mem=self._node_type1["memory"],
            ): 2,
            _NodeResourceSpec.of(
                cpu=self._node_type2["CPU"],
                gpu=self._node_type2.get("GPU", 0),
                mem=self._node_type2["memory"],
            ): 1,
            _NodeResourceSpec.of(
                cpu=self._node_type3["CPU"],
                gpu=self._node_type3.get("GPU", 0),
                mem=self._node_type3["memory"],
            ): 1,
        }

        with patch("ray.nodes", return_value=node_table):
            assert _get_node_resource_spec_and_count() == expected

    @pytest.mark.parametrize("cpu_util", [0.5, 0.75])
    @pytest.mark.parametrize("gpu_util", [0.5, 0.75])
    @pytest.mark.parametrize("mem_util", [0.5, 0.75])
    def test_try_scale_up_cluster(self, cpu_util, gpu_util, mem_util):

        # Test _try_scale_up_cluster
        scale_up_threshold = 0.75
        scale_up_delta = 1
        utilization = ExecutionResources(
            cpu=cpu_util, gpu=gpu_util, object_store_memory=mem_util
        )
        fake_coordinator = FakeAutoscalingCoordinator()
        resource_spec1 = _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000)
        resource_spec2 = _NodeResourceSpec.of(cpu=8, gpu=1, mem=1000)

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {resource_spec1: 2, resource_spec2: 1},
        )
        # Bypass the request rate limit for this test.
        autoscaler._last_request_time = 0

        autoscaler.try_trigger_scaling()

        # Should scale up if any resource is above the threshold.
        should_scale_up = (
            cpu_util >= scale_up_threshold
            or gpu_util >= scale_up_threshold
            or mem_util >= scale_up_threshold
        )
        resources_allocated = autoscaler.get_total_resources()
        if not should_scale_up:
            assert resources_allocated == ExecutionResources.zero()
        else:
            expected_num_resource_spec1_requested = 2 + scale_up_delta
            expected_num_resource_spec2_requested = 1 + scale_up_delta
            expected_resources = ExecutionResources(
                cpu=(
                    resource_spec1.cpu * expected_num_resource_spec1_requested
                    + resource_spec2.cpu * expected_num_resource_spec2_requested
                ),
                gpu=(
                    resource_spec1.gpu * expected_num_resource_spec1_requested
                    + resource_spec2.gpu * expected_num_resource_spec2_requested
                ),
                memory=(
                    resource_spec1.mem * expected_num_resource_spec1_requested
                    + resource_spec2.mem * expected_num_resource_spec2_requested
                ),
            )

            assert resources_allocated == expected_resources


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
