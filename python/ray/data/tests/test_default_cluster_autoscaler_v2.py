from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    DefaultClusterAutoscalerV2,
    _NodeResourceSpec,
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


@pytest.fixture(autouse=True)
def patch_autoscaling_coordinator():
    mock_coordinator = MagicMock()

    with patch(
        "ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator.get_or_create_autoscaling_coordinator",
        return_value=mock_coordinator,
    ):
        # Patch ray.get in the autoscaling_coordinator module to avoid issues with MagicMock ObjectRefs
        with patch(
            "ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator.ray.get",
            return_value=None,
        ):
            yield


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
        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
        )

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
            assert autoscaler._get_node_resource_spec_and_count() == expected

    @pytest.mark.parametrize("cpu_util", [0.5, 0.75])
    @pytest.mark.parametrize("gpu_util", [0.5, 0.75])
    @pytest.mark.parametrize("mem_util", [0.5, 0.75])
    @patch(
        "ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2.DefaultClusterAutoscalerV2._send_resource_request"
    )  # noqa: E501
    def test_try_scale_up_cluster(
        self, _send_resource_request, cpu_util, gpu_util, mem_util
    ):

        # Test _try_scale_up_cluster
        scale_up_threshold = 0.75
        scale_up_delta = 1
        utilization = ExecutionResources(
            cpu=cpu_util, gpu=gpu_util, object_store_memory=mem_util
        )

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
        )
        _send_resource_request.assert_called_with([])

        resource_spec1 = _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000)
        resource_spec2 = _NodeResourceSpec.of(cpu=8, gpu=1, mem=1000)
        autoscaler._get_node_resource_spec_and_count = MagicMock(
            return_value={
                resource_spec1: 2,
                resource_spec2: 1,
            },
        )

        autoscaler.try_trigger_scaling()

        # Should scale up if any resource is above the threshold.
        should_scale_up = (
            cpu_util >= scale_up_threshold
            or gpu_util >= scale_up_threshold
            or mem_util >= scale_up_threshold
        )
        if not should_scale_up:
            _send_resource_request.assert_called_with([])
        else:
            expected_num_resource_spec1_requested = 2 + scale_up_delta
            expected_resource_request = [
                {
                    "CPU": resource_spec1.cpu,
                    "GPU": resource_spec1.gpu,
                    "memory": resource_spec1.mem,
                }
            ] * expected_num_resource_spec1_requested

            expected_num_resource_spec2_requested = 1 + scale_up_delta
            expected_resource_request.extend(
                [
                    {
                        "CPU": resource_spec2.cpu,
                        "GPU": resource_spec2.gpu,
                        "memory": resource_spec2.mem,
                    }
                ]
                * expected_num_resource_spec2_requested
            )

            _send_resource_request.assert_called_with(expected_resource_request)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
