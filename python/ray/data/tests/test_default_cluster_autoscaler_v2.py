import unittest
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    DefaultClusterAutoscalerV2,
    _NodeResourceSpec,
)


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


class TestClusterAutoscaling(unittest.TestCase):
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
            topology=MagicMock(),
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
                self._node_type1["CPU"], self._node_type1["memory"]
            ): 2,
            _NodeResourceSpec.of(
                self._node_type2["CPU"], self._node_type2["memory"]
            ): 1,
        }

        with patch("ray.nodes", return_value=node_table):
            assert autoscaler._get_node_resource_spec_and_count() == expected

    @patch(
        "ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2.DefaultClusterAutoscalerV2._send_resource_request",  # noqa: E501
    )
    def test_try_scale_up_cluster(self, _send_resource_request):
        # Test _try_scale_up_cluster
        scale_up_delta = 1
        autoscaler = DefaultClusterAutoscalerV2(
            topology=MagicMock(),
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
        )
        _send_resource_request.assert_called_with([])

        resource_spec1 = _NodeResourceSpec.of(
            self._node_type1["CPU"], self._node_type1["memory"]
        )
        resource_spec2 = _NodeResourceSpec.of(
            self._node_type2["CPU"], self._node_type2["memory"]
        )
        autoscaler._get_node_resource_spec_and_count = MagicMock(
            return_value={
                resource_spec1: 2,
                resource_spec2: 1,
            },
        )

        # Test different CPU/memory utilization combinations.
        scale_up_threshold = (
            DefaultClusterAutoscalerV2.DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD
        )
        for cpu_util in [scale_up_threshold / 2, scale_up_threshold]:
            for mem_util in [scale_up_threshold / 2, scale_up_threshold]:
                # Should scale up if either CPU or memory utilization is above
                # the threshold.
                should_scale_up = (
                    cpu_util >= scale_up_threshold or mem_util >= scale_up_threshold
                )
                autoscaler._get_cluster_cpu_and_mem_util = MagicMock(
                    return_value=(cpu_util, mem_util),
                )
                autoscaler.try_trigger_scaling()
                if not should_scale_up:
                    _send_resource_request.assert_called_with([])
                else:
                    expected_resource_request = [
                        {
                            "CPU": self._node_type1["CPU"],
                            "memory": self._node_type1["memory"],
                        }
                    ] * (2 + 1)
                    expected_resource_request.extend(
                        [
                            {
                                "CPU": self._node_type2["CPU"],
                                "memory": self._node_type2["memory"],
                            }
                        ]
                        * (1 + 1)
                    )
                    _send_resource_request.assert_called_with(expected_resource_request)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
