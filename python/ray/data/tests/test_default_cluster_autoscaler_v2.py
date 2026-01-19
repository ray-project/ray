from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.core.generated import autoscaler_pb2
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

        # Patch cluster config to return None
        with (
            patch("ray.nodes", return_value=node_table),
            patch(
                "ray._private.state.state.get_cluster_config",
                return_value=None,
            ),
        ):
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
            resource_limits=ExecutionResources.inf(),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {resource_spec1: 2, resource_spec2: 1},
        )

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

    def test_get_node_resource_spec_and_count_from_zero(self):
        """Test that get_node_resource_spec_and_count can discover node types
        from cluster config even when there are zero worker nodes."""
        # Simulate a cluster with only head node (no worker nodes)
        node_table = [
            {
                "Resources": self._head_node,
                "Alive": True,
            },
        ]

        # Create a mock cluster config with 2 worker node types
        cluster_config = autoscaler_pb2.ClusterConfig()

        # Node type 1: 4 CPU, 0 GPU, 1000 memory
        node_group_config1 = autoscaler_pb2.NodeGroupConfig()
        node_group_config1.resources["CPU"] = 4
        node_group_config1.resources["memory"] = 1000
        node_group_config1.max_count = 10
        cluster_config.node_group_configs.append(node_group_config1)

        # Node type 2: 8 CPU, 2 GPU, 2000 memory
        node_group_config2 = autoscaler_pb2.NodeGroupConfig()
        node_group_config2.resources["CPU"] = 8
        node_group_config2.resources["GPU"] = 2
        node_group_config2.resources["memory"] = 2000
        node_group_config2.max_count = 5
        cluster_config.node_group_configs.append(node_group_config2)

        expected = {
            _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000): 0,
            _NodeResourceSpec.of(cpu=8, gpu=2, mem=2000): 0,
        }

        with patch("ray.nodes", return_value=node_table):
            with patch(
                "ray._private.state.state.get_cluster_config",
                return_value=cluster_config,
            ):
                result = _get_node_resource_spec_and_count()
                assert result == expected

    def test_try_scale_up_cluster_from_zero(self):
        """Test that the autoscaler can scale up from zero worker nodes."""
        scale_up_threshold = 0.75
        scale_up_delta = 1
        # High utilization to trigger scaling
        utilization = ExecutionResources(cpu=0.9, gpu=0.9, object_store_memory=0.9)

        # Mock the node resource spec with zero counts
        resource_spec1 = _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000)
        resource_spec2 = _NodeResourceSpec.of(cpu=8, gpu=2, mem=2000)
        fake_coordinator = FakeAutoscalingCoordinator()

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=ExecutionResources.inf(),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {
                resource_spec1: 0,
                resource_spec2: 0,
            },
        )

        autoscaler.try_trigger_scaling()

        # Should request scale_up_delta nodes of each type
        # Verify via get_total_resources which returns what was allocated
        resources_allocated = autoscaler.get_total_resources()
        expected_resources = ExecutionResources(
            cpu=resource_spec1.cpu * scale_up_delta
            + resource_spec2.cpu * scale_up_delta,
            gpu=resource_spec1.gpu * scale_up_delta
            + resource_spec2.gpu * scale_up_delta,
            memory=resource_spec1.mem * scale_up_delta
            + resource_spec2.mem * scale_up_delta,
        )
        assert resources_allocated == expected_resources

    def test_get_node_resource_spec_and_count_skips_max_count_zero(self):
        """Test that node types with max_count=0 are skipped."""
        # Simulate a cluster with only head node (no worker nodes)
        node_table = [
            {
                "Resources": self._head_node,
                "Alive": True,
            },
        ]

        # Create a mock cluster config with one valid node type and one with max_count=0
        cluster_config = autoscaler_pb2.ClusterConfig()

        # Node type 1: 4 CPU, 0 GPU, 1000 memory, max_count=10
        node_group_config1 = autoscaler_pb2.NodeGroupConfig()
        node_group_config1.resources["CPU"] = 4
        node_group_config1.resources["memory"] = 1000
        node_group_config1.max_count = 10
        cluster_config.node_group_configs.append(node_group_config1)

        # Node type 2: 8 CPU, 2 GPU, 2000 memory, max_count=0 (should be skipped)
        node_group_config2 = autoscaler_pb2.NodeGroupConfig()
        node_group_config2.resources["CPU"] = 8
        node_group_config2.resources["GPU"] = 2
        node_group_config2.resources["memory"] = 2000
        node_group_config2.max_count = 0  # This should be skipped
        cluster_config.node_group_configs.append(node_group_config2)

        # Only the first node type should be discovered
        expected = {
            _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000): 0,
        }

        with patch("ray.nodes", return_value=node_table):
            with patch(
                "ray._private.state.state.get_cluster_config",
                return_value=cluster_config,
            ):
                result = _get_node_resource_spec_and_count()
                assert result == expected

    def test_get_node_resource_spec_and_count_missing_all_resources(self):
        """Regression test for nodes with empty resources (ie missing CPU, GPU, and memory keys entirely)."""

        # Simulate a node with no standard resources defined
        node_empty_resources = {
            "Alive": True,
            "Resources": {
                "dummy_resource": 1,
            },
        }

        node_table = [
            {
                "Resources": self._head_node,
                "Alive": True,
            },
            node_empty_resources,
        ]

        # Expect everything to default to 0
        expected = {_NodeResourceSpec.of(cpu=0, gpu=0, mem=0): 1}

        with (
            patch("ray.nodes", return_value=node_table),
            patch(
                "ray._private.state.state.get_cluster_config",
                return_value=None,
            ),
        ):
            result = _get_node_resource_spec_and_count()
            assert result == expected

    def test_try_scale_up_respects_cpu_limits(self):
        """Test that cluster autoscaling respects user-configured CPU limits."""
        scale_up_threshold = 0.75
        scale_up_delta = 1
        # High utilization to trigger scaling
        utilization = ExecutionResources(cpu=0.9, gpu=0.5, object_store_memory=0.5)

        # Each node has 4 CPUs, 2 nodes = 8 CPUs total
        # Requesting 3 nodes (2+1 delta) would be 12 CPUs, exceeding limit
        resource_spec = _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000)
        fake_coordinator = FakeAutoscalingCoordinator()

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=ExecutionResources(cpu=8),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {resource_spec: 2},
        )

        autoscaler.try_trigger_scaling()

        # With limit of 8 CPUs, should only request 2 bundles (8 CPUs), not 3 (12 CPUs)
        resources_allocated = autoscaler.get_total_resources()
        assert resources_allocated.cpu == 8
        assert resources_allocated.memory == 2000

    def test_try_scale_up_respects_gpu_limits(self):
        """Test that cluster autoscaling respects user-configured GPU limits."""
        scale_up_threshold = 0.75
        scale_up_delta = 1
        # High utilization to trigger scaling
        utilization = ExecutionResources(cpu=0.5, gpu=0.9, object_store_memory=0.5)

        # Each node has 1 GPU, 2 nodes = 2 GPUs total
        # Requesting 3 nodes (2+1 delta) would be 3 GPUs, exceeding limit
        resource_spec = _NodeResourceSpec.of(cpu=4, gpu=1, mem=1000)
        fake_coordinator = FakeAutoscalingCoordinator()

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=ExecutionResources(gpu=2),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {resource_spec: 2},
        )

        autoscaler.try_trigger_scaling()

        # With limit of 2 GPUs, should only request 2 bundles, not 3
        resources_allocated = autoscaler.get_total_resources()
        assert resources_allocated.gpu == 2
        assert resources_allocated.cpu == 8

    def test_try_scale_up_respects_memory_limits(self):
        """Test that cluster autoscaling respects user-configured memory limits."""
        scale_up_threshold = 0.75
        scale_up_delta = 1
        # High utilization to trigger scaling
        utilization = ExecutionResources(cpu=0.5, gpu=0.5, object_store_memory=0.9)

        # Each node has 2000 memory, 2 nodes = 4000 memory total
        # Requesting 3 nodes (2+1 delta) would be 6000 memory, exceeding limit
        resource_spec = _NodeResourceSpec.of(cpu=4, gpu=0, mem=2000)
        fake_coordinator = FakeAutoscalingCoordinator()

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=ExecutionResources(memory=4000),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {resource_spec: 2},
        )

        autoscaler.try_trigger_scaling()

        # With limit of 4000 memory, should only request 2 bundles, not 3
        resources_allocated = autoscaler.get_total_resources()
        assert resources_allocated.memory == 4000
        assert resources_allocated.cpu == 8

    def test_try_scale_up_no_limits(self):
        """Test that cluster autoscaling works normally when no limits are set."""
        scale_up_threshold = 0.75
        scale_up_delta = 1
        # High utilization to trigger scaling
        utilization = ExecutionResources(cpu=0.9, gpu=0.5, object_store_memory=0.5)

        resource_spec = _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000)
        fake_coordinator = FakeAutoscalingCoordinator()

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=ExecutionResources.inf(),
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_delta,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {resource_spec: 2},
        )

        autoscaler.try_trigger_scaling()

        # With no limits, all 3 bundles (2+1 delta) should be requested
        resources_allocated = autoscaler.get_total_resources()
        assert resources_allocated.cpu == 12  # 3 nodes * 4 CPUs
        assert resources_allocated.memory == 3000  # 3 nodes * 1000 memory


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
