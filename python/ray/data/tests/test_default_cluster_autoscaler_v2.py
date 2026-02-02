import logging
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

    def test_low_utilization_sends_current_allocation(self):
        """Test that low utilization sends current allocation.

        Test scenario:
        1. Dataset has already been allocated resources (1 nodes)
        2. Utilization is low (0%, below default threshold)
        3. Should send current allocation to preserve resource footprint
        """
        utilization: ExecutionResources = ...

        class FakeUtilizationGauge(ResourceUtilizationGauge):
            def observe(self):
                pass

            def get(self):
                return utilization

        node_resource_spec = _NodeResourceSpec.of(cpu=1, gpu=0, mem=0)
        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=ExecutionResources.inf(),
            execution_id="test_execution_id",
            resource_utilization_calculator=FakeUtilizationGauge(),
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=FakeAutoscalingCoordinator(),
            get_node_counts=lambda: {node_resource_spec: 0},
        )

        # Trigger scaling with high utilization. The cluster autoscaler should request
        # one node.
        utilization = ExecutionResources(cpu=1)
        autoscaler.try_trigger_scaling()
        assert autoscaler.get_total_resources() == ExecutionResources(cpu=1)

        # Trigger scaling with low utilization. The cluster autoscaler should re-request
        # one node rather than no resources.
        utilization = ExecutionResources(cpu=0)
        autoscaler.try_trigger_scaling()
        assert autoscaler.get_total_resources() == ExecutionResources(cpu=1)

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

    @pytest.mark.parametrize(
        "resource_limits,node_spec,existing_nodes,scale_up_increment,expected_nodes",
        [
            # CPU limit: 8 CPUs allows 2 nodes (8 CPUs), not 3 (12 CPUs)
            (
                ExecutionResources.for_limits(cpu=8),
                _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000),
                2,
                1,
                2,
            ),
            # GPU limit: 2 GPUs allows 2 nodes (2 GPUs), not 3 (3 GPUs)
            (
                ExecutionResources.for_limits(gpu=2),
                _NodeResourceSpec.of(cpu=4, gpu=1, mem=1000),
                2,
                1,
                2,
            ),
            # Memory limit: 4000 allows 2 nodes (4000 mem), not 3 (6000 mem)
            (
                ExecutionResources.for_limits(memory=4000),
                _NodeResourceSpec.of(cpu=4, gpu=0, mem=2000),
                2,
                1,
                2,
            ),
            # No limits: all 3 nodes (2 existing + 1 delta) should be requested
            (
                ExecutionResources.inf(),
                _NodeResourceSpec.of(cpu=4, gpu=0, mem=1000),
                2,
                1,
                3,
            ),
        ],
    )
    def test_try_scale_up_respects_resource_limits(
        self,
        resource_limits,
        node_spec,
        existing_nodes,
        scale_up_increment,
        expected_nodes,
    ):
        """Test that cluster autoscaling respects user-configured resource limits."""
        scale_up_threshold = 0.75
        # High utilization to trigger scaling
        utilization = ExecutionResources(cpu=0.9, gpu=0.9, object_store_memory=0.9)
        fake_coordinator = FakeAutoscalingCoordinator()

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=resource_limits,
            execution_id="test_execution_id",
            cluster_scaling_up_delta=scale_up_increment,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {node_spec: existing_nodes},
        )

        autoscaler.try_trigger_scaling()

        resources_allocated = autoscaler.get_total_resources()
        assert resources_allocated.cpu == node_spec.cpu * expected_nodes
        assert resources_allocated.gpu == node_spec.gpu * expected_nodes
        assert resources_allocated.memory == node_spec.mem * expected_nodes

    def test_try_scale_up_respects_resource_limits_heterogeneous_nodes(self):
        """Test that smaller bundles are included even when larger bundles exceed limits.

        This tests a scenario where:
        1. Initial cluster (1 small node, 4 CPUs) is within the budget (10 CPUs)
        2. Scaling up is triggered due to high utilization
        3. The autoscaler wants to add both large and small nodes
        4. Only small nodes are requested because large nodes would exceed the limit
        """
        # CPU limit of 10 allows the initial state (4 CPUs) plus room for growth
        resource_limits = ExecutionResources.for_limits(cpu=10)

        large_node_spec = _NodeResourceSpec.of(cpu=8, gpu=1, mem=4000)
        small_node_spec = _NodeResourceSpec.of(cpu=4, gpu=0, mem=2000)

        scale_up_threshold = 0.75
        utilization = ExecutionResources(cpu=0.9, gpu=0.9, object_store_memory=0.9)
        fake_coordinator = FakeAutoscalingCoordinator()

        # Initial cluster: 1 small node (4 CPUs) - within the 10 CPU budget
        # Node types available: large (8 CPUs) and small (4 CPUs)
        def get_heterogeneous_nodes():
            return {
                large_node_spec: 0,  # 0 existing large nodes
                small_node_spec: 1,  # 1 existing small node (4 CPUs)
            }

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=resource_limits,
            execution_id="test_execution_id",
            cluster_scaling_up_delta=1,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=get_heterogeneous_nodes,
        )

        autoscaler.try_trigger_scaling()

        resources_allocated = autoscaler.get_total_resources()
        # With delta=1:
        #   - Active bundles: 1 small (4 CPUs) - existing nodes, always included
        #   - Pending bundles: 1 small (4 CPUs) + 1 large (8 CPUs) - scale-up delta
        # After capping to 10 CPUs:
        #   - Active: 4 CPUs (always included)
        #   - Sorted pending: [small (4), large (8)]
        #   - Add small: 4 + 4 = 8 CPUs ✓
        #   - Add large: 8 + 8 = 16 CPUs ✗ (exceeds limit)
        # Result: 2 small bundles (8 CPUs)
        # Ray autoscaler would see: need 2 small nodes, have 1 → spin up 1 more
        assert resources_allocated.cpu == 8, (
            f"Expected 8 CPUs (2 small node bundles), got {resources_allocated.cpu}. "
            "Smaller bundles should be included even when larger ones exceed limits."
        )
        assert resources_allocated.gpu == 0
        assert resources_allocated.memory == 4000

    def test_try_scale_up_existing_nodes_prioritized_over_delta(self):
        """Test that existing node bundles are prioritized over scale-up delta bundles.

        This tests a scenario where:
        - Large existing node: 1 node at 6 CPUs (currently allocated)
        - Small node type available: can add nodes at 2 CPUs each
        - User limit: 8 CPUs
        - Scale-up delta: 2 (want to add 2 small nodes)

        The existing large node (6 CPUs) should always be included, and only
        scale-up bundles that fit within the remaining budget should be added.
        Without this prioritization, smaller scale-up bundles could crowd out
        the representation of existing nodes.
        """
        resource_limits = ExecutionResources.for_limits(cpu=8)

        large_node_spec = _NodeResourceSpec.of(cpu=6, gpu=0, mem=3000)
        small_node_spec = _NodeResourceSpec.of(cpu=2, gpu=0, mem=1000)

        scale_up_threshold = 0.75
        utilization = ExecutionResources(cpu=0.9, gpu=0.9, object_store_memory=0.9)
        fake_coordinator = FakeAutoscalingCoordinator()

        # Existing cluster: 1 large node (6 CPUs)
        # Scale-up delta: 2 (want to add 2 of each node type)
        def get_node_counts():
            return {
                large_node_spec: 1,  # 1 existing large node (6 CPUs)
                small_node_spec: 0,  # 0 existing small nodes
            }

        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            resource_limits=resource_limits,
            execution_id="test_execution_id",
            cluster_scaling_up_delta=2,
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            cluster_scaling_up_util_threshold=scale_up_threshold,
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=get_node_counts,
        )

        autoscaler.try_trigger_scaling()

        resources_allocated = autoscaler.get_total_resources()
        # Active bundles: 1 large (6 CPUs) - must be included
        # Pending bundles: 2 large (12 CPUs) + 2 small (4 CPUs) = delta requests
        # After capping to 8 CPUs:
        #   - Active: 6 CPUs (always included)
        #   - Remaining budget: 2 CPUs
        #   - Sorted pending: [small (2), small (2), large (6), large (6)]
        #   - Add small: 6 + 2 = 8 CPUs ✓
        #   - Add another small: 8 + 2 = 10 CPUs ✗
        # Result: 1 large (active) + 1 small (delta) = 8 CPUs
        assert resources_allocated.cpu == 8, (
            f"Expected 8 CPUs (1 existing large + 1 delta small), got {resources_allocated.cpu}. "
            "Existing node bundles should always be included before scale-up delta."
        )
        # Verify we have the large node's resources (it must be included)
        assert resources_allocated.memory >= large_node_spec.mem, (
            f"Existing large node (mem={large_node_spec.mem}) should be included. "
            f"Got total memory={resources_allocated.memory}"
        )

    def test_try_scale_up_logs_info_message(self, propagate_logs, caplog):
        fake_coordinator = FakeAutoscalingCoordinator()
        node_spec = _NodeResourceSpec.of(cpu=1, gpu=0, mem=8 * 1024**3)
        utilization = ExecutionResources(cpu=1, gpu=1, object_store_memory=1)
        autoscaler = DefaultClusterAutoscalerV2(
            resource_manager=MagicMock(),
            execution_id="test_execution_id",
            resource_utilization_calculator=StubUtilizationGauge(utilization),
            min_gap_between_autoscaling_requests_s=0,
            autoscaling_coordinator=fake_coordinator,
            get_node_counts=lambda: {node_spec: 1},
        )

        with caplog.at_level(logging.INFO):
            autoscaler.try_trigger_scaling()

        expected_message = (
            "The utilization of one or more logical resource is higher than the "
            "specified threshold of 75%: CPU=100%, GPU=100%, object_store_memory=100%. "
            "Requesting 1 node(s) of each shape: "
            "[{CPU: 1, GPU: 0, memory: 8.0GiB}: 1 -> 2]"
        )
        log_messages = [record.message for record in caplog.records]
        assert expected_message in log_messages, (
            f"Expected log message not found.\n"
            f"Expected: {expected_message}\n"
            f"Actual logs: {log_messages}"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
