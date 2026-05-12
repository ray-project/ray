import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.test_utils import check_apps_running
from ray.serve.schema import DeploymentStatus, ReplicaState
from ray.tests.conftest import *  # noqa


class TestScaleDownReplicaSelection:
    @staticmethod
    def _quick_upscale_config():
        return {
            "target_ongoing_requests": 0.01,
            "upscale_delay_s": 0.05,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 0.5,
            "downscale_delay_s": 2,
            "aggregation_function": "max",
        }

    @staticmethod
    def _wait_for_upscale(
        expected_num_replicas: int,
        handle,
        timeout: int = 30,
    ):
        replica_tag_set = set()

        def check_new_replica():
            replica_tag_set.add(handle.get_info.remote().result()["replica_tag"])
            return len(replica_tag_set) == expected_num_replicas

        wait_for_condition(check_new_replica, timeout=timeout, retry_interval_ms=50)

    @staticmethod
    def _wait_until_min_replica(
        app_name: str, deployment_name: str, min_replicas: int, timeout: int = 30
    ):
        def check_min_replicas():
            deployment_info = (
                serve.status().applications[app_name].deployments[deployment_name]
            )
            return (
                deployment_info.status == DeploymentStatus.HEALTHY
                and deployment_info.replica_states[ReplicaState.RUNNING] == min_replicas
            )

        wait_for_condition(check_min_replicas, timeout=timeout)

    @staticmethod
    def _deploy_test_app(
        app_name: str,
        deployment_name: str = "test_deployment",
        *,
        ray_actor_options: dict,
        placement_group_bundles: list[dict] = None,
        placement_group_bundle_label_selector: list[dict] = None,
        autoscaling_config: dict = None,
    ):
        @serve.deployment(name=deployment_name)
        class TestDeployment:
            async def get_info(self):
                return {
                    "node_id": ray.get_runtime_context().get_node_id(),
                    "replica_tag": serve.get_replica_context().replica_tag,
                }

        return serve.run(
            TestDeployment.options(
                ray_actor_options=ray_actor_options,
                placement_group_bundles=placement_group_bundles,
                placement_group_bundle_label_selector=placement_group_bundle_label_selector,
                autoscaling_config=autoscaling_config,
            ).bind(),
            name=app_name,
            route_prefix=f"/{app_name}",
        )

    def test_downscale_fallback_node(self, ray_cluster):
        cluster = ray_cluster

        primary_label = {"type": "primary"}
        fallback_label = {"type": "fallback"}

        ray_actor_options = {
            "num_cpus": 0.25,
            "label_selector": primary_label,
            "fallback_strategy": [{"label_selector": fallback_label}],
        }

        # Both nodes get equal capacity (1 CPU each = 4 replicas at 0.25 CPU)
        # so that priority #4 (fewer replicas per node) doesn't confound
        # the test for priority #3 (fallback nodes removed first).
        num_replicas_per_node = 4

        cluster.add_node(num_cpus=0)
        cluster.wait_for_nodes()
        fallback_node = cluster.add_node(
            num_cpus=1,
            labels=fallback_label,
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()
        app_name = "downscale_fallback_app"
        deployment_name = "test_deployment"

        fallback_node_id = fallback_node.node_id

        try:
            handle = self._deploy_test_app(
                app_name,
                ray_actor_options=ray_actor_options,
                autoscaling_config={
                    "min_replicas": 1,
                    "max_replicas": num_replicas_per_node * 2,
                    **self._quick_upscale_config(),
                },
            )
            wait_for_condition(check_apps_running, apps=[app_name])

            primary_node = cluster.add_node(
                num_cpus=1,
                labels=primary_label,
            )
            cluster.wait_for_nodes()
            primary_node_id = primary_node.node_id

            # The first replica is always the fallback node
            assert handle.get_info.remote().result()["node_id"] == fallback_node_id

            # After calling the deployment will scale up
            # resulting in equal replicas on the fallback and primary nodes.
            # After some time, the deployment will scale down to min replica.
            self._wait_for_upscale(
                expected_num_replicas=num_replicas_per_node * 2,
                handle=handle,
                timeout=30,
            )
            self._wait_until_min_replica(
                app_name=app_name,
                deployment_name=deployment_name,
                min_replicas=1,
                timeout=30,
            )
            # Replicas on the fallback node should be removed first (priority #3),
            # so the remaining replica should be on the primary node.
            assert handle.get_info.remote().result()["node_id"] == primary_node_id
        finally:
            serve.shutdown()

    # TODO: Add test for downscale placement group fallback_strategy when it's added to deployment options.

    def test_downscale_prefers_nodes_with_fewer_total_replicas(self, ray_cluster):
        cluster = ray_cluster
        cluster.add_node(num_cpus=0)
        cluster.wait_for_nodes()
        primary_label = {"type": "primary"}
        first_node = cluster.add_node(
            num_cpus=1,
            labels=primary_label,
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        ray_actor_options = {"num_cpus": 0}
        placement_group_bundles = [{"CPU": 0.25}] * 4
        placement_group_bundle_label_selector = [primary_label]
        app_name = "downscale_fewer_total_replicas_app"
        deployment_name = "test_deployment"
        first_node_id = first_node.node_id

        try:
            handle = self._deploy_test_app(
                app_name,
                deployment_name=deployment_name,
                ray_actor_options=ray_actor_options,
                placement_group_bundles=placement_group_bundles,
                placement_group_bundle_label_selector=placement_group_bundle_label_selector,
                autoscaling_config={
                    "min_replicas": 1,
                    "max_replicas": 3,
                    **self._quick_upscale_config(),
                },
            )
            wait_for_condition(check_apps_running, apps=[app_name])

            second_node = cluster.add_node(
                num_cpus=2,
                labels=primary_label,
            )
            cluster.wait_for_nodes()
            second_node_id = second_node.node_id

            # The first replica is always the first node
            assert handle.get_info.remote().result()["node_id"] == first_node_id
            # After calling the deployment will scale up
            # resulting in some replicas on the first node and some replicas on the second node.
            # After some time, the deployment will scale down.
            self._wait_for_upscale(
                expected_num_replicas=3,
                handle=handle,
                timeout=30,
            )
            self._wait_until_min_replica(
                app_name=app_name,
                deployment_name=deployment_name,
                min_replicas=1,
                timeout=30,
            )
            # First node has fewer total replicas, so it is removed first
            # (priority #4). Remaining replica should be on the 2nd node.
            assert handle.get_info.remote().result()["node_id"] == second_node_id
        finally:
            serve.shutdown()

    def test_downscale_prefers_not_head_node(self, ray_cluster):
        """Head node is never relinquished, even when it would otherwise be removed first.

        The head node has only 1 replica, matches only the fallback label,
        and is older — so priorities #3, #4, and #5 all favor removing it.
        This test verifies that priority #2 (keep head node) overrides all
        of them.
        """
        cluster = ray_cluster
        fallback_label = {"type": "fallback"}
        primary_label = {"type": "primary"}
        head_node = cluster.add_node(num_cpus=1, labels=fallback_label)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address, ignore_reinit_error=True)
        serve.start()

        ray_actor_options = {
            "num_cpus": 1,
            "label_selector": primary_label,
            "fallback_strategy": [{"label_selector": fallback_label}],
        }
        app_name = "downscale_prefers_not_head_app"
        deployment_name = "test_deployment"
        head_node_id = head_node.node_id

        try:
            handle = self._deploy_test_app(
                app_name,
                deployment_name=deployment_name,
                ray_actor_options=ray_actor_options,
                autoscaling_config={
                    "min_replicas": 1,
                    "max_replicas": 3,
                    **self._quick_upscale_config(),
                },
            )
            wait_for_condition(check_apps_running, apps=[app_name])

            cluster.add_node(num_cpus=2, labels=primary_label)
            cluster.wait_for_nodes()

            # The first replica lands on the head node (the only node with
            # the fallback label, and no primary node exists yet).
            assert handle.get_info.remote().result()["node_id"] == head_node_id

            # Scale up to 3 replicas (1 head + 2 worker), then back down to 1.
            self._wait_for_upscale(
                expected_num_replicas=3,
                handle=handle,
                timeout=30,
            )
            self._wait_until_min_replica(
                app_name=app_name,
                deployment_name=deployment_name,
                min_replicas=1,
                timeout=30,
            )
            # The head node's replica survives despite being on a fallback
            # node (#3), having fewer replicas (#4), and being the oldest
            # (#5) — priority #2 (never relinquish head node) wins.
            assert handle.get_info.remote().result()["node_id"] == head_node_id
        finally:
            serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
