import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.test_utils import check_apps_running
from ray.serve.config import GangSchedulingConfig
from ray.tests.conftest import *  # noqa


class TestGangScheduling:
    """Tests for gang scheduling with placement groups."""

    def test_sufficient_resources(self, ray_cluster):
        """Verifies that gang scheduling succeeds when cluster has sufficient resources."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(
            num_replicas=8,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=4),
        )
        class GangDeployment:
            def __call__(self):
                return ray.get_runtime_context().get_node_id()

        handle = serve.run(GangDeployment.bind(), name="gang_app_success")
        wait_for_condition(
            check_apps_running,
            apps=["gang_app_success"],
        )

        # Verify all replicas are running and responding
        refs = [handle.remote() for _ in range(8)]
        results = [ref.result() for ref in refs]
        assert len(results) == 8

        serve.delete("gang_app_success")
        serve.shutdown()

    def test_sufficient_resources_with_options(self, ray_cluster):
        """Verifies gang scheduling via .options() succeeds and responds to requests."""
        cluster = ray_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start()

        @serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0})
        class GangDeployment:
            def __call__(self):
                return ray.get_runtime_context().get_node_id()

        app = GangDeployment.options(
            num_replicas=8,
            ray_actor_options={"num_cpus": 0.25},
            gang_scheduling_config=GangSchedulingConfig(gang_size=4),
        ).bind()

        handle = serve.run(app, name="gang_app_options")
        wait_for_condition(
            check_apps_running,
            apps=["gang_app_options"],
        )

        # Verify all replicas are running and responding
        refs = [handle.remote() for _ in range(8)]
        results = [ref.result() for ref in refs]
        assert len(results) == 8

        serve.delete("gang_app_options")
        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
