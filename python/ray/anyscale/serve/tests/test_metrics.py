import sys
from typing import Dict, Optional

import pytest

import ray
from ray import serve
from ray._private.test_utils import fetch_prometheus_metrics, wait_for_condition
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.test_utils import check_num_alive_nodes
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import ServeDeploySchema


def check_sum_metric_eq(
    metric_name: str,
    expected: float,
    tags: Optional[Dict[str, str]] = None,
    metrics_port: int = 9999,
) -> bool:
    metrics = fetch_prometheus_metrics([f"localhost:{metrics_port}"])
    metric_samples = metrics.get(metric_name, None)
    if metric_samples is None:
        metric_sum = 0
    else:
        metric_samples = [
            sample for sample in metric_samples if tags.items() <= sample.labels.items()
        ]
        metric_sum = sum(sample.value for sample in metric_samples)

    # Check the metrics sum to the expected number
    assert (
        metric_sum == expected
    ), f"The following metrics don't sum to {expected}: {metric_samples}. {metrics}"

    # # For debugging
    if metric_samples:
        print(f"The following sum to {expected} for '{metric_name}' and tags {tags}:")
        for sample in metric_samples:
            print(sample)

    return True


@serve.deployment
class WaitForSignal:
    async def __call__(self):
        signal = ray.get_actor("signal123")
        await signal.wait.remote()


@serve.deployment
class Router:
    def __init__(self, handles):
        self.handles = handles

    async def __call__(self, index: int):
        return await self.handles[index - 1].remote()


@ray.remote
def call(deployment_name, app_name, *args):
    handle = DeploymentHandle(deployment_name, app_name)
    handle.remote(*args)


@ray.remote
class CallActor:
    def __init__(self, deployment_name: str, app_name: str):
        self.handle = DeploymentHandle(deployment_name, app_name)

    async def call(self, *args):
        await self.handle.remote(*args)


@pytest.mark.skipif(
    not RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY, reason="Need compact strategy."
)
def test_node_compactions(autoscaling_cluster_with_metrics):
    client = _get_global_client()
    check_sum_metric_eq(metric_name="serve_num_compacted_nodes", expected=0)

    import_path = "ray.anyscale.serve.tests.test_telemetry.app_A"
    config = {
        "applications": [
            {
                "name": "A",
                "import_path": import_path,
                "route_prefix": "/a",
                "deployments": [{"name": "Noop"}],
            },
            {
                "name": "B",
                "import_path": import_path,
                "route_prefix": "/b",
                "deployments": [{"name": "Noop", "ray_actor_options": {"num_cpus": 2}}],
            },
        ]
    }

    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    client._wait_for_application_running("B")
    wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker node

    config["applications"][0]["deployments"][0]["num_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**config))
    client._wait_for_application_running("A")
    wait_for_condition(check_num_alive_nodes, target=3)  # 1 head + 2 worker node

    # Delete app 'B'. One node should be compacted.
    del config["applications"][1]
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(check_num_alive_nodes, target=2)  # 1 head + 1 worker node

    wait_for_condition(
        check_sum_metric_eq, metric_name="serve_num_compacted_nodes", expected=0
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
