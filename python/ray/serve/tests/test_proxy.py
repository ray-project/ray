import os
import sys

import grpc
import pytest

import ray
from ray import serve
from ray._common.network_utils import build_address
from ray._common.test_utils import wait_for_condition
from ray.actor import ActorHandle
from ray.cluster_utils import Cluster
from ray.serve._private.constants import (
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import (
    ping_grpc_healthz,
    ping_grpc_list_applications,
    request_with_retries,
)
from ray.serve.config import gRPCOptions
from ray.serve.context import _get_global_client
from ray.serve.generated import serve_pb2
from ray.serve.schema import ProxyStatus, ServeInstanceDetails
from ray.tests.conftest import call_ray_stop_only  # noqa: F401
from ray.util.state import list_actors


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


class TestTimeoutKeepAliveConfig:
    """Test setting keep_alive_timeout_s in config and env."""

    def get_proxy_actor(self) -> ActorHandle:
        [proxy_actor] = list_actors(filters=[("class_name", "=", "ProxyActor")])
        return ray.get_actor(proxy_actor.name, namespace=SERVE_NAMESPACE)

    def test_default_keep_alive_timeout_s(self, ray_shutdown):
        """Test when no keep_alive_timeout_s is set.

        When the keep_alive_timeout_s is not set, the uvicorn keep alive is 5.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s
            == DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S
        )

    def test_set_keep_alive_timeout_in_http_configs(self, ray_shutdown):
        """Test when keep_alive_timeout_s is in http configs.

        When the keep_alive_timeout_s is set in http configs, the uvicorn keep alive
        is set correctly.
        """
        keep_alive_timeout_s = 222
        serve.start(http_options={"keep_alive_timeout_s": keep_alive_timeout_s})
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s
            == keep_alive_timeout_s
        )

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_keep_alive_timeout_in_env(self, ray_instance, ray_shutdown):
        """Test when keep_alive_timeout_s is in env.

        When the keep_alive_timeout_s is set in env, the uvicorn keep alive
        is set correctly.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s == 333
        )

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_timeout_keep_alive_in_both_config_and_env(
        self, ray_instance, ray_shutdown
    ):
        """Test when keep_alive_timeout_s is in both http configs and env.

        When the keep_alive_timeout_s is set in env, the uvicorn keep alive
        is set to the one in env.
        """
        keep_alive_timeout_s = 222
        serve.start(http_options={"keep_alive_timeout_s": keep_alive_timeout_s})
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s == 333
        )


def test_grpc_proxy_on_draining_nodes(ray_cluster):
    """Test gRPC request on the draining node.

    When there are no replicas on head node and some replicas on the worker node, the
    ListApplications and Healthz methods should respond successfully. When there are
    no replicas on any nodes, ListApplications and Healthz methods should continue to
    succeeding on the head node. But should return draining response on the worker node.

    Also note, this is to ensure the previous fix to serve downscaling also applies to
    gRPC proxy. Head node will not need to be downscaled and never be in the draining
    state. Worker nodes will be in draining when there is no replicas. We will fail the
    health check in this case, so ALB knows not to route to this node anymore.
    """
    head_node_grpc_port = 9000
    worker_node_grpc_port = 9001

    # Setup worker gRPC proxy to be pointing to port 9001. Head node gRPC proxy will
    # continue to be pointing to the default port 9000.
    os.environ["TEST_WORKER_NODE_GRPC_PORT"] = str(worker_node_grpc_port)

    # Set up a cluster with 2 nodes.
    cluster = ray_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Start serve with gRPC proxy
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]
    serve.start(
        http_options={"location": "EveryNode"},
        grpc_options=gRPCOptions(
            port=head_node_grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )

    # Deploy 2 replicas, both should be on the worker node.
    @serve.deployment(num_replicas=2)
    class HelloModel:
        def __call__(self):
            return serve_pb2.UserDefinedResponse(greeting="hello")

    model = HelloModel.bind()
    app_name = "app1"
    serve.run(model, name=app_name)

    # Ensure worker node has both replicas.
    def check_replicas_on_worker_nodes():
        return (
            len(
                {
                    a.node_id
                    for a in list_actors(address=cluster.address)
                    if a.class_name.startswith("ServeReplica")
                }
            )
            == 1
        )

    wait_for_condition(check_replicas_on_worker_nodes)

    # Ensure total actors of 2 proxies, 1 controller, and 2 replicas, and 2 nodes exist.
    wait_for_condition(lambda: len(list_actors(address=cluster.address)) == 5)
    assert len(ray.nodes()) == 2

    # Set up gRPC channels.
    head_node_channel = grpc.insecure_channel(
        build_address("localhost", head_node_grpc_port)
    )
    worker_node_channel = grpc.insecure_channel(
        build_address("localhost", worker_node_grpc_port)
    )

    # Ensures ListApplications method on the head node is succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=head_node_channel, app_names=[app_name]
    )

    # Ensures Healthz method on the head node is succeeding.
    ping_grpc_healthz(head_node_channel)

    # Ensures ListApplications method on the worker node is succeeding.
    wait_for_condition(
        ping_grpc_list_applications,
        channel=worker_node_channel,
        app_names=[app_name],
        timeout=30,
    )

    # Ensures Healthz method on the worker node is succeeding.
    ping_grpc_healthz(worker_node_channel)

    # Delete the deployment should bring the active actors down to 3 and drop
    # replicas on all nodes.
    serve.delete(name=app_name)

    wait_for_condition(
        lambda: len(
            list_actors(address=cluster.address, filters=[("STATE", "=", "ALIVE")])
        )
        == 3,
    )

    # Ensures ListApplications method on the head node is succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=head_node_channel, app_names=[]
    )

    # Ensures Healthz method on the head node is succeeding.
    ping_grpc_healthz(head_node_channel)

    # Ensures ListApplications method on the worker node is draining.
    wait_for_condition(
        ping_grpc_list_applications,
        channel=worker_node_channel,
        app_names=[],
        test_draining=True,
    )

    # Ensures Healthz method on the worker node is draining.
    ping_grpc_healthz(worker_node_channel, test_draining=True)


def test_drain_and_undrain_http_proxy_actors(
    monkeypatch, shutdown_ray, call_ray_stop_only  # noqa: F811
):
    """Test the state transtion of the proxy actor between
    HEALTHY, DRAINING and DRAINED
    """
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "10")

    cluster = Cluster()
    head_node = cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(address=head_node.address)
    serve.start(http_options={"location": "EveryNode"})

    @serve.deployment
    class HelloModel:
        def __call__(self):
            return "hello"

    serve.run(HelloModel.options(num_replicas=2).bind())

    # 3 proxies, 1 controller, 2 replicas.
    wait_for_condition(lambda: len(list_actors()) == 6)
    assert len(ray.nodes()) == 3

    client = _get_global_client()
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    proxy_actor_ids = {proxy.actor_id for _, proxy in serve_details.proxies.items()}

    assert len(proxy_actor_ids) == 3

    serve.run(HelloModel.options(num_replicas=1).bind())
    # 1 proxy should be draining

    def check_proxy_status(proxy_status_to_count):
        serve_details = ServeInstanceDetails(
            **ray.get(client._controller.get_serve_instance_details.remote())
        )
        proxy_status_list = [proxy.status for _, proxy in serve_details.proxies.items()]
        print("all proxies!!!", [proxy for _, proxy in serve_details.proxies.items()])
        current_status = {
            status: proxy_status_list.count(status) for status in proxy_status_list
        }
        return current_status == proxy_status_to_count, current_status

    wait_for_condition(
        condition_predictor=check_proxy_status,
        proxy_status_to_count={ProxyStatus.HEALTHY: 2, ProxyStatus.DRAINING: 1},
    )

    serve.run(HelloModel.options(num_replicas=2).bind())
    # The draining proxy should become healthy.
    wait_for_condition(
        condition_predictor=check_proxy_status,
        proxy_status_to_count={ProxyStatus.HEALTHY: 3},
    )
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )

    assert {
        proxy.actor_id for _, proxy in serve_details.proxies.items()
    } == proxy_actor_ids

    serve.run(HelloModel.options(num_replicas=1).bind())
    # 1 proxy should be draining and eventually be drained.
    wait_for_condition(
        condition_predictor=check_proxy_status,
        timeout=40,
        proxy_status_to_count={ProxyStatus.HEALTHY: 2},
    )

    # Clean up serve.
    serve.shutdown()


def _kill_http_proxies():
    http_proxies = ray.get(
        serve.context._global_client._controller.get_proxies.remote()
    )
    for http_proxy in http_proxies.values():
        ray.kill(http_proxy, no_restart=False)


def test_http_proxy_failure(serve_instance):
    @serve.deployment(name="proxy_failure")
    def function(_):
        return "hello1"

    serve.run(function.bind())

    assert request_with_retries(timeout=1.0).text == "hello1"

    for _ in range(10):
        response = request_with_retries(timeout=30)
        assert response.text == "hello1"

    _kill_http_proxies()

    def function2(_):
        return "hello2"

    serve.run(function.options(func_or_class=function2).bind())

    def check_new():
        for _ in range(10):
            response = request_with_retries(timeout=30)
            if response.text != "hello2":
                return False
        return True

    wait_for_condition(check_new)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
