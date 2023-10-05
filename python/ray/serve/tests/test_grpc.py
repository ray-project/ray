import os
import sys

import grpc

# coding: utf-8
import pytest

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.cluster_utils import Cluster
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.config import gRPCOptions
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.tests.common.utils import (
    ping_fruit_stand,
    ping_grpc_another_method,
    ping_grpc_call_method,
    ping_grpc_healthz,
    ping_grpc_list_applications,
    ping_grpc_model_multiplexing,
    ping_grpc_streaming,
)
from ray.serve.tests.test_config_files.grpc_deployment import g, g2


def test_serving_request_through_grpc_proxy(ray_cluster):
    """Test serving request through gRPC proxy.

    When Serve runs with a gRPC deployment, the app should be deployed successfully,
    both ListApplications and Healthz methods returning successful responses, and
    registered gRPC methods are routing to the correct replica and return the correct
    response.
    """
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)

    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]

    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )

    # Ensures the app is not yet deployed.
    app_name = "default"
    deployment_name = "grpc-deployment"
    replica_name = DeploymentID(deployment_name, app_name)
    assert replica_name not in replicas

    channel = grpc.insecure_channel("localhost:9000")

    # Ensures the not found is responding correctly.
    ping_grpc_call_method(channel, app_name, test_not_found=True)

    serve.run(target=g)
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )

    # Ensures the app is deployed.
    assert len(replicas[replica_name]) == 1

    # Ensures ListApplications method succeeding.
    ping_grpc_list_applications(channel, [app_name])

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)

    # Ensures a custom defined method is responding correctly.
    ping_grpc_call_method(channel, app_name)

    # Ensures another custom defined method is responding correctly.
    ping_grpc_another_method(channel, app_name)

    # Ensures model multiplexing is responding correctly.
    ping_grpc_model_multiplexing(channel, app_name)

    # Ensure Streaming method is responding correctly.
    ping_grpc_streaming(channel, app_name)

    serve.run(target=g2)
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )

    # Ensures the app is deployed.
    deployment_name = "grpc-deployment-model-composition"
    replica_name = DeploymentID(deployment_name, app_name)
    assert len(replicas[replica_name]) == 1

    # Ensure model composition is responding correctly.
    ping_fruit_stand(channel, app_name)


def test_serve_start_dictionary_grpc_options(ray_cluster):
    """Test serve able to start with dictionary grpc_options.

    When Serve starts with dictionary grpc_options, it should not throw errors and able
    to serve health check and list applications gRPC requests.
    """
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)

    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]

    serve.start(
        grpc_options={
            "port": grpc_port,
            "grpc_servicer_functions": grpc_servicer_functions,
        },
    )

    channel = grpc.insecure_channel("localhost:9000")

    # Ensures ListApplications method succeeding.
    ping_grpc_list_applications(channel, [])

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)


def test_grpc_proxy_routing_without_metadata(ray_cluster):
    """Test metadata are not required when calling gRPC proxy with only one app.

    When there is only one app deployed, gRPC proxy will route the request to the app
    with or without the metadata. If there are multiple app deployed, without metadata
    will return a notfound response.
    """
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)

    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]

    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )

    app1 = "app1"
    serve.run(target=g, name=app1, route_prefix=f"/{app1}")

    # Ensures the app is not yet deployed.
    deployment_name = "grpc-deployment"
    app1_replica_name = DeploymentID(deployment_name, app1)
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )

    # Ensures the app is deployed.
    assert len(replicas[app1_replica_name]) == 1

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    # Ensures the gRPC Proxy responding correctly without metadata.
    response, call = stub.__call__.with_call(request=request)
    assert call.code() == grpc.StatusCode.OK
    assert response.greeting == "Hello foo from bar"

    # Ensures the gRPC Proxy responding correctly with metadata.
    metadata = (("application", app1),)
    response, call = stub.__call__.with_call(request=request, metadata=metadata)
    assert call.code() == grpc.StatusCode.OK
    assert response.greeting == "Hello foo from bar"

    # Deploy another app.
    app2 = "app2"
    serve.run(target=g2, name=app2, route_prefix=f"/{app2}")
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )
    deployment_name = "grpc-deployment-model-composition"

    # Ensure both apps are deployed
    app2_replica_name = DeploymentID(deployment_name, app2)
    assert len(replicas[app1_replica_name]) == 1
    assert len(replicas[app2_replica_name]) == 1

    # Ensure the gRPC request without metadata will now return not found response.
    with pytest.raises(grpc.RpcError) as exception_info:
        _, _ = stub.__call__.with_call(request=request)
    rpc_error = exception_info.value
    assert rpc_error.code() == grpc.StatusCode.NOT_FOUND
    assert "Application metadata not set" in rpc_error.details()


def test_grpc_proxy_with_request_id(ray_cluster):
    """Test gRPC request with and without request id.

    When no request id is passed, gRPC proxy will respond with a random request id in
    the trailing metadata. When request id is passed, gRPC proxy will respond with the
    original request id.
    """
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)

    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]

    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )

    app1 = "app1"
    serve.run(target=g, name=app1, route_prefix=f"/{app1}")

    # Ensures the app is not yet deployed.
    deployment_name = "grpc-deployment"
    app1_replica_name = DeploymentID(deployment_name, app1)
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )

    # Ensures the app is deployed.
    assert len(replicas[app1_replica_name]) == 1

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    # Ensures the gRPC Proxy returning the original request id.
    custom_request_id = "fake-request-id"
    metadata = (("request_id", custom_request_id),)
    response, call = stub.__call__.with_call(request=request, metadata=metadata)
    response_request_id = None
    for key, value in call.trailing_metadata():
        if key == "request_id":
            response_request_id = value
            break
    assert custom_request_id == response_request_id

    # Ensures the gRPC Proxy returning a new request id.
    response, call = stub.__call__.with_call(request=request)
    response_request_id = None
    for key, value in call.trailing_metadata():
        if key == "request_id":
            response_request_id = value
            break
    assert custom_request_id != response_request_id


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
    cluster = Cluster()
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
    serve.run(target=model, name=app_name)

    # Ensure worker node has both replicas.
    def check_replicas_on_worker_nodes():
        _actors = ray._private.state.actors().values()
        replica_nodes = [
            a["Address"]["NodeID"]
            for a in _actors
            if a["ActorClassName"].startswith("ServeReplica")
        ]
        return len(set(replica_nodes)) == 1

    wait_for_condition(check_replicas_on_worker_nodes)

    # Ensure total actors of 2 proxies, 1 controller, and 2 replicas, and 2 nodes exist.
    wait_for_condition(lambda: len(ray._private.state.actors()) == 5)
    assert len(ray.nodes()) == 2

    # Set up gRPC channels.
    head_node_channel = grpc.insecure_channel(f"localhost:{head_node_grpc_port}")
    worker_node_channel = grpc.insecure_channel(f"localhost:{worker_node_grpc_port}")

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

    def _check():
        _actors = ray._private.state.actors().values()
        return (
            len(
                list(
                    filter(
                        lambda a: a["State"] == "ALIVE",
                        _actors,
                    )
                )
            )
            == 3
        )

    wait_for_condition(_check)

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


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1",
        },
    ],
    indirect=True,
)
@pytest.mark.parametrize("streaming", [False, True])
def test_grpc_proxy_timeouts(ray_instance, ray_shutdown, streaming: bool):
    """Test gRPC request timed out.

    When the request timed out, gRPC proxy should return timeout response for both
    unary and streaming request.
    """
    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]

    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )

    signal_actor = SignalActor.remote()

    @serve.deployment
    class HelloModel:
        def __call__(self, user_message):
            ray.get(signal_actor.wait.remote())
            return serve_pb2.UserDefinedResponse(greeting="hello")

        def Streaming(self, user_message):
            for i in range(10):
                ray.get(signal_actor.wait.remote())
                yield serve_pb2.UserDefinedResponse(greeting="hello")

    serve.run(HelloModel.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    timeout_response = "timed out after 0.1s."

    if streaming:
        # Ensure streaming request respond with timeout response when running longer
        # than the serve request timeout setting
        with pytest.raises(grpc.RpcError) as exception_info:
            list(stub.Streaming(request=request))
    else:
        # Ensure unary request respond with timeout response when running longer than
        # the serve request timeout setting
        with pytest.raises(grpc.RpcError) as exception_info:
            stub.__call__(request=request)

    rpc_error = exception_info.value
    assert rpc_error.code() == grpc.StatusCode.CANCELLED
    assert timeout_response in rpc_error.details()

    # Unblock the handlers to avoid graceful shutdown time.
    ray.get(signal_actor.send.remote())


@pytest.mark.parametrize("streaming", [False, True])
def test_grpc_proxy_internal_error(ray_instance, ray_shutdown, streaming: bool):
    """Test gRPC request error out.

    When the request error out, gRPC proxy should return INTERNAL status and the error
    message in the response for both unary and streaming request.
    """
    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
    ]

    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )
    error_message = "test error case"

    @serve.deployment()
    class HelloModel:
        def __call__(self, user_message):
            raise RuntimeError(error_message)

        def Streaming(self, user_message):
            raise RuntimeError(error_message)

    model = HelloModel.bind()
    app_name = "app1"
    serve.run(target=model, name=app_name)

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    if streaming:
        rpc_error = stub.Streaming(request=request)
    else:
        with pytest.raises(grpc.RpcError) as exception_info:
            _ = stub.__call__(request=request)
        rpc_error = exception_info.value

    assert rpc_error.code() == grpc.StatusCode.INTERNAL
    assert error_message in rpc_error.details()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
