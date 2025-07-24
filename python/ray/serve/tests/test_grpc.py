import os
import sys
from typing import Any

import grpc

# coding: utf-8
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.test_utils import (
    ping_fruit_stand,
    ping_grpc_another_method,
    ping_grpc_call_method,
    ping_grpc_healthz,
    ping_grpc_list_applications,
    ping_grpc_model_multiplexing,
    ping_grpc_streaming,
    send_signal_on_cancellation,
)
from ray.serve.config import gRPCOptions
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.grpc_util import RayServegRPCContext
from ray.serve.tests.test_config_files.grpc_deployment import g, g2
from ray.util.state import list_actors


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

    channel = grpc.insecure_channel("localhost:9000")

    # Ensures the not found is responding correctly.
    app_name = "default"
    ping_grpc_call_method(channel, app_name, test_not_found=True)

    serve.run(g)

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

    serve.run(g2)

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

    serve.run(g)

    # Ensures ListApplications method succeeding.
    ping_grpc_list_applications(channel, ["default"])

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
    serve.run(g, name=app1, route_prefix=f"/{app1}")

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
    serve.run(g2, name=app2, route_prefix=f"/{app2}")

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
    serve.run(g, name=app1, route_prefix=f"/{app1}")

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
    grpc_request_timeout_s = 0.1

    serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
            request_timeout_s=grpc_request_timeout_s,
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
    assert rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    assert timeout_response in rpc_error.details()

    # Unblock the handlers to avoid graceful shutdown time.
    ray.get(signal_actor.send.remote(clear=True))


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
    serve.run(model, name=app_name)

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


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_grpc_proxy_cancellation(ray_instance, ray_shutdown, streaming: bool):
    """Test gRPC request client cancelled.

    When the request is canceled, gRPC proxy should cancel the underlying task.
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

    running_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    @serve.deployment
    class Downstream:
        async def wait_for_singal(self):
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await running_signal_actor.send.remote()

        async def __call__(self, *args):
            await self.wait_for_singal()
            return serve_pb2.UserDefinedResponse(greeting="hello")

        async def Streaming(self, *args):
            await self.wait_for_singal()
            yield serve_pb2.UserDefinedResponse(greeting="hello")

    downstream = Downstream.bind()
    serve.run(downstream, name="downstream", route_prefix="/downstream")

    # Send a request and wait for it to start executing.
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    metadata = (("application", "downstream"),)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
    if streaming:
        r = stub.Streaming(request=request, metadata=metadata)
    else:
        r = stub.__call__.future(request=request, metadata=metadata)
    await running_signal_actor.wait.remote()

    # Cancel it and verify that it is cancelled via signal.
    r.cancel()
    await cancelled_signal_actor.wait.remote()

    with pytest.raises(grpc.FutureCancelledError):
        r.result()

    ray.get(running_signal_actor.send.remote(clear=True))
    ray.get(cancelled_signal_actor.send.remote(clear=True))


@pytest.mark.parametrize("streaming", [False, True])
def test_using_grpc_context(ray_instance, ray_shutdown, streaming: bool):
    """Test using gRPC context.

    When the deployment sets code, details, and trailing metadata in the gRPC context,
    the response will reflect those values.
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
    error_code = grpc.StatusCode.DATA_LOSS
    error_message = "my specific error message"
    trailing_metadata = ("foo", "bar")

    @serve.deployment()
    class HelloModel:
        def __call__(
            self,
            user_message: serve_pb2.UserDefinedMessage,
            grpc_context: RayServegRPCContext,
        ):
            grpc_context.set_code(error_code)
            grpc_context.set_details(error_message)
            grpc_context.set_trailing_metadata([trailing_metadata])
            return serve_pb2.UserDefinedResponse(greeting="hello")

        def Streaming(
            self,
            user_message: serve_pb2.UserDefinedMessage,
            grpc_context: RayServegRPCContext,
        ):
            grpc_context.set_code(error_code)
            grpc_context.set_details(error_message)
            grpc_context.set_trailing_metadata([trailing_metadata])
            yield serve_pb2.UserDefinedResponse(greeting="hello")

    model = HelloModel.bind()
    app_name = "app1"
    serve.run(model, name=app_name)

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        if streaming:
            list(stub.Streaming(request=request))
        else:
            _ = stub.__call__(request=request)
    rpc_error = exception_info.value

    assert rpc_error.code() == error_code
    assert error_message == rpc_error.details()
    assert trailing_metadata in rpc_error.trailing_metadata()
    # request_id should always be set in the trailing metadata.
    assert any([key == "request_id" for key, _ in rpc_error.trailing_metadata()])


@pytest.mark.parametrize("streaming", [False, True])
def test_using_grpc_context_exception(ray_instance, ray_shutdown, streaming: bool):
    """Test setting code on gRPC context then raised exception.

    When the deployment in the gRPC context and then raised exception, the response
    code should still be internal error instead of user defined error.
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
    user_defined_error_code = grpc.StatusCode.DATA_LOSS
    real_error_message = "test error"

    @serve.deployment()
    class HelloModel:
        def __call__(
            self,
            user_message: serve_pb2.UserDefinedMessage,
            grpc_context: RayServegRPCContext,
        ):
            grpc_context.set_code(user_defined_error_code)
            raise RuntimeError(real_error_message)

        def Streaming(
            self,
            user_message: serve_pb2.UserDefinedMessage,
            grpc_context: RayServegRPCContext,
        ):
            grpc_context.set_code(user_defined_error_code)
            raise RuntimeError(real_error_message)

    model = HelloModel.bind()
    app_name = "app1"
    serve.run(model, name=app_name)

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        if streaming:
            list(stub.Streaming(request=request))
        else:
            _ = stub.__call__(request=request)
    rpc_error = exception_info.value

    assert rpc_error.code() == grpc.StatusCode.INTERNAL
    assert real_error_message in rpc_error.details()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("issue", ["incorrect_spelling", "more_args"])
def test_using_grpc_context_bad_function_signature(
    ray_instance, ray_shutdown, streaming: bool, issue: str
):
    """Test using gRPC context with bad function signature.

    When the deployment sets code, details, and trailing metadata in the gRPC context,
    the response will reflect those values.
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
    error_code = grpc.StatusCode.DATA_LOSS
    error_message = "my specific error message"
    trailing_metadata = ("foo", "bar")

    if issue == "incorrect_spelling":

        @serve.deployment()
        class HelloModel:
            def __call__(
                self,
                user_message: serve_pb2.UserDefinedMessage,
                grpc_context_incorrect_spelling: RayServegRPCContext,
            ):
                grpc_context_incorrect_spelling.set_code(error_code)
                grpc_context_incorrect_spelling.set_details(error_message)
                grpc_context_incorrect_spelling.set_trailing_metadata(
                    [trailing_metadata]
                )
                return serve_pb2.UserDefinedResponse(greeting="hello")

            def Streaming(
                self,
                user_message: serve_pb2.UserDefinedMessage,
                grpc_context_incorrect_spelling: RayServegRPCContext,
            ):
                grpc_context_incorrect_spelling.set_code(error_code)
                grpc_context_incorrect_spelling.set_details(error_message)
                grpc_context_incorrect_spelling.set_trailing_metadata(
                    [trailing_metadata]
                )
                yield serve_pb2.UserDefinedResponse(greeting="hello")

    elif issue == "more_args":

        @serve.deployment()
        class HelloModel:
            def __call__(
                self,
                user_message: serve_pb2.UserDefinedMessage,
                grpc_context: RayServegRPCContext,
                extra_required_arg: Any,
            ):
                grpc_context.set_code(error_code)
                grpc_context.set_details(error_message)
                grpc_context.set_trailing_metadata([trailing_metadata])
                return serve_pb2.UserDefinedResponse(greeting="hello")

            def Streaming(
                self,
                user_message: serve_pb2.UserDefinedMessage,
                grpc_context: RayServegRPCContext,
                extra_required_arg: Any,
            ):
                grpc_context.set_code(error_code)
                grpc_context.set_details(error_message)
                grpc_context.set_trailing_metadata([trailing_metadata])
                yield serve_pb2.UserDefinedResponse(greeting="hello")

    model = HelloModel.bind()
    app_name = "app1"
    serve.run(model, name=app_name)

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        if streaming:
            list(stub.Streaming(request=request))
        else:
            _ = stub.__call__(request=request)
    rpc_error = exception_info.value

    assert rpc_error.code() == grpc.StatusCode.INTERNAL
    assert "missing 1 required positional argument:" in rpc_error.details()
    if issue == "incorrect_spelling":
        assert "grpc_context_incorrect_spelling" in rpc_error.details()
    elif issue == "more_args":
        assert "extra_required_arg" in rpc_error.details()


def test_grpc_client_sending_large_payload(ray_instance, ray_shutdown):
    """Test gRPC client sending large payload.

    Serve's gRPC proxy should be configured to allow the client to send large payloads
    without error.
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
    serve.run(g)

    # This option allows the client to pass larger message.
    options = [
        ("grpc.max_receive_message_length", 1024 * 1024 * 1024),
    ]
    channel = grpc.insecure_channel("localhost:9000", options=options)
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # This is a large payload that exists gRPC's default message limit.
    large_payload = "foobar" * 1_000_000
    request = serve_pb2.UserDefinedMessage(name=large_payload, num=30, foo="bar")
    metadata = (("application", "default"),)
    response = stub.__call__(request=request, metadata=metadata)
    assert response == serve_pb2.UserDefinedResponse(
        greeting=f"Hello {large_payload} from bar",
        num_x2=60,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
