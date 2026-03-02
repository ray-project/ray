import asyncio
import sys
from typing import Any

import grpc

# coding: utf-8
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import (
    get_application_url,
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
from ray.serve.grpc_util import RayServegRPCContext, gRPCInputStream
from ray.serve.tests.test_config_files.grpc_deployment import g, g2


def test_serving_grpc_requests(ray_cluster):
    """Test serving gRPC requests.

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

    serve.run(g)

    url = get_application_url("gRPC", use_localhost=True)
    channel = grpc.insecure_channel(url)

    # Ensures ListApplications method succeeding.
    ping_grpc_list_applications(channel, ["default"])

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)


def test_grpc_routing_without_metadata(ray_cluster):
    """Test metadata are not required when calling gRPC proxy with only one app.

    When there is only one app deployed, gRPC proxy will route the request to the app
    with or without the metadata. If there are multiple app deployed, without metadata
    will return a notfound response.
    """
    # Routing doesn't happen in direct ingress mode.
    if RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    url = get_application_url("gRPC", app_name=app1, use_localhost=True)
    channel = grpc.insecure_channel(url)
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


def test_grpc_request_with_request_id(ray_cluster):
    """Test gRPC request with and without request id.

    When no request id is passed, gRPC proxy will respond with a random request id in
    the trailing metadata. When request id is passed, gRPC proxy will respond with the
    original request id.
    """
    # Custom request id is not yet supported for direct ingress
    if RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    url = get_application_url("gRPC", app_name=app1, use_localhost=True)
    channel = grpc.insecure_channel(url)
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


@pytest.mark.parametrize("streaming", [False, True])
def test_grpc_request_timeouts(ray_instance, ray_shutdown, streaming: bool):
    """Test gRPC request timed out.

    When the request timed out, gRPC proxy should return timeout response for both
    unary and streaming request.
    """
    # TODO(landscapepainter): This skipping mechanism needs to be removed when gRPC streaming for DI is implemented.
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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
        async def __call__(self, user_message):
            await signal_actor.wait.remote()
            return serve_pb2.UserDefinedResponse(greeting="hello")

        async def Streaming(self, user_message):
            for i in range(10):
                await signal_actor.wait.remote()
                yield serve_pb2.UserDefinedResponse(greeting="hello")

    serve.run(HelloModel.bind())

    url = get_application_url("gRPC", use_localhost=True)
    channel = grpc.insecure_channel(url)
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
def test_grpc_request_internal_error(ray_instance, ray_shutdown, streaming: bool):
    """Test gRPC request error out.

    When the request error out, gRPC proxy should return INTERNAL status and the error
    message in the response for both unary and streaming request.
    """
    # TODO(landscapepainter): This skipping mechanism needs to be removed when gRPC streaming for DI is implemented.
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    url = get_application_url("gRPC", app_name=app_name, use_localhost=True)
    channel = grpc.insecure_channel(url)
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
async def test_grpc_request_cancellation(ray_instance, ray_shutdown, streaming: bool):
    """Test gRPC request client cancelled.

    When the request is canceled, gRPC proxy should cancel the underlying task.
    """
    # TODO(landscapepainter): This skipping mechanism needs to be removed when gRPC streaming for DI is implemented.
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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
    url = get_application_url("gRPC", app_name="downstream", use_localhost=True)
    channel = grpc.insecure_channel(url)
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
    # TODO(landscapepainter): This skipping mechanism needs to be removed when gRPC streaming for DI is implemented.
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    url = get_application_url("gRPC", app_name=app_name, use_localhost=True)
    channel = grpc.insecure_channel(url)
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

    When the deployment sets a status code on the gRPC context and then raises an
    exception, the user-defined status code should be preserved in the response.
    """
    # TODO(landscapepainter): This skipping mechanism needs to be removed when gRPC streaming for DI is implemented.
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    url = get_application_url("gRPC", app_name=app_name, use_localhost=True)
    channel = grpc.insecure_channel(url)
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        if streaming:
            list(stub.Streaming(request=request))
        else:
            _ = stub.__call__(request=request)
    rpc_error = exception_info.value

    # User-defined status code should be preserved instead of INTERNAL
    assert rpc_error.code() == user_defined_error_code
    assert real_error_message in rpc_error.details()


@pytest.mark.parametrize("streaming", [False, True])
def test_exception_without_grpc_context_code(
    ray_instance, ray_shutdown, streaming: bool
):
    """Test raising exception without setting gRPC status code.

    When the deployment raises an exception without setting a status code on the
    gRPC context, the response should be INTERNAL error.
    """
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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
    real_error_message = "test error without status code"

    @serve.deployment()
    class HelloModel:
        def __call__(
            self,
            user_message: serve_pb2.UserDefinedMessage,
            grpc_context: RayServegRPCContext,
        ):
            # Don't set any status code, just raise exception
            raise RuntimeError(real_error_message)

        def Streaming(
            self,
            user_message: serve_pb2.UserDefinedMessage,
            grpc_context: RayServegRPCContext,
        ):
            # Don't set any status code, just raise exception
            raise RuntimeError(real_error_message)

    model = HelloModel.bind()
    app_name = "app1"
    serve.run(model, name=app_name)

    url = get_application_url("gRPC", app_name=app_name, use_localhost=True)
    channel = grpc.insecure_channel(url)
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        if streaming:
            list(stub.Streaming(request=request))
        else:
            _ = stub.__call__(request=request)
    rpc_error = exception_info.value

    # Without user-defined status code, should be INTERNAL
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
    # TODO(landscapepainter): This skipping mechanism needs to be removed when gRPC streaming for DI is implemented.
    if streaming and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    url = get_application_url("gRPC", app_name=app_name, use_localhost=True)
    channel = grpc.insecure_channel(url)
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
    url = get_application_url("gRPC", use_localhost=True)
    channel = grpc.insecure_channel(url, options=options)
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


def test_grpc_client_streaming(ray_instance, ray_shutdown):
    """Test gRPC client streaming (stream-unary) requests.

    When a client sends a stream of requests, the deployment should receive
    all messages and return a single response.
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

    @serve.deployment
    class ClientStreamingService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            """Receives stream of requests, returns sum of all num values."""
            total = 0
            count = 0
            async for request in request_stream:
                total += request.num
                count += 1
            return serve_pb2.UserDefinedResponse(
                greeting=f"Received {count} messages",
                num_x2=total * 2,
            )

    serve.run(ClientStreamingService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Create a generator of requests
    def request_generator():
        for i in range(5):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i + 1, foo="bar")

    # Call client streaming method
    response = stub.ClientStreaming(request_generator())
    assert response.greeting == "Received 5 messages"
    assert response.num_x2 == 30  # (1+2+3+4+5) * 2 = 30


def test_grpc_unary_not_found(ray_instance, ray_shutdown):
    """Test gRPC unary returns clean NOT_FOUND when app doesn't exist.

    When proxy_request yields only ResponseStatus (no body), returning None would
    cause serialization errors. This verifies unary_unary returns empty bytes
    and the client receives a clean gRPC error.
    """
    if RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    # Deploy two apps so routing requires metadata; single-app routes to default
    serve.run(g, name="app1", route_prefix="/app1")
    serve.run(g, name="app2", route_prefix="/app2")

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")

    # Call with non-existent app metadata - triggers not_found_response (only
    # ResponseStatus yielded, no body). Before fix, returning None caused
    # server-side serialization error.
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.__call__.with_call(
            request=request,
            metadata=(("application", "nonexistent_app"),),
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_grpc_client_streaming_not_found(ray_instance, ray_shutdown):
    """Test gRPC client streaming returns clean NOT_FOUND when app doesn't exist.

    When proxy_request yields only ResponseStatus (no body), returning None would
    cause serialization errors. This verifies stream_unary returns empty bytes
    and the client receives a clean gRPC error.
    """
    if RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip()

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

    @serve.deployment
    class ClientStreamingService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            async for _ in request_stream:
                pass
            return serve_pb2.UserDefinedResponse(greeting="ok", num_x2=0)

    # Deploy two apps so routing requires metadata; single-app routes to default
    serve.run(ClientStreamingService.bind(), name="app1", route_prefix="/app1")
    serve.run(ClientStreamingService.bind(), name="app2", route_prefix="/app2")

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        yield serve_pb2.UserDefinedMessage(name="x", num=1, foo="bar")

    # Call with non-existent app metadata - triggers not_found_response (only
    # ResponseStatus yielded, no body). Before fix, returning None caused
    # server-side serialization error.
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ClientStreaming(
            request_generator(),
            metadata=(("application", "nonexistent_app"),),
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_grpc_bidirectional_streaming(ray_instance, ray_shutdown):
    """Test gRPC bidirectional streaming (stream-stream) requests.

    When a client sends a stream of requests, the deployment should
    yield responses for each request.
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

    @serve.deployment
    class BidiStreamingService:
        async def BidiStreaming(self, request_stream: gRPCInputStream):
            """Receives stream of requests, yields response for each."""
            async for request in request_stream:
                yield serve_pb2.UserDefinedResponse(
                    greeting=f"Hello {request.name}",
                    num_x2=request.num * 2,
                )

    serve.run(BidiStreamingService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Create a generator of requests
    def request_generator():
        for i in range(3):
            yield serve_pb2.UserDefinedMessage(name=f"user_{i}", num=i * 10, foo="bar")

    # Call bidirectional streaming method
    responses = list(stub.BidiStreaming(request_generator()))
    assert len(responses) == 3
    assert responses[0].greeting == "Hello user_0"
    assert responses[0].num_x2 == 0
    assert responses[1].greeting == "Hello user_1"
    assert responses[1].num_x2 == 20
    assert responses[2].greeting == "Hello user_2"
    assert responses[2].num_x2 == 40


def test_grpc_client_streaming_with_grpc_context(ray_instance, ray_shutdown):
    """Test gRPC client streaming with gRPC context.

    The deployment should be able to access and modify the gRPC context
    during client streaming.
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

    custom_code = grpc.StatusCode.DATA_LOSS
    custom_message = "Custom error from client streaming"

    @serve.deployment
    class ClientStreamingWithContext:
        async def ClientStreaming(
            self,
            request_stream: gRPCInputStream,
            grpc_context: RayServegRPCContext,
        ):
            """Receives stream and sets custom status code."""
            count = 0
            async for request in request_stream:
                count += 1

            grpc_context.set_code(custom_code)
            grpc_context.set_details(custom_message)
            grpc_context.set_trailing_metadata([("custom-key", "custom-value")])
            return serve_pb2.UserDefinedResponse(
                greeting=f"Processed {count} messages",
                num_x2=count * 2,
            )

    serve.run(ClientStreamingWithContext.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        for i in range(3):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        stub.ClientStreaming(request_generator())

    rpc_error = exception_info.value
    assert rpc_error.code() == custom_code
    assert custom_message == rpc_error.details()
    assert ("custom-key", "custom-value") in rpc_error.trailing_metadata()


def test_grpc_bidirectional_streaming_with_grpc_context(ray_instance, ray_shutdown):
    """Test gRPC bidirectional streaming with gRPC context.

    The deployment should be able to access and modify the gRPC context
    during bidirectional streaming.
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

    custom_code = grpc.StatusCode.DATA_LOSS
    custom_message = "Custom error from bidi streaming"

    @serve.deployment
    class BidiStreamingWithContext:
        async def BidiStreaming(
            self,
            request_stream: gRPCInputStream,
            grpc_context: RayServegRPCContext,
        ):
            """Receives stream and yields responses with custom status."""
            grpc_context.set_code(custom_code)
            grpc_context.set_details(custom_message)
            grpc_context.set_trailing_metadata([("bidi-key", "bidi-value")])

            async for request in request_stream:
                yield serve_pb2.UserDefinedResponse(
                    greeting=f"Echo: {request.name}",
                    num_x2=request.num * 2,
                )

    serve.run(BidiStreamingWithContext.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        for i in range(2):
            yield serve_pb2.UserDefinedMessage(name=f"test_{i}", num=i * 5, foo="bar")

    with pytest.raises(grpc.RpcError) as exception_info:
        list(stub.BidiStreaming(request_generator()))

    rpc_error = exception_info.value
    assert rpc_error.code() == custom_code
    assert custom_message == rpc_error.details()
    assert ("bidi-key", "bidi-value") in rpc_error.trailing_metadata()


@pytest.mark.parametrize("streaming_type", ["client", "bidi"])
def test_grpc_streaming_internal_error(ray_instance, ray_shutdown, streaming_type: str):
    """Test gRPC streaming request with internal error.

    When the handler raises an exception, it should return INTERNAL status.
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
    error_message = "test streaming error"

    @serve.deployment
    class ErrorStreamingService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            async for _ in request_stream:
                pass
            raise RuntimeError(error_message)

        async def BidiStreaming(self, request_stream: gRPCInputStream):
            async for _ in request_stream:
                raise RuntimeError(error_message)

    serve.run(ErrorStreamingService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        for i in range(3):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")

    if streaming_type == "client":
        with pytest.raises(grpc.RpcError) as exception_info:
            stub.ClientStreaming(request_generator())
        rpc_error = exception_info.value
    else:
        with pytest.raises(grpc.RpcError) as exception_info:
            list(stub.BidiStreaming(request_generator()))
        rpc_error = exception_info.value

    assert rpc_error.code() == grpc.StatusCode.INTERNAL
    assert error_message in rpc_error.details()


@pytest.mark.parametrize("streaming_type", ["client", "bidi"])
def test_grpc_streaming_timeout(ray_instance, ray_shutdown, streaming_type: str):
    """Test gRPC streaming request timeout.

    When the request takes longer than the timeout, it should return
    DEADLINE_EXCEEDED status.
    """
    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
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
    class SlowStreamingService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            async for _ in request_stream:
                pass
            # Wait for signal that never comes (causes timeout)
            await signal_actor.wait.remote()
            return serve_pb2.UserDefinedResponse(greeting="done")

        async def BidiStreaming(self, request_stream: gRPCInputStream):
            async for request in request_stream:
                await signal_actor.wait.remote()
                yield serve_pb2.UserDefinedResponse(greeting=f"Hello {request.name}")

    serve.run(SlowStreamingService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        for i in range(2):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")

    timeout_response = f"timed out after {grpc_request_timeout_s}s."

    if streaming_type == "client":
        with pytest.raises(grpc.RpcError) as exception_info:
            stub.ClientStreaming(request_generator())
    else:
        with pytest.raises(grpc.RpcError) as exception_info:
            list(stub.BidiStreaming(request_generator()))

    rpc_error = exception_info.value
    assert rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    assert timeout_response in rpc_error.details()

    # Unblock the handlers to avoid graceful shutdown time.
    ray.get(signal_actor.send.remote(clear=True))


def test_grpc_client_streaming_empty_stream(ray_instance, ray_shutdown):
    """Test gRPC client streaming with empty stream.

    When the client sends no messages, the handler should still work.
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

    @serve.deployment
    class EmptyStreamService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            count = 0
            async for _ in request_stream:
                count += 1
            return serve_pb2.UserDefinedResponse(
                greeting=f"Received {count} messages",
                num_x2=count * 2,
            )

    serve.run(EmptyStreamService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Empty generator - sends no messages
    def empty_generator():
        return
        yield  # Never reached, but makes it a generator

    response = stub.ClientStreaming(empty_generator())
    assert response.greeting == "Received 0 messages"
    assert response.num_x2 == 0


def test_grpc_bidi_streaming_empty_stream(ray_instance, ray_shutdown):
    """Test gRPC bidirectional streaming with empty stream.

    When the client sends no messages, the handler should yield no responses.
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

    @serve.deployment
    class EmptyBidiService:
        async def BidiStreaming(self, request_stream: gRPCInputStream):
            async for request in request_stream:
                yield serve_pb2.UserDefinedResponse(
                    greeting=f"Hello {request.name}",
                    num_x2=request.num * 2,
                )

    serve.run(EmptyBidiService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Empty generator - sends no messages
    def empty_generator():
        return
        yield  # Never reached, but makes it a generator

    responses = list(stub.BidiStreaming(empty_generator()))
    assert len(responses) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming_type", ["client", "bidi"])
async def test_grpc_streaming_cancellation(
    ray_instance, ray_shutdown, streaming_type: str
):
    """Test gRPC streaming request client cancellation.

    When the client cancels the request, it should propagate to the handler.
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
    class CancellableStreamingService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            async with send_signal_on_cancellation(cancelled_signal_actor):
                async for _ in request_stream:
                    pass
                await running_signal_actor.send.remote()
                # Wait indefinitely - should be cancelled
                await asyncio.sleep(10000)
            return serve_pb2.UserDefinedResponse(greeting="done")

        async def BidiStreaming(self, request_stream: gRPCInputStream):
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await running_signal_actor.send.remote()
                async for request in request_stream:
                    yield serve_pb2.UserDefinedResponse(
                        greeting=f"Hello {request.name}"
                    )

    serve.run(CancellableStreamingService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        for i in range(2):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")

    if streaming_type == "client":
        r = stub.ClientStreaming.future(request_generator())
    else:
        r = stub.BidiStreaming(request_generator())

    await running_signal_actor.wait.remote()

    # Cancel it and verify that it is cancelled via signal.
    r.cancel()
    await cancelled_signal_actor.wait.remote()

    with pytest.raises(grpc.FutureCancelledError):
        r.result()

    ray.get(running_signal_actor.send.remote(clear=True))
    ray.get(cancelled_signal_actor.send.remote(clear=True))


@pytest.mark.parametrize("streaming_type", ["client", "bidi"])
def test_grpc_streaming_context_with_exception(
    ray_instance, ray_shutdown, streaming_type: str
):
    """Test setting gRPC context then raising exception in streaming.

    When the handler sets a custom gRPC status code on the context then raises
    an exception, the response should preserve the user-set status code.
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
    real_error_message = "real streaming error"

    @serve.deployment
    class ContextExceptionService:
        async def ClientStreaming(
            self,
            request_stream: gRPCInputStream,
            grpc_context: RayServegRPCContext,
        ):
            grpc_context.set_code(user_defined_error_code)
            async for _ in request_stream:
                pass
            raise RuntimeError(real_error_message)

        async def BidiStreaming(
            self,
            request_stream: gRPCInputStream,
            grpc_context: RayServegRPCContext,
        ):
            grpc_context.set_code(user_defined_error_code)
            async for _ in request_stream:
                raise RuntimeError(real_error_message)

    serve.run(ContextExceptionService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    def request_generator():
        for i in range(2):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")

    if streaming_type == "client":
        with pytest.raises(grpc.RpcError) as exception_info:
            stub.ClientStreaming(request_generator())
    else:
        with pytest.raises(grpc.RpcError) as exception_info:
            list(stub.BidiStreaming(request_generator()))

    rpc_error = exception_info.value
    assert rpc_error.code() == user_defined_error_code
    assert real_error_message in rpc_error.details()


@pytest.mark.parametrize("streaming_type", ["client", "bidi"])
def test_grpc_streaming_backpressure(ray_instance, ray_shutdown, streaming_type: str):
    """Test gRPC streaming with slow consumer (backpressure).

    When the server processes messages slower than the client sends them,
    all messages should still be received correctly without loss.
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

    num_messages = 20
    process_delay_s = 0.05  # 50ms delay per message

    @serve.deployment
    class SlowConsumerService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            """Slow consumer - processes each message with a delay."""
            received_messages = []
            async for request in request_stream:
                await asyncio.sleep(process_delay_s)
                received_messages.append(request.name)
            return serve_pb2.UserDefinedResponse(
                greeting=f"Received {len(received_messages)} messages",
                num_x2=len(received_messages),
            )

        async def BidiStreaming(self, request_stream: gRPCInputStream):
            """Slow consumer - processes each message with a delay."""
            count = 0
            async for request in request_stream:
                await asyncio.sleep(process_delay_s)
                count += 1
                yield serve_pb2.UserDefinedResponse(
                    greeting=f"Processed: {request.name}",
                    num_x2=count,
                )

    serve.run(SlowConsumerService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Fast producer - sends messages as quickly as possible
    def fast_request_generator():
        for i in range(num_messages):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")

    if streaming_type == "client":
        response = stub.ClientStreaming(fast_request_generator())
        assert response.greeting == f"Received {num_messages} messages"
        assert response.num_x2 == num_messages
    else:
        responses = list(stub.BidiStreaming(fast_request_generator()))
        assert len(responses) == num_messages
        # Verify all messages were processed in order
        for i, response in enumerate(responses):
            assert response.greeting == f"Processed: msg_{i}"
            assert response.num_x2 == i + 1


@pytest.mark.parametrize("streaming_type", ["client", "bidi"])
def test_grpc_streaming_client_error_mid_stream(
    ray_instance, ray_shutdown, streaming_type: str
):
    """Test gRPC streaming when client raises error mid-stream.

    When the client generator raises an exception while streaming,
    the error should propagate to the client as a gRPC error.
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

    messages_before_error = 2

    @serve.deployment
    class ErrorMidStreamService:
        async def ClientStreaming(self, request_stream: gRPCInputStream):
            """Receives messages until client errors."""
            received = []
            async for request in request_stream:
                received.append(request.name)
            return serve_pb2.UserDefinedResponse(
                greeting=f"Received {len(received)} messages",
                num_x2=len(received),
            )

        async def BidiStreaming(self, request_stream: gRPCInputStream):
            """Receives and responds to messages until client errors."""
            count = 0
            async for request in request_stream:
                count += 1
                yield serve_pb2.UserDefinedResponse(
                    greeting=f"Echo: {request.name}",
                    num_x2=count,
                )

    serve.run(ErrorMidStreamService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Generator that sends some messages then raises an error
    def error_request_generator():
        for i in range(messages_before_error):
            yield serve_pb2.UserDefinedMessage(name=f"msg_{i}", num=i, foo="bar")
        # Simulate client-side error mid-stream
        raise RuntimeError("Client error during streaming")

    # Client error should result in a gRPC error (or RuntimeError propagating)
    if streaming_type == "client":
        with pytest.raises((grpc.RpcError,)):
            stub.ClientStreaming(error_request_generator())
    else:
        with pytest.raises((grpc.RpcError,)):
            # Consume responses until error propagates
            list(stub.BidiStreaming(error_request_generator()))


def test_grpc_streaming_client_closes_channel_mid_stream(ray_instance, ray_shutdown):
    """Test gRPC streaming when client closes channel mid-stream.

    When the client closes the gRPC channel while streaming, the server
    should detect this and stop processing.
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

    client_ready_signal = SignalActor.remote()
    server_received_signal = SignalActor.remote()
    server_cancelled_signal = SignalActor.remote()

    @serve.deployment
    class ChannelCloseService:
        async def BidiStreaming(self, request_stream: gRPCInputStream):
            """Receives messages and signals when first one arrives."""
            async with send_signal_on_cancellation(server_cancelled_signal):
                count = 0
                async for request in request_stream:
                    count += 1
                    await server_received_signal.send.remote()
                    yield serve_pb2.UserDefinedResponse(
                        greeting=f"Got: {request.name}",
                        num_x2=count,
                    )
                    # Wait for more messages (will be cancelled)
                    await asyncio.sleep(10000)

    serve.run(ChannelCloseService.bind())

    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Generator that waits for signal before sending more messages
    def slow_request_generator():
        yield serve_pb2.UserDefinedMessage(name="first", num=1, foo="bar")
        # Wait for server to receive the message
        ray.get(client_ready_signal.wait.remote())
        # This message may or may not be sent depending on channel close timing
        yield serve_pb2.UserDefinedMessage(name="second", num=2, foo="bar")

    # Start the streaming call
    response_iterator = stub.BidiStreaming(slow_request_generator())

    # Get first response
    first_response = next(response_iterator)
    assert first_response.greeting == "Got: first"

    # Wait for server to process the first message
    ray.get(server_received_signal.wait.remote(), timeout=5)

    # Close the channel abruptly
    channel.close()

    # Signal the generator to continue (it will fail to send)
    ray.get(client_ready_signal.send.remote())

    # The iterator should raise an error since channel is closed
    with pytest.raises(grpc.RpcError):
        list(response_iterator)

    # Verify the server received the cancellation
    ray.get(server_cancelled_signal.wait.remote(), timeout=5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
