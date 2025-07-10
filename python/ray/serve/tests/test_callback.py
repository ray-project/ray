import importlib
import logging
import os
import sys

import httpx
import pytest
import starlette
from starlette.middleware import Middleware

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.exceptions import RayActorError
from ray.serve._private.utils import call_function_from_import_path
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.context import _get_global_client
from ray.serve.schema import LoggingConfig, ProxyStatus, ServeInstanceDetails


# ==== Callbacks used in this test ====
class ASGIMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        scope.get("headers").append((b"custom_header_key", "custom_header_value"))
        await self.app(scope, receive, send)


def add_middleware():
    return [Middleware(ASGIMiddleware)]


class MyServeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        log_msg = super().format(record)
        return "MyCustom message: hello " + log_msg


def add_logger():
    ray_logger = logging.getLogger("ray.serve")
    handler = logging.StreamHandler()
    handler.setFormatter(MyServeFormatter())
    ray_logger.addHandler(handler)


def raise_error_callback():
    raise RuntimeError("this is from raise_error_callback")


def return_bad_objects():
    return [1, 2, 3]


ADD_MIDDLEWARE_IMPORT_PATH = "ray.serve.tests.test_callback.add_middleware"
ADD_LOGGER_IMPORT_PATH = "ray.serve.tests.test_callback.add_logger"
RAISE_ERROR_IMPORT_PATH = "ray.serve.tests.test_callback.raise_error_callback"
RETURN_BAD_OBJECTS_IMPORT_PATH = "ray.serve.tests.test_callback.return_bad_objects"
NOT_CALLABLE_OBJECT = 1
# ==== end ====


@pytest.fixture()
def ray_instance(request):
    """Starts and stops a Ray instance for this test.

    Args:
        request: request.param should contain a dictionary of env vars and
            their values. The Ray instance will be started with these env vars.
    """

    original_env_vars = os.environ.copy()

    try:
        requested_env_vars = request.param
    except AttributeError:
        requested_env_vars = {}

    os.environ.update(requested_env_vars)
    importlib.reload(ray.serve._private.constants)
    importlib.reload(ray.serve._private.controller)
    importlib.reload(ray.serve._private.proxy)

    yield ray.init()

    serve.shutdown()
    ray.shutdown()

    os.environ.clear()
    os.environ.update(original_env_vars)


def test_call_function_from_import_path():
    """Basic test for call_function_from_import_path"""

    # basic
    assert [1, 2, 3] == call_function_from_import_path(RETURN_BAD_OBJECTS_IMPORT_PATH)

    # rasie exception when callback function raise exception
    with pytest.raises(RuntimeError, match="this is from raise_error_callback"):
        call_function_from_import_path(RAISE_ERROR_IMPORT_PATH)

    # raise exception when providing invalid import path
    with pytest.raises(ValueError, match="cannot be imported"):
        call_function_from_import_path("not_exist")

    # raise exception when providing non callable object
    with pytest.raises(TypeError, match="is not callable"):
        call_function_from_import_path(
            "ray.serve.tests.test_callback.NOT_CALLABLE_OBJECT"
        )


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH": ADD_LOGGER_IMPORT_PATH,
            "RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH": ADD_MIDDLEWARE_IMPORT_PATH,
        },
    ],
    indirect=True,
)
def test_callback(ray_instance, capsys):
    """Test callback function works in http proxy and controller"""
    serve.start(
        http_options=HTTPOptions(
            host="0.0.0.0",
            request_timeout_s=500,
        ),
    )

    @serve.deployment
    class Model:
        def __call__(self, request: starlette.requests.Request):
            headers = request.scope.get("headers")
            for k, v in headers:
                if k == b"custom_header_key":
                    return v
            return "Not found custom headers"

    serve.run(Model.bind())
    resp = httpx.get("http://localhost:8000")

    assert resp.text == "custom_header_value"

    captured = capsys.readouterr()
    assert "MyCustom message: hello" in captured.err


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH": "not_exist",
            "RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH": RAISE_ERROR_IMPORT_PATH,
        },
    ],
    indirect=True,
)
def test_callback_fail(ray_instance):
    """Test actor call call_function_from_import_path rasing exception.

    Actor will fail to be started and further call will raise RayActorError.
    """

    actor_def = ray.serve._private.proxy.ProxyActor
    handle = actor_def.remote(
        http_options=HTTPOptions(host="http_proxy", root_path="/", port=123),
        grpc_options=gRPCOptions(),
        node_ip_address="127.0.0.1",
        node_id="123",
        logging_config=LoggingConfig(),
        long_poll_client="fake_client",
    )
    with pytest.raises(RayActorError, match="this is from raise_error_callback"):
        ray.get(handle.ready.remote())

    serve_controller = ray.serve._private.controller.ServeController
    actor_def = ray.actor._make_actor(serve_controller, {})
    handle = actor_def.remote(
        http_options=HTTPOptions(),
        grpc_options=gRPCOptions(),
        global_logging_config=LoggingConfig(),
    )
    with pytest.raises(RayActorError, match="cannot be imported"):
        ray.get(handle.check_alive.remote())


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH": RETURN_BAD_OBJECTS_IMPORT_PATH,
        },
    ],
    indirect=True,
)
def test_http_proxy_return_aribitary_objects(ray_instance):
    """Test invalid callback path in http proxy"""

    actor_def = ray.serve._private.proxy.ProxyActor
    handle = actor_def.remote(
        http_options=HTTPOptions(host="http_proxy", root_path="/", port=123),
        grpc_options=gRPCOptions(),
        node_ip_address="127.0.0.1",
        node_id="123",
        logging_config=LoggingConfig(),
        long_poll_client="fake_client",
    )
    with pytest.raises(
        RayActorError, match="must return a list of Starlette middlewares"
    ):
        ray.get(handle.ready.remote())


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH": RAISE_ERROR_IMPORT_PATH,
        },
    ],
    indirect=True,
)
def test_http_proxy_callback_failures(ray_instance, capsys):
    """Test http proxy keeps restarting when callback function fails"""

    try:
        serve.start()
    except RayActorError:
        # serve.start will fail because the http proxy is not started successfully
        # and client use proxy handle to check the proxy readiness, so it will raise
        # RayActorError.
        pass

    client = _get_global_client()

    def check_http_proxy_keep_restarting():
        # The proxy will be under "STARTING" status and keep restarting.
        prev_actor_id = None
        while True:
            serve_details = ServeInstanceDetails(**client.get_serve_details())
            for _, proxy_info in serve_details.proxies.items():
                if proxy_info.status != ProxyStatus.STARTING:
                    return False
                if prev_actor_id is None:
                    prev_actor_id = proxy_info.actor_id
                    break
                elif prev_actor_id != proxy_info.actor_id:
                    return True

    wait_for_condition(check_http_proxy_keep_restarting)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
