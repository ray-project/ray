import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

import grpc
import pytest
import requests

from ray import ActorID, serve
from ray._common.test_utils import wait_for_condition
from ray.exceptions import ActorUnavailableError
from ray.serve._private.common import RequestMetadata
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.replica_result import gRPCReplicaResult
from ray.serve.schema import ApplicationStatus, LoggingConfig
from ray.serve.tests.conftest import *  # noqa
from ray.serve.tests.conftest import _shared_serve_instance  # noqa


@serve.deployment
class Downstream:
    def __call__(self):
        return "hi"


downstream_node = Downstream.bind()


@pytest.mark.asyncio
async def test_grpc_done_callback_translates_real_unavailable_call():
    async def unavailable(_request, context):
        await context.abort(grpc.StatusCode.UNAVAILABLE, "replica unavailable")

    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "ray.serve.tests.UnavailableService",
                {
                    "Call": grpc.unary_unary_rpc_method_handler(
                        unavailable,
                        request_deserializer=lambda value: value,
                        response_serializer=lambda value: value,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")

    try:
        call = channel.unary_unary(
            "/ray.serve.tests.UnavailableService/Call",
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )(b"request")
        result = gRPCReplicaResult(
            call,
            metadata=RequestMetadata(
                request_id="request-id",
                internal_request_id="internal-request-id",
                is_streaming=False,
                _on_separate_loop=False,
            ),
            actor_id=ActorID(b"2" * 16),
            loop=asyncio.get_running_loop(),
        )
        callback_result = asyncio.get_running_loop().create_future()
        result.add_done_callback(callback_result.set_result)

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await call
        assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE

        translated = await asyncio.wait_for(callback_result, timeout=5)
        assert isinstance(translated, ActorUnavailableError)
    finally:
        await channel.close()
        await server.stop(grace=None)


@serve.deployment
class Ingress:
    def __init__(self, handle, **handle_options):
        self._handle = handle
        self._handle._init(**handle_options)

    async def __call__(self):
        return await self._handle.options(_by_reference=False).remote()


@pytest.mark.skipif(
    sys.platform == "win32", reason="Temp directory cleanup fails on Windows"
)
@pytest.mark.parametrize(
    "ray_instance",
    [{"RAY_SERVE_LOG_TO_STDERR": "1"}],
    indirect=True,
)
def test_no_spammy_errors_in_composed_app(ray_instance, tmp_dir):
    """Direct all stdout/stderr to logs, and check that the false errors
    from gRPC are not there in replica logs."""

    logs_dir = Path(tmp_dir)
    logging_config = LoggingConfig(encoding="JSON", logs_dir=str(logs_dir))

    h = serve.run(
        Ingress.options(logging_config=logging_config).bind(
            Downstream.options(logging_config=logging_config).bind(),
        )
    )

    for _ in range(10):
        assert h.options(_by_reference=False).remote().result() == "hi"

    for log_file in os.listdir(logs_dir):
        if not log_file.startswith("replica_default"):
            continue

        with open(logs_dir / log_file) as f:
            logs = f.read()

        assert "Exception in callback" not in logs
        assert "PollerCompletionQueue._handle_events" not in logs
        assert "BlockingIOError" not in logs
        assert "Resource temporarily unavailable" not in logs


def check_running():
    assert (
        serve.status().applications[SERVE_DEFAULT_APP_NAME].status
        == ApplicationStatus.RUNNING
    )
    return True


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows signal handling not compatible"
)
@pytest.mark.parametrize(
    "ray_instance",
    [{"RAY_SERVE_PROXY_USE_GRPC": "1"}],
    indirect=True,
)
def test_no_spammy_errors_in_grpc_proxy(ray_instance, tmp_dir):
    """Direct all stdout/stderr to logs, and check that the false errors
    from gRPC are not there in proxy logs."""

    serve.start(
        http_options={"host": "0.0.0.0"},
        grpc_options={
            "port": 9000,
            "grpc_servicer_functions": [
                "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",  # noqa
            ],
        },
    )

    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--address=auto",
            "ray.serve.tests.test_grpc_e2e.downstream_node",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    # Since we start Serve in a subprocess which is non-blocking, we won't know
    # when `serve run` completes (i.e. done waiting on proxies to be serving), so
    # we query the application until we get a valid response.
    wait_for_condition(
        lambda: requests.post("http://localhost:8000").status_code == 200,
    )
    for _ in range(10):
        assert requests.post("http://localhost:8000").text == "hi"

    p.send_signal(signal.SIGINT)
    p.wait()
    process_output, _ = p.communicate()
    logs = process_output.decode("utf-8").strip()
    assert "Exception in callback" not in logs
    assert "PollerCompletionQueue._handle_events" not in logs
    assert "BlockingIOError" not in logs
    assert "Resource temporarily unavailable" not in logs


def test_same_loop_handle(serve_instance):
    # With a local handle, where there is no running asyncio loop,
    # setting _run_router_in_separate_loop=False should error.
    h = serve.run(Downstream.bind())
    with pytest.raises(RuntimeError, match="No event loop running"):
        h._init(_run_router_in_separate_loop=False)

    # However setting _run_router_in_separate_loop=False in a replica
    # should work since there is a running asyncio event loop.
    h = serve.run(
        Ingress.bind(
            Downstream.bind(),
            _run_router_in_separate_loop=False,
        )
    )
    assert h.options(_by_reference=False).remote().result() == "hi"


def test_custom_serialization_method(serve_instance):
    @serve.deployment
    class Downstream:
        def __call__(self, message: str):
            return f"Hello {message}!"

    h = serve.run(Downstream.bind())
    assert (
        h.options(
            _by_reference=False,
            request_serialization="pickle",
            response_serialization="pickle",
        )
        .remote("world1")
        .result()
        == "Hello world1!"
    )

    assert (
        h.options(
            _by_reference=False,
            request_serialization="pickle",
            response_serialization="cloudpickle",
        )
        .remote("world2")
        .result()
        == "Hello world2!"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
