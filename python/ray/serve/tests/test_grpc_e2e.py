import os
import sys
from pathlib import Path

import pytest

from ray import serve
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus, LoggingConfig
from ray.serve.tests.conftest import *  # noqa
from ray.serve.tests.conftest import _shared_serve_instance  # noqa


@serve.deployment
class Downstream:
    def __call__(self):
        return "hi"


downstream_node = Downstream.bind()


@serve.deployment
class Ingress:
    def __init__(self, handle, **handle_options):
        self._handle = handle
        self._handle._init(**handle_options)

    async def __call__(self):
        return await self._handle.options(_by_reference=False).remote()


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
