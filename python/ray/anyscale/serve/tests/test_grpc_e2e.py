import os
import sys
from pathlib import Path

import pytest

from ray import serve
from ray.serve.schema import LoggingConfig
from ray.serve.tests.conftest import *  # noqa
from ray.serve.tests.conftest import _shared_serve_instance  # noqa


@serve.deployment
class Downstream:
    def __call__(self):
        return "hi"


@serve.deployment
class Ingress:
    def __init__(self, handle, **handle_options):
        self._handle = handle
        self._handle._init(**handle_options)

    async def __call__(self):
        return await self._handle.remote()


@pytest.mark.parametrize(
    "ray_instance",
    [{"RAY_SERVE_LOG_TO_STDERR": "0"}],
    indirect=True,
)
def test_no_spammy_errors_in_composed_app(ray_instance, tmp_dir):
    """Direct all stdout/stderr to logs, and check that the false errors
    from gRPC are not there."""

    logs_dir = Path(tmp_dir)
    logging_config = LoggingConfig(encoding="JSON", logs_dir=str(logs_dir))

    h = serve.run(
        Ingress.options(logging_config=logging_config).bind(
            Downstream.options(logging_config=logging_config).bind(),
            # Ensure handle that Ingress holds to Downstream is using gRPC
            _by_reference=False,
        )
    )
    h._init(_by_reference=False)

    for _ in range(10):
        assert h.remote().result() == "hi"

    for log_file in os.listdir(logs_dir):
        if not log_file.startswith("replica_default"):
            continue

        with open(logs_dir / log_file) as f:
            logs = f.read()

        assert "Exception in callback PollerCompletionQueue._handle_events" not in logs
        assert "BlockingIOError" not in logs
        assert "Resource temporarily unavailable" not in logs

    serve.shutdown()


def test_same_loop_handle(serve_instance):
    # With a local handle, where there is no running asyncio loop,
    # setting _run_router_in_separate_loop=False should error.
    h = serve.run(Downstream.bind())
    with pytest.raises(RuntimeError, match="No event loop running"):
        h._init(_by_reference=False, _run_router_in_separate_loop=False)

    # However setting _run_router_in_separate_loop=False in a replica
    # should work since there is a running asyncio event loop.
    h = serve.run(
        Ingress.bind(
            Downstream.bind(),
            _by_reference=False,
            _run_router_in_separate_loop=False,
        )
    )
    h._init(_by_reference=False)
    assert h.remote().result() == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
