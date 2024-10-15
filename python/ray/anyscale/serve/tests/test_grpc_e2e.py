import os
import sys
from pathlib import Path

import pytest

from ray import serve
from ray.serve.schema import LoggingConfig
from ray.serve.tests.conftest import *  # noqa


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

    @serve.deployment(logging_config=logging_config)
    class Downstream:
        def __call__(self):
            return "hi"

    @serve.deployment(logging_config=logging_config)
    class Ingress:
        def __init__(self, handle):
            self._handle = handle

        async def __call__(self):
            print("supppp")
            return await self._handle.remote()

    h = serve.run(Ingress.bind(Downstream.bind()))
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
