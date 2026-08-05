import os
import sys

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve.schema import ServeInstanceDetails
from ray.serve.tests.conftest import *  # noqa: F401 F403
from ray.tests.conftest import *  # noqa: F401 F403

# For local testing on a Macbook, set `export TEST_ON_DARWIN=1`.
TEST_ON_DARWIN = os.environ.get("TEST_ON_DARWIN", "0") == "1"

SERVE_HEAD_URL = "http://localhost:8265/api/serve/applications/"


@serve.deployment
class Hello:
    def __call__(self) -> str:
        return "hello"


app = Hello.bind()


def _get_serve_details() -> ServeInstanceDetails:
    r = httpx.get(SERVE_HEAD_URL, timeout=15)
    assert r.status_code == 200, f"{r.status_code}: {r.text}"
    return ServeInstanceDetails(**r.json())


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_rest_api_recovers_after_controller_is_replaced(ray_start_stop):  # noqa: F811
    """The dashboard caches the controller handle; killing the controller must
    not leave the Serve REST API permanently broken.

    `ServeHead.get_serve_controller` validates the cached handle with
    `check_alive.remote()` and, when that fails, drops the handle and
    re-resolves the controller by name. This exercises that path end to end:
    the REST API has to keep serving 200s across a controller death and a
    fresh `serve.run`, without a dashboard restart.
    """
    serve.run(app, name="app1")
    wait_for_condition(
        lambda: _get_serve_details().applications["app1"].status == "RUNNING",
        timeout=30,
    )
    # The dashboard has now cached a handle to this controller.
    original_controller_pid = ray.get_actor(
        SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
    )._actor_id.hex()

    # Tear Serve down entirely: the cached handle is now stale and the
    # controller no longer exists to re-resolve.
    serve.shutdown()
    wait_for_condition(
        lambda: _get_serve_details().applications == {},
        timeout=30,
    )

    # Bring Serve back up. The dashboard must notice its cached handle is
    # unusable and re-resolve the *new* controller rather than keep failing.
    serve.run(app, name="app2")
    wait_for_condition(
        lambda: _get_serve_details().applications["app2"].status == "RUNNING",
        timeout=30,
    )
    new_controller_pid = ray.get_actor(
        SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
    )._actor_id.hex()
    assert new_controller_pid != original_controller_pid, (
        "expected a different controller actor; the test would not be "
        "exercising re-resolution otherwise"
    )


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_rest_api_recovers_after_controller_is_killed(ray_start_stop):  # noqa: F811
    """Same path, but the controller is killed out from under Serve rather
    than shut down cleanly, so the cached handle fails while Serve still
    considers itself running.
    """
    serve.run(app, name="app1")
    wait_for_condition(
        lambda: _get_serve_details().applications["app1"].status == "RUNNING",
        timeout=30,
    )

    controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
    ray.kill(controller, no_restart=False)

    # The controller restarts; the dashboard's cached handle has to survive or
    # be re-resolved. Either way the REST API must go back to 200 on its own.
    wait_for_condition(
        lambda: _get_serve_details().applications["app1"].status == "RUNNING",
        timeout=60,
        retry_interval_ms=1000,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", "-s", __file__]))
