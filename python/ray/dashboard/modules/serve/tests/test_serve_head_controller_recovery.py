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


def _controller_actor_id() -> str:
    return ray.get_actor(
        SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
    )._actor_id.hex()


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_rest_api_recovers_from_stale_cached_controller(ray_start_stop):  # noqa: F811
    """The dashboard caches the controller handle; a replaced controller must
    not leave the Serve REST API permanently broken.

    `ServeHead.get_serve_controller` validates the cached handle with
    `check_alive.remote()`, drops it when that fails, and re-resolves the
    controller by name.

    The REST API is deliberately NOT called between the shutdown and the
    restart: any call in that window would already trip the failure path and
    leave `self._controller` as None, so the call after the restart would be a
    cold `get_actor` rather than a re-resolution from a stale handle.
    """
    serve.run(app, name="app1")
    wait_for_condition(
        lambda: _get_serve_details().applications["app1"].status == "RUNNING",
        timeout=30,
    )
    # The dashboard has now cached a handle to this controller.
    original_controller = _controller_actor_id()

    # Replace the controller without touching the REST API, so the dashboard
    # still holds the handle to the old one.
    serve.shutdown()
    serve.run(app, name="app2")
    new_controller = _controller_actor_id()
    assert new_controller != original_controller, (
        "expected a different controller actor; the test would not be "
        "exercising re-resolution otherwise"
    )

    # First REST call since the swap: the cached handle is stale and must be
    # dropped and re-resolved for this to return 200.
    wait_for_condition(
        lambda: _get_serve_details().applications["app2"].status == "RUNNING",
        timeout=30,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", "-s", __file__]))
