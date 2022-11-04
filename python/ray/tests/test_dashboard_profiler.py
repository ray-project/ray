import pytest
import subprocess
import os
import requests

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_until_succeeded_without_exception,
)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_profiler_endpoints(ray_start_with_dashboard):
    # Sanity check py-spy is installed.
    subprocess.check_call(["py-spy", "help"])

    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    address_info = ray_start_with_dashboard
    webui_url = address_info["webui_url"]
    webui_url = format_web_url(webui_url)

    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

        def do_stuff_infinite(self):
            while True:
                pass

    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    a.do_stuff_infinite.remote()

    def get_actor_stack():
        response = requests.get(f"{webui_url}/worker/traceback?pid={pid}")
        response.raise_for_status()
        content = response.content.decode("utf-8")
        print(content)
        # Sanity check we got the stack trace text.
        assert "do_stuff_infinite" in content, content
        assert "ray::core::CoreWorker" in content, content

    assert wait_until_succeeded_without_exception(
        get_actor_stack, (requests.RequestException, AssertionError),
        timeout_ms=20000, retry_interval_ms=1000,
    )


    def get_actor_flamegraph():
        response = requests.get(f"{webui_url}/worker/cpu_profile?pid={pid}")
        response.raise_for_status()
        assert response.headers["Content-Type"] == "image/svg+xml", response.headers
        content = response.content.decode("utf-8")
        print(content)
        # Sanity check we got the flame graph SVG.
        assert "<!DOCTYPE svg" in content, content
        assert "do_stuff_infinite" in content, content
        assert "ray::core" in content, content

    assert wait_until_succeeded_without_exception(
        get_actor_flamegraph, (requests.RequestException, AssertionError),
        timeout_ms=20000, retry_interval_ms=1000,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
