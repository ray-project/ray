import pytest
import subprocess
import os
import requests
import sys
import time

import numpy as np
import ray
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_until_succeeded_without_exception,
    wait_for_condition,
)
from ray.dashboard.modules.reporter.profile_manager import MemoryProfilingManager


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX: https://github.com/ray-project/ray/issues/30114",
)
@pytest.mark.parametrize("native", ["0", "1"])
def test_profiler_endpoints(ray_start_with_dashboard, native):
    # Sanity check py-spy is installed.
    subprocess.check_call(["py-spy", "--version"])

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
        url = f"{webui_url}/worker/traceback?pid={pid}&native={native}"
        print("GET URL", url)
        response = requests.get(url)
        print("STATUS CODE", response.status_code)
        print("HEADERS", response.headers)
        content = response.content.decode("utf-8")
        print("CONTENT", content)
        response.raise_for_status()
        assert "text/plain" in response.headers["Content-Type"], response.headers
        # Sanity check we got the stack trace text.
        assert "do_stuff_infinite" in content, content
        if native == "1":
            assert "ray::core::CoreWorker" in content, content
        else:
            assert "ray::core::CoreWorker" not in content, content

    assert wait_until_succeeded_without_exception(
        get_actor_stack,
        (requests.RequestException, AssertionError),
        timeout_ms=20000,
        retry_interval_ms=1000,
    )

    def get_actor_flamegraph():
        response = requests.get(
            f"{webui_url}/worker/cpu_profile?pid={pid}&native={native}"
        )
        response.raise_for_status()
        assert response.headers["Content-Type"] == "image/svg+xml", response.headers
        content = response.content.decode("utf-8")
        print(content)
        # Sanity check we got the flame graph SVG.
        assert "<!DOCTYPE svg" in content, content
        assert "do_stuff_infinite" in content, content
        if native == "1":
            assert "ray::core" in content, content
        else:
            assert "ray::core" not in content, content

    assert wait_until_succeeded_without_exception(
        get_actor_flamegraph,
        (requests.RequestException, AssertionError),
        timeout_ms=20000,
        retry_interval_ms=1000,
    )


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX: https://github.com/ray-project/ray/issues/30114",
)
def test_profiler_failure_message(ray_start_with_dashboard):
    # Sanity check py-spy is installed.
    subprocess.check_call(["py-spy", "--version"])

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
        print("CONTENT", content)
        assert "do_stuff_infinite" in content, content

    # First ensure dashboard is up, before we test for failure cases.
    assert wait_until_succeeded_without_exception(
        get_actor_stack,
        (requests.RequestException, AssertionError),
        timeout_ms=20000,
        retry_interval_ms=1000,
    )

    # Check we return the right status code and error message on failure.
    response = requests.get(f"{webui_url}/worker/traceback?pid=1234567")
    content = response.content.decode("utf-8")
    print(content)
    assert "text/plain" in response.headers["Content-Type"], response.headers
    assert "Failed to execute" in content, content

    # Check we return the right status code and error message on failure.
    response = requests.get(f"{webui_url}/worker/cpu_profile?pid=1234567")
    content = response.content.decode("utf-8")
    print(content)
    assert "text/plain" in response.headers["Content-Type"], response.headers
    assert "Failed to execute" in content, content


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
async def test_memory_profiler(shutdown_only, tmp_path):
    # Sanity check py-spy is installed.
    subprocess.check_call(["memray", "--help"])
    ray.init()

    p = MemoryProfilingManager(tmp_path)

    @ray.remote
    class Actor:
        def __init__(self):
            self.a = []

        def getpid(self):
            return os.getpid()

        def do_stuff_infinite(self):
            while True:
                time.sleep(1)
                self.a.append(np.random.rand(1024))  # 1KB/s

    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    a.do_stuff_infinite.remote()

    print("Failure test.")
    # Random pid should fail.
    result, output = await p.attach(pid=123123123)
    assert result is False
    print(output)

    print("Successful test.")
    # Should be able to attach to the running proc
    result, output = await p.attach(pid=pid)
    assert result is True
    print(output)

    print("Reattachment test.")
    # Reattachment should work (no-op).
    result, output = await p.attach(pid=pid)
    assert result is True
    print(output)

    print("Profiling test, flamegraph format.")
    result, output = await p.memory_profile(pid=pid, format="flamegraph")
    assert result is True

    print("Profiling test, table format.")
    result, output = await p.memory_profile(pid=pid, format="table")
    assert result is True

    # Create a new actor.
    a = Actor.remote()
    pid = ray.get(a.getpid.remote())

    print("Profiling without attach")
    # profilng without attach should still work.
    result, output = await p.memory_profile(pid=pid)
    assert result is True


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_memory_profiler_e2e(ray_start_with_dashboard):
    # Sanity check py-spy is installed.
    subprocess.check_call(["memray", "--help"])

    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    address_info = ray_start_with_dashboard
    webui_url = address_info["webui_url"]
    webui_url = format_web_url(webui_url)
    ip = address_info.address_info["node_ip_address"]
    # Ensure dashboard is up using state API.
    wait_for_condition(lambda: len(ray.experimental.state.api.list_nodes()) == 1)

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

    # If the ip is not provided, the request should fail.
    response = requests.get(f"{webui_url}/worker/memory_profile?pid={pid}")
    with pytest.raises(requests.exceptions.HTTPError):
        assert "ip address and pid has to be provided" in response.text
        response.raise_for_status()

    # Run profiler
    # SANG-TODO native, format, duration
    def request_mem_profile(pid, ip, duration, format, native):
        response = requests.get(
            f"{webui_url}/worker/memory_profile?"
            f"pid={pid}&ip={ip}&duration={duration}"
            f"&format={format}&native={native}"
        )
        return response

    def run_profiler(pid, ip, duration, format, native):
        response = request_mem_profile(pid, ip, duration, format, native)
        response.raise_for_status()
        content = response.content.decode("utf-8")
        assert "text/html" in response.headers["Content-Type"], response.headers
        return content

    # incorrect format
    response = request_mem_profile(pid, ip, 5, "random", "0")
    with pytest.raises(requests.exceptions.HTTPError):
        assert "Unsupported format random is given." in response.text
        response.raise_for_status()

    # Run memory profiler
    start = time.time()
    result = run_profiler(pid, ip, 1, "flamegraph", "0")
    # Test duration.
    assert time.time() - start >= 1
    # Verify the result is a html file.
    assert "</html>" in result

    # Running it again still should work.
    result = run_profiler(pid, ip, 0, "flamegraph", "0")
    assert "</html>" in result

    # Test table.
    b = Actor.remote()
    pid = ray.get(b.getpid.remote())
    result = run_profiler(pid, ip, 1, "table", "0")
    assert "</html>" in result


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
