import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from anyscale.ci.ray_wheels_lib import (
    PYTHON_VERSIONS,
    _get_wheel_names,
    check_wheels_exist_on_s3,
)


def test_get_wheel_names():
    ray_version = "1.11.0"
    wheel_names = _get_wheel_names(ray_version)

    assert len(wheel_names) == len(PYTHON_VERSIONS)

    for wheel_name in wheel_names:
        assert len(wheel_name.split("-")) == 5
        (
            ray_type,
            ray_version,
            python_version,
            python_version2,
            platform,
        ) = wheel_name.split("-")
        platform = platform.split(".")[0]  # Remove the .whl suffix

        assert ray_type == "ray"
        assert ray_version == ray_version
        assert f"{python_version}-{python_version2}" in PYTHON_VERSIONS
        assert platform == "manylinux2014_x86_64"


@pytest.fixture
def start_test_http_server():
    class RayWheelsServer(BaseHTTPRequestHandler):
        request_paths = []

        def do_GET(self):
            if "/1111111/" in self.path:
                self.send_response(404)
            else:
                self.send_response(200)
            self.request_paths.append(self.path)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()

    server = HTTPServer(("", 8080), RayWheelsServer)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start()

    yield (
        RayWheelsServer,
        f"http://{server.server_address[0]}:{server.server_address[1]}",
    )

    server.shutdown()
    server_thread.join()


def test_check_wheels_exist_on_s3(start_test_http_server):
    ray_wheels_server, url = start_test_http_server
    wheel_names = _get_wheel_names(ray_version="1.0.0")
    expected_request_paths = [
        f"/releases/1.0.0/1234567/{wheel_name}.whl" for wheel_name in wheel_names
    ]
    result = check_wheels_exist_on_s3(
        ray_wheels_s3_url=url,
        branch="releases/1.0.0",
        commit_hash="1234567",
        ray_version="1.0.0",
    )
    assert result is True
    for request_path in ray_wheels_server.request_paths:
        if request_path in expected_request_paths:
            expected_request_paths.remove(request_path)
    assert len(expected_request_paths) == 0


def test_check_wheels_exist_on_s3_fail(start_test_http_server):
    ray_wheels_server, url = start_test_http_server

    wheel_names = _get_wheel_names(ray_version="1.0.0")
    # 1111111 commit does not exist
    expected_request_path = f"/releases/1.0.0/1111111/{wheel_names[0]}.whl"
    result = check_wheels_exist_on_s3(
        ray_wheels_s3_url=url,
        branch="releases/1.0.0",
        commit_hash="1111111",
        ray_version="1.0.0",
    )
    assert result is False
    assert ray_wheels_server.request_paths == [expected_request_path]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
