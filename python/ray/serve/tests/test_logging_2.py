import os
import sys
import uuid

import httpx
import pytest
from fastapi import FastAPI

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.logging_utils import (
    get_serve_logs_dir,
)
from ray.serve._private.utils import get_component_file_name
from ray.util.state import list_nodes


def test_http_access_log_in_proxy_logs_file(serve_instance):
    name = "deployment_name"
    fastapi_app = FastAPI()

    @serve.deployment(name=name)
    @serve.ingress(fastapi_app)
    class Handler:
        @fastapi_app.get("/")
        def get_root(self):
            return "Hello World!"

    serve.run(Handler.bind(), logging_config={"encoding": "TEXT"})

    # Get log file information
    nodes = list_nodes()
    serve_log_dir = get_serve_logs_dir()
    node_ip_address = nodes[0].node_ip
    proxy_log_file_name = get_component_file_name(
        "proxy", node_ip_address, component_type=None, suffix=".log"
    )
    proxy_log_path = os.path.join(serve_log_dir, proxy_log_file_name)

    request_id = str(uuid.uuid4())
    response = httpx.get("http://localhost:8000", headers={"X-Request-ID": request_id})
    assert response.status_code == 200

    def verify_request_id_in_logs(proxy_log_path, request_id):
        with open(proxy_log_path, "r") as f:
            for line in f:
                if request_id in line:
                    return True
        return False

    wait_for_condition(
        verify_request_id_in_logs, proxy_log_path=proxy_log_path, request_id=request_id
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
