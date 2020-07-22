import sys

import requests
import pytest

import ray
from ray import serve
from ray.serve.constants import SERVE_PROXY_NAME
from ray.cluster_utils import Cluster


def test_multiple_routers():
    cluster = Cluster()
    head_node = cluster.add_node()
    cluster.add_node()

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2
    serve.init()

    # two actors should be started
    head_http = ray.get_actor(SERVE_PROXY_NAME + "-0")
    worker_http = ray.get_actor(SERVE_PROXY_NAME + "-1")

    # they should be healthy
    assert ray.get(head_http.ready.remote())
    assert ray.get(worker_http.ready.remote())

    # we can send requests to them
    assert requests.get("http://127.0.0.1:8000/-/routes").status_code == 200

    # kill the head_http server, the HTTP server should still functions
    ray.kill(head_http, no_restart=True)
    assert requests.get("http://127.0.0.1:8000/-/routes").status_code == 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
