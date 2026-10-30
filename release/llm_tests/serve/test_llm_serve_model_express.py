"""ModelExpress scale-out: a cold-started replica, then a peer-started one.

Cluster: 2 nodes x 2 H100. Each replica uses TP=2, so the second replica lands
on the other node and must load its weights from the first over the network.
"""

import pathlib
import sys
import threading
import time

import pytest
import requests
import yaml

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.llm import build_openai_app
from ray.serve.llm.mx import build_mx_app
from ray.util.state import get_log, list_logs, list_nodes

from utils import shutdown_serve_and_wait_for_controller

CONFIG_PATH = (
    pathlib.Path(__file__).parent / "configs" / "serve_qwen3_8b_mx_scaleout.yaml"
)
URL = "http://localhost:8000/v1/chat/completions"
# Logged by each vLLM worker that loaded its weights from a peer.
P2P_LOAD_MARKER = "RDMA transfer complete"


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    yield
    shutdown_serve_and_wait_for_controller()
    ray.shutdown()


def _running_replicas() -> int:
    app = serve.status().applications.get("llm")
    if app is None:
        return 0
    return sum(
        dep.replica_states.get("RUNNING", 0)
        for name, dep in app.deployments.items()
        if name.startswith("LLMServer")
    )


def _loaded_from_peer() -> bool:
    """Whether any vLLM worker's logs show a weight load from a peer."""
    for node in list_nodes(filters=[("state", "=", "ALIVE")]):
        files = list_logs(node_id=node.node_id, glob_filter="worker-*")
        for name in files.get("worker_err", []) + files.get("worker_out", []):
            for line in get_log(node_id=node.node_id, filename=name, tail=-1):
                if P2P_LOAD_MARKER in line:
                    return True
    return False


def _send_load(model_id: str, stop: threading.Event):
    body = {
        "model": model_id,
        "messages": [{"role": "user", "content": "Write a long story."}],
        "max_tokens": 1024,
        "ignore_eos": True,
        "stream": True,
    }
    while not stop.is_set():
        try:
            with requests.post(URL, json=body, stream=True, timeout=600) as resp:
                for _ in resp.iter_lines():
                    if stop.is_set():
                        break
        except requests.RequestException:
            time.sleep(1)


def test_llm_serve_model_express_scaleout():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    apps = {app["name"]: app for app in config["applications"]}
    model_id = apps["llm"]["args"]["llm_configs"][0]["model_loading_config"]["model_id"]

    serve.run(build_mx_app(apps["mx"]["args"]), name="mx", route_prefix=None)

    # Cold start: no peer yet, so replica 1 loads from storage.
    start = time.monotonic()
    serve.run(build_openai_app(apps["llm"]["args"]), name="llm", blocking=False)
    wait_for_condition(
        lambda: _running_replicas() == 1, timeout=1800, retry_interval_ms=5000
    )
    cold_start_s = time.monotonic() - start
    assert not _loaded_from_peer()

    # Peer start: sustained load scales the deployment to 2 replicas.
    stop = threading.Event()
    load = [
        threading.Thread(target=_send_load, args=(model_id, stop), daemon=True)
        for _ in range(8)
    ]
    for t in load:
        t.start()
    try:
        start = time.monotonic()
        wait_for_condition(
            lambda: _running_replicas() == 2, timeout=1800, retry_interval_ms=5000
        )
        peer_start_s = time.monotonic() - start
    finally:
        stop.set()

    # Replica 2 loaded its weights from replica 1, not from storage.
    assert _loaded_from_peer()

    print(f"cold start: {cold_start_s:.0f} s, peer start: {peer_start_s:.0f} s")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
