from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
import sys

import ray
from ray import serve
from ray.serve.llm.request_router import KVAwareRouter
from ray.util.state import list_actors

from utils import MODEL_ID, build_kv_app, build_kv_config, patch_ingress

APP_NAME = "token_channel_cross_node_test"
NUM_REPLICAS = 2
NUM_REQUESTS = 8
PROMPTS = (
    "Large language models are",
    "The capital of France is",
    "Hello, my name is",
    "Ray Serve is designed to",
)


def _endpoint_host(endpoint):
    return endpoint.rsplit(":", 1)[0].rsplit("/", 1)[-1]


def _gpu_node_ips():
    return {
        node["NodeManagerAddress"]
        for node in ray.nodes()
        if node["Alive"] and node["Resources"].get("GPU", 0) > 0
    }


def _replica_node_ips():
    ip_by_node = {node["NodeID"]: node["NodeManagerAddress"] for node in ray.nodes()}
    return {
        ip_by_node[actor.node_id]
        for actor in list_actors(filters=[("state", "=", "ALIVE")])
        if "LLMServer" in (actor.class_name or "") and actor.node_id in ip_by_node
    }


def _post_completion(host, prompt):
    response = requests.post(
        f"http://{host}:8000/v1/completions",
        json={"model": MODEL_ID, "prompt": prompt, "max_tokens": 8},
        timeout=120,
    )
    assert response.status_code == 200, response.text


@pytest.fixture(scope="module")
def deployed_app():
    if not ray.is_initialized():
        ray.init(address="auto")
    serve.shutdown()

    gpu_node_ips = _gpu_node_ips()
    assert len(gpu_node_ips) == NUM_REPLICAS, gpu_node_ips

    llm_config = build_kv_config(
        request_router_class=KVAwareRouter,
        kv_events_port_base=23557,
        num_replicas=NUM_REPLICAS,
    )
    with patch_ingress():
        serve.run(build_kv_app(llm_config), name=APP_NAME)
    replica_ips = _replica_node_ips()
    assert replica_ips == gpu_node_ips, (replica_ips, gpu_node_ips)
    yield replica_ips
    serve.shutdown()


def _send_batch(host, round_index):
    with ThreadPoolExecutor(max_workers=NUM_REQUESTS) as pool:
        futures = [
            pool.submit(
                _post_completion,
                host,
                f"{PROMPTS[i % len(PROMPTS)]} {round_index}-{i}",
            )
            for i in range(NUM_REQUESTS)
        ]
        for future in futures:
            future.result()


@pytest.mark.asyncio
async def test_router_connects_to_remote_token_receiver(deployed_app):
    replica_ips = deployed_app
    host = sorted(replica_ips)[0]
    router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)

    attempts = []
    cross_node = []
    target_nodes = set()
    for round_index in range(5):
        await router.broadcast("reset_token_pushes").results_async()
        _send_batch(host, round_index)
        reports = await router.broadcast("get_token_push_report").results_async()
        attempts = [
            (report["node_ip"], push) for report in reports for push in report["pushes"]
        ]
        cross_node = [
            (source, push)
            for source, push in attempts
            if push["sent"] and source != _endpoint_host(push["endpoint"])
        ]
        target_nodes = {
            _endpoint_host(push["endpoint"]) for _, push in attempts if push["sent"]
        }
        if cross_node and replica_ips <= target_nodes:
            break

    # A successful nonblocking send means ZMQ connected to the advertised
    # remote endpoint and accepted the payload into that pipe.
    assert cross_node, f"No cross-node token push succeeded: {attempts}"
    assert replica_ips <= target_nodes, (replica_ips, attempts)


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
