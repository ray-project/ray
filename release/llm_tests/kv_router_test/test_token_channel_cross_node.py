import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
import sys

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_LIFECYCLE_OWNER_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    get_worker_id,
)
from ray.serve._private.common import ReplicaID
from ray.serve.llm.request_router import KVAwareRouter
from ray.util.state import list_actors

from utils import (
    MODEL_ID,
    build_kv_app,
    build_kv_config,
    discover_replica_endpoints,
    patch_ingress,
)

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
@pytest.mark.requires_2x_1GPU
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


@pytest.mark.asyncio
@pytest.mark.requires_2x_1GPU
async def test_cross_node_lifecycle_and_replica_mesh(deployed_app):
    """A remote engine reports to its owner; router peers share load and affinity."""
    router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
    server = serve.get_app_handle(APP_NAME)
    endpoints = await discover_replica_endpoints(server, NUM_REPLICAS)
    workers = [
        get_worker_id(ReplicaID.from_full_id_str(replica_id).unique_id)
        for replica_id in endpoints
    ]
    bound, other = workers
    host, port = next(iter(endpoints.values()))
    request_id = "cross-node-lifecycle"
    session_id = "cross-node-session"

    async def broadcast(method, *args):
        return await router.broadcast(method, *args).results_async()

    async def registered():
        ids = await broadcast("get_registered_worker_ids")
        return bool(ids) and all(set(ids_) == set(workers) for ids_ in ids)

    await async_wait_for_condition(registered, timeout=120)
    snapshots = await broadcast("get_lifecycle_snapshot", request_id, bound)
    assert len({_endpoint_host(s["lifecycle_endpoint"]) for s in snapshots}) >= 2
    owner = next(
        s for s in snapshots if _endpoint_host(s["lifecycle_endpoint"]) != host
    )
    peer = next(
        s
        for s in snapshots
        if _endpoint_host(s["lifecycle_endpoint"])
        != _endpoint_host(owner["lifecycle_endpoint"])
    )

    async def mesh_connected():
        peers = await broadcast("get_replica_mesh_peers")
        return len(peers) == len(snapshots) and all(
            len(endpoints_) == len(snapshots) - 1 for endpoints_ in peers
        )

    await async_wait_for_condition(mesh_connected, timeout=60)
    # Dynamo registers peers before the ZMQ subscriptions finish connecting.
    await asyncio.sleep(1)

    # Force the initial session binding onto the worker on the other node.
    async def select(replica_id, request_id, tokens, allowed, session=None):
        results = await broadcast(
            "select_worker_on",
            replica_id,
            request_id,
            tokens,
            allowed,
            32,
            None,
            session,
        )
        (selection,) = [result for result in results if result is not None]
        return selection["worker_id"]

    booked = [request_id] + [f"cross-node-skew-{i}" for i in range(8)]
    try:
        assert (
            await select(
                owner["replica_id"], request_id, list(range(64)), [bound], session_id
            )
            == bound
        )
        for i, skew_id in enumerate(booked[1:]):
            await select(
                owner["replica_id"],
                skew_id,
                list(range(1000 + i * 64, 1064 + i * 64)),
                [bound],
            )

        async def load_is(expected):
            loads = await broadcast("get_worker_active_requests", bound)
            return len(loads) == len(snapshots) and all(
                load == expected for load in loads
            )

        await async_wait_for_condition(load_is, expected=len(booked), timeout=30)

        async def affinity_replicated():
            pinned = await broadcast(
                "get_session_worker", session_id, [10000] * 64, workers
            )
            return all(worker == bound for worker in pinned)

        # Unpinned scoring prefers the idle worker, so only the replicated
        # session binding can explain a peer choosing the loaded worker.
        assert await broadcast("get_session_worker", None, [10000] * 64, workers) == [
            other
        ] * len(snapshots)
        await async_wait_for_condition(affinity_replicated, timeout=30)

        def complete_on_engine():
            response = requests.post(
                f"http://{host}:{port}/v1/completions",
                json={
                    "model": MODEL_ID,
                    "prompt": "Hello, my name is",
                    "max_tokens": 8,
                },
                headers={
                    "X-Request-Id": request_id,
                    KV_LIFECYCLE_OWNER_HEADER: owner["lifecycle_endpoint"],
                },
                timeout=120,
            )
            assert response.status_code == 200, response.text

        await asyncio.to_thread(complete_on_engine)
        # Only the engine's ZMQ events can release this booking; the mesh must
        # then propagate the free to the other node's routers.
        await async_wait_for_condition(load_is, expected=len(booked) - 1, timeout=30)
        readings = await broadcast("get_lifecycle_snapshot", request_id, bound)
        assert all(reading["lifecycle"] is None for reading in readings)
        logs = await broadcast("get_event_log")
        events = [[name for name, args in log if args[0] == request_id] for log in logs]
        assert [names for names in events if names] == [
            ["on_prefill_complete", "on_request_completed"]
        ]
        assert (
            await select(
                peer["replica_id"],
                "cross-node-next-turn",
                [10000] * 64,
                workers,
                session_id,
            )
            == bound
        )
    finally:
        await broadcast(
            "on_lifecycle_events",
            [
                ("on_request_completed", (rid,))
                for rid in booked + ["cross-node-next-turn"]
            ],
        )


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
