"""Real NIXL transfers through two prefill, decode, and ingress replicas."""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import yaml

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray.llm._internal.serve.core.configs.openai_api_models import CompletionRequest
from ray.serve.llm import build_pd_openai_app

from pd_kv_test_utils import GATE_NAMESPACE, PhaseGate, patch_pd_classes

CONFIG_PATH = Path(__file__).with_name("serve_pd_kv_router.yaml")
LONG_PROMPT = (
    "Read this context, then finish the final sentence with the city name. "
    + "The question is about national capitals and the answer is a single city. " * 12
    + "The capital of France is"
)


async def _broadcast(router, method, *args):
    return await router.broadcast(method, *args).results_async()


async def _states(router, stage, request_id):
    return await _broadcast(router, "get_stage_state", stage, request_id)


async def _workers(router, stage, ingress_count):
    async def registered():
        per_ingress = await _broadcast(router, "get_stage_workers", stage)
        return len(per_ingress) == ingress_count and all(
            len(workers) == 2 and workers == per_ingress[0] for workers in per_ingress
        )

    await async_wait_for_condition(registered, timeout=120, retry_interval_ms=500)
    return (await _broadcast(router, "get_stage_workers", stage))[0]


async def _route_records(router):
    return {
        request_id: route
        for records in await _broadcast(router, "get_route_records")
        for request_id, route in records.items()
    }


async def _complete_and_route(router, payload):
    before = set(await _route_records(router))
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=120
    ) as client:
        response = await client.post("/v1/completions", json=payload)
    assert response.status_code == 200, response.text
    new_routes = {
        request_id: route
        for request_id, route in (await _route_records(router)).items()
        if request_id not in before
    }
    assert len(new_routes) == 1, new_routes
    return next(iter(new_routes.items()))


async def _transfer_params(router, request_id):
    traces = await _broadcast(router, "get_request_trace", request_id)
    return next(
        trace["transfer_params"] for trace in traces if trace["transfer_params"]
    )


async def _assert_prefill_engine(deployed_app, request_id, replica_id):
    async def engines_known():
        identities = await _broadcast(deployed_app["prefill"], "get_connector_identity")
        return len(identities) == 2

    await async_wait_for_condition(engines_known, timeout=30)
    engine_ids = dict(
        await _broadcast(deployed_app["prefill"], "get_connector_identity")
    )
    params = await _transfer_params(deployed_app["router"], request_id)
    assert params["remote_engine_id"] == engine_ids[replica_id]


@pytest.fixture(scope="module")
def deployed_app():
    with CONFIG_PATH.open() as config_file:
        app_config = yaml.safe_load(config_file)["applications"][0]

    if not ray.is_initialized():
        ray.init(address="auto")
    assert ray.cluster_resources().get("GPU", 0) >= 4
    serve.shutdown()
    gate_name = f"pd-kv-gate-{uuid4().hex}"
    gate = PhaseGate.options(name=gate_name, namespace=GATE_NAMESPACE).remote()
    try:
        serve.start(proxy_location="HeadOnly")
        with patch_pd_classes(gate_name):
            serve.run(
                build_pd_openai_app(app_config["args"]),
                name=app_config["name"],
                route_prefix=app_config["route_prefix"],
            )
        router = serve.get_deployment_handle("LLMRouter", app_name=app_config["name"])
        router._init(_run_router_in_separate_loop=True)
        model = app_config["args"]["decode_config"]["model_loading_config"]["model_id"]
        # Serve readiness can precede HAProxy's final backend reload.
        wait_for_condition(
            lambda: httpx.post(
                "http://127.0.0.1:8000/v1/completions",
                json={"model": model, "prompt": "ready", "max_tokens": 1},
                timeout=30,
            ).status_code
            == 200,
            timeout=120,
            retry_interval_ms=500,
        )

        async def idle():
            for stage in ("prefill", "decode"):
                states = await _states(router, stage, "warmup")
                if len(states) != 2 or any(
                    load["active_requests"] != 0
                    for state in states
                    for load in state["loads"].values()
                ):
                    return False
            return True

        asyncio.run(async_wait_for_condition(idle, timeout=30))
        yield {
            "model": model,
            "router": router,
            "prefill": serve.get_deployment_handle(
                "Prefill:qwen", app_name=app_config["name"]
            ),
            "decode": serve.get_deployment_handle(
                "Decode:qwen", app_name=app_config["name"]
            ),
            "gate": gate,
        }
    finally:
        serve.shutdown()
        ray.kill(gate)
        ray.shutdown()


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_generation(deployed_app):
    """The YAML app serves streamed completions and chat generation."""
    model = deployed_app["model"]
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=90
    ) as client:
        # Exercise the direct streaming path through P, D, and HAProxy.
        async with client.stream(
            "POST",
            "/v1/completions",
            json={
                "model": model,
                "prompt": "The capital of France is",
                "max_tokens": 24,
                "temperature": 0,
                "stream": True,
            },
        ) as response:
            assert response.status_code == 200
            text, done = "", False
            async for line in response.aiter_lines():
                if line == "data: [DONE]":
                    done = True
                elif line.startswith("data: "):
                    text += "".join(
                        choice["text"]
                        for choice in json.loads(line[6:]).get("choices", [])
                    )
            assert done and "Paris" in text

        # Chat uses the same P/D deployment with a different OpenAI endpoint.
        response = await client.post(
            "/v1/chat/completions",
            json={
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": "What is the capital of France? Answer with the city name only.",
                    }
                ],
                "max_tokens": 32,
                "temperature": 0,
                "chat_template_kwargs": {"enable_thinking": False},
            },
        )
        assert response.status_code == 200, response.text
        assert "Paris" in response.json()["choices"][0]["message"]["content"]

        # Repeated client IDs must still produce distinct P/D routing attempts.
        responses = await asyncio.gather(
            *[
                client.post(
                    "/v1/completions",
                    headers={"x-request-id": "same-client-id"},
                    json={
                        "model": model,
                        "prompt": "The capital of France is",
                        "max_tokens": 16,
                        "temperature": 0,
                    },
                )
                for _ in range(2)
            ]
        )
        assert all(response.status_code == 200 for response in responses)
        assert len({response.json()["id"] for response in responses}) == 2


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_stream_close_frees_decode(deployed_app):
    """Closing an active HTTP stream frees D load on every ingress replica."""
    router = deployed_app["router"]
    ingress_count = len(await _broadcast(router, "get_replica_id"))
    assert ingress_count == 2
    before = set(await _route_records(router))
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=90
    ) as client:
        async with client.stream(
            "POST",
            "/v1/completions",
            json={
                "model": deployed_app["model"],
                "prompt": "Count slowly from one onward:",
                "max_tokens": 2048,
                "ignore_eos": True,
                "stream": True,
            },
        ) as response:
            assert response.status_code == 200
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    assert line != "data: [DONE]"
                    break
            else:
                pytest.fail("The stream ended before its first completion chunk")

            new_ids = set(await _route_records(router)) - before
            assert len(new_ids) == 1, new_ids
            request_id = new_ids.pop()

            # Prove D is still booked on both ingress replicas before disconnect.
            states = await _states(router, "decode", request_id)
            assert len(states) == ingress_count
            assert all(
                state["request"] is not None
                and state["loads"][state["request"]["worker_id"]]["active_requests"] > 0
                for state in states
            )

        # Closing the client stream must broadcast D completion to both routers.
        async def decode_freed():
            states = await _states(router, "decode", request_id)
            traces = await _broadcast(router, "get_request_trace", request_id)
            return (
                len(states) == len(traces) == ingress_count
                and all(
                    state["request"] is None
                    and all(
                        load["active_requests"] == 0 for load in state["loads"].values()
                    )
                    for state in states
                )
                and all(
                    ("decode", "on_request_completed")
                    in {(stage, name) for stage, name, _ in trace["events"]}
                    for trace in traces
                )
            )

        await async_wait_for_condition(decode_freed, timeout=30)
        p_states = await _states(router, "prefill", request_id)
        assert len(p_states) == ingress_count
        assert all(
            state["request"] is None
            and all(load["active_requests"] == 0 for load in state["loads"].values())
            for state in p_states
        )


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_nixl_handoff(deployed_app):
    """P and D reservations converge independently around a real NIXL transfer."""
    router, gate = deployed_app["router"], deployed_app["gate"]
    ingress_ids = await _broadcast(router, "get_replica_id")
    assert len(set(ingress_ids)) == 2
    prefill_workers = await _workers(router, "prefill", len(ingress_ids))
    decode_workers = await _workers(router, "decode", len(ingress_ids))
    payload = {
        "model": deployed_app["model"],
        "prompt": LONG_PROMPT,
        "max_tokens": 64,
        "temperature": 0,
        "ignore_eos": True,
    }
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=180
    ) as client:
        await gate.enable.remote()
        request_task = asyncio.create_task(client.post("/v1/completions", json=payload))
        try:
            # Hold the selected P request until both ingress trackers book it.
            request_id = await asyncio.wait_for(
                gate.reached.remote("prefill_booked"), timeout=90
            )

            async def prefill_synced():
                states = await _states(router, "prefill", request_id)
                return len(states) == len(ingress_ids) and all(
                    state["request"] is not None
                    and state["loads"][state["request"]["worker_id"]][
                        "potential_prefill_tokens"
                    ]
                    > 0
                    for state in states
                )

            await async_wait_for_condition(prefill_synced, timeout=30)
            p_states = await _states(router, "prefill", request_id)
            p_worker = p_states[0]["request"]["worker_id"]
            assert all(state["request"]["worker_id"] == p_worker for state in p_states)
            assert p_worker in prefill_workers
            assert all(
                state["request"] is None
                for state in await _states(router, "decode", request_id)
            )

            await gate.release.remote("prefill_booked")
            assert (
                await asyncio.wait_for(gate.reached.remote("before_decode"), 90)
                == request_id
            )

            # P completion clears P load everywhere before D is selected.
            async def prefill_freed():
                states = await _states(router, "prefill", request_id)
                return all(
                    state["request"] is None
                    and all(
                        load["active_requests"] == 0 for load in state["loads"].values()
                    )
                    for state in states
                )

            await async_wait_for_condition(prefill_freed, timeout=30)
            assert all(
                state["request"] is None
                and all(
                    load["active_requests"] == 0 for load in state["loads"].values()
                )
                for state in await _states(router, "decode", request_id)
            )

            await gate.release.remote("before_decode")
            assert (
                await asyncio.wait_for(gate.reached.remote("decode_booked"), 90)
                == request_id
            )

            # D selection books the same worker on both ingress replicas.
            async def decode_synced():
                states = await _states(router, "decode", request_id)
                return len(states) == len(ingress_ids) and all(
                    state["request"] is not None
                    and state["loads"][state["request"]["worker_id"]]["active_requests"]
                    == 1
                    for state in states
                )

            await async_wait_for_condition(decode_synced, timeout=30)
            d_states = await _states(router, "decode", request_id)
            d_worker = d_states[0]["request"]["worker_id"]
            assert all(state["request"]["worker_id"] == d_worker for state in d_states)
            assert d_worker in decode_workers
            assert all(
                state["request"] is None
                for state in await _states(router, "prefill", request_id)
            )

            await gate.release.remote("decode_booked")
            assert (
                await asyncio.wait_for(gate.reached.remote("decode_completing"), 90)
                == request_id
            )

            # Pause completion to observe live decode progress on both trackers.
            async def decode_progress_synced():
                states = await _states(router, "decode", request_id)
                return all(
                    state["request"] is not None
                    and state["request"]["output_tokens"] > 0
                    for state in states
                )

            await async_wait_for_condition(decode_progress_synced, timeout=30)
            assert all(
                state["request"] is None
                for state in await _states(router, "prefill", request_id)
            )
            await gate.release.remote("decode_completing")
            response = await request_task
        finally:
            await gate.disable.remote()

    assert response.status_code == 200, response.text
    result = response.json()
    assert "Paris" in result["choices"][0]["text"]
    assert result["usage"]["prompt_tokens_details"]["cached_tokens"] > 0

    # Completion frees only D; P must remain idle.
    async def decode_freed():
        states = await _states(router, "decode", request_id)
        return all(
            state["request"] is None
            and all(load["active_requests"] == 0 for load in state["loads"].values())
            for state in states
        )

    await async_wait_for_condition(decode_freed, timeout=30)
    assert all(
        state["request"] is None
        and all(load["active_requests"] == 0 for load in state["loads"].values())
        for state in await _states(router, "prefill", request_id)
    )

    async def all_events_arrived():
        traces = await _broadcast(router, "get_request_trace", request_id)
        return all(
            {
                ("prefill", "on_request_completed"),
                ("decode", "on_decode_progress"),
                ("decode", "on_request_completed"),
            }
            <= {(stage, name) for stage, name, _ in trace["events"]}
            for trace in traces
        )

    await async_wait_for_condition(all_events_arrived, timeout=30)
    traces = await _broadcast(router, "get_request_trace", request_id)
    for trace in traces:
        progress = trace["decode_progress"]
        assert progress, traces
        assert all(
            reported > 0 and tokens == reported and not p_active
            for reported, tokens, p_active in progress
        ), traces
        counts = [tokens for _, tokens, _ in progress]
        assert counts == sorted(set(counts))
    assert sum("prefill" in trace["routes"] for trace in traces) == 1
    assert sum("decode" in trace["routes"] for trace in traces) == 1
    assert (
        next(
            trace["routes"]["prefill"]
            for trace in traces
            if "prefill" in trace["routes"]
        )
        == p_worker
    )
    assert (
        next(
            trace["routes"]["decode"] for trace in traces if "decode" in trace["routes"]
        )
        == d_worker
    )

    # Match the decode server's received NIXL metadata to the selected P engine.
    params = next(
        trace["transfer_params"] for trace in traces if trace["transfer_params"]
    )
    prefill_ids = dict(
        await _broadcast(deployed_app["prefill"], "get_connector_identity")
    )
    assert params["remote_engine_id"] == prefill_ids[prefill_workers[p_worker]]
    assert params["remote_block_ids"]

    async def decode_received_transfer():
        received = dict(
            await _broadcast(deployed_app["decode"], "get_received_params", request_id)
        )
        return received.get(decode_workers[d_worker]) == params

    await async_wait_for_condition(decode_received_transfer, timeout=30)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_prefill_load(deployed_app):
    """With equal overlap, HTTP routing picks the less-loaded P replica."""
    router = deployed_app["router"]
    ingress_count = len(await _broadcast(router, "get_replica_id"))
    worker_replicas = await _workers(router, "prefill", ingress_count)
    workers = sorted(worker_replicas)
    payload = {
        "model": deployed_app["model"],
        "prompt": f"Case {uuid4().hex}: "
        + "Blue lanterns orbit a distant observatory. " * 12
        + "The capital of France is",
        "max_tokens": 16,
        "temperature": 0,
    }
    token_ids = await router.tokenize.remote(
        CompletionRequest.model_validate(payload).model_dump()
    )
    overlaps = await _broadcast(router, "get_stage_overlap", "prefill", token_ids)
    assert all(
        scores[workers[0]]["device_blocks"] == scores[workers[1]]["device_blocks"]
        for scores in overlaps
    )

    busy_id = f"busy-p-{uuid4().hex}"
    try:
        # Hold one P worker busy while a real request traverses the router.
        await router.select_stage.remote(
            "prefill", busy_id, list(range(50000, 52048)), [workers[0]], 16
        )

        async def busy_synced():
            states = await _states(router, "prefill", busy_id)
            return all(
                state["request"] is not None
                and state["loads"][workers[0]]["potential_prefill_tokens"]
                > state["loads"][workers[1]]["potential_prefill_tokens"]
                for state in states
            )

        await async_wait_for_condition(busy_synced, timeout=30)
        request_id, route = await _complete_and_route(router, payload)
        assert route["prefill"] == workers[1]
        await _assert_prefill_engine(
            deployed_app, request_id, worker_replicas[workers[1]]
        )
    finally:
        await router.release_stage.remote("prefill", busy_id)


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_prefill_cache(deployed_app):
    """A real P cache hit attracts the next request when P loads are equal."""
    router = deployed_app["router"]
    ingress_count = len(await _broadcast(router, "get_replica_id"))
    worker_replicas = await _workers(router, "prefill", ingress_count)
    workers = sorted(worker_replicas)
    payload = {
        "model": deployed_app["model"],
        "prompt": f"Case {uuid4().hex}: "
        + "Saffron telescope notes from a quiet mountain station. " * 12
        + "The capital of France is",
        "max_tokens": 16,
        "temperature": 0,
    }
    warm_id, warm_route = await _complete_and_route(router, payload)
    warm_worker = warm_route["prefill"]
    cold_worker = next(worker for worker in workers if worker != warm_worker)
    token_ids = next(
        tokens
        for tokens in await _broadcast(router, "get_route_token_ids", warm_id)
        if tokens is not None
    )

    # Wait for P's KV event, then ensure no ongoing load biases the next choice.
    async def cached_and_idle():
        overlaps = await _broadcast(router, "get_stage_overlap", "prefill", token_ids)
        states = await _states(router, "prefill", warm_id)
        return all(
            scores[warm_worker]["device_blocks"] > scores[cold_worker]["device_blocks"]
            for scores in overlaps
        ) and all(
            state["request"] is None
            and all(load["active_requests"] == 0 for load in state["loads"].values())
            for state in states
        )

    await async_wait_for_condition(cached_and_idle, timeout=60, retry_interval_ms=500)
    next_id, route = await _complete_and_route(router, payload)
    assert route["prefill"] == warm_worker
    await _assert_prefill_engine(deployed_app, next_id, worker_replicas[warm_worker])


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_decode_load(deployed_app):
    """HTTP D routing ignores overlap and avoids heavy decode load."""
    router = deployed_app["router"]
    ingress_count = len(await _broadcast(router, "get_replica_id"))
    worker_replicas = await _workers(router, "decode", ingress_count)
    workers = sorted(worker_replicas)
    prompt = "A unique astronomy passage about cobalt and orbiting lanterns. " * 15
    payload = {
        "model": deployed_app["model"],
        "prompt": prompt + "The capital of Japan is",
        "max_tokens": 16,
        "temperature": 0,
    }
    # A real request leaves prompt KV blocks on one decode replica.
    warm_request_id, warm_route = await _complete_and_route(router, payload)
    warm_worker = warm_route["decode"]
    cold_worker = next(worker for worker in workers if worker != warm_worker)
    token_ids = next(
        tokens
        for tokens in await _broadcast(router, "get_route_token_ids", warm_request_id)
        if tokens is not None
    )
    assert token_ids

    async def overlap_differs():
        per_ingress = await _broadcast(router, "get_stage_overlap", "decode", token_ids)
        return all(
            scores[warm_worker]["device_blocks"] > scores[cold_worker]["device_blocks"]
            for scores in per_ingress
        )

    await async_wait_for_condition(overlap_differs, timeout=60, retry_interval_ms=500)
    booked = []
    try:
        # Make the cached replica busy, then route another real request.
        for index in range(8):
            request_id = f"busy-d-{uuid4().hex}"
            tokens = list(range(60000 + index * 512, 60000 + (index + 1) * 512))
            await router.select_stage.remote(
                "decode", request_id, tokens, [warm_worker], 2048
            )
            booked.append(request_id)

        async def busy_synced():
            states = await _states(router, "decode", booked[-1])
            return all(
                state["request"] is not None
                and state["loads"][warm_worker]["potential_decode_blocks"]
                > state["loads"][cold_worker]["potential_decode_blocks"]
                for state in states
            )

        await async_wait_for_condition(busy_synced, timeout=30)
        request_id, route = await _complete_and_route(router, payload)
        assert route["decode"] == cold_worker

        async def served_by_cold_worker():
            received = dict(
                await _broadcast(
                    deployed_app["decode"], "get_received_params", request_id
                )
            )
            return len(received) == len(workers) and all(
                (params is not None) == (replica_id == worker_replicas[cold_worker])
                for replica_id, params in received.items()
            )

        await async_wait_for_condition(served_by_cold_worker, timeout=30)
    finally:
        for request_id in booked:
            await router.release_stage.remote("decode", request_id)
