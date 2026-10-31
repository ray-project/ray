"""Tests for KV routing state across LLMRouter ingress replicas.

Each ingress owns a local KV selector. The routing ingress atomically selects
and reserves a worker, then asynchronously broadcasts that already-selected
reservation so peer ingresses can mirror the same load state. Engine lifecycle
events are broadcast separately and advance or free reservations admitted
locally. Cached KV blocks are tracked per replica: every ingress independently
subscribes to each engine worker's KV-event stream. Session bindings stream
from the routing ingress's selection service and are broadcast the same way,
so a session stays on its worker whichever ingress routes its next turn.
"""

import asyncio
import sys
import threading

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.serve._private.test_utils import get_application_url
from ray.serve.llm.request_router import KVAwareRouter

from utils import MODEL_ID, build_kv_app, build_kv_config, patch_ingress

APP_NAME = "token_load_test"
KV_EVENTS_PORT_BASE = 21800
MAX_TOKENS = 16
BLOCK_SIZE = 16
# A multi-block prompt so the engine caches retrievable KV blocks.
PROMPT_TEXT = (
    "Repeat the following instruction carefully and then answer it in full "
    "detail: describe the water cycle, including evaporation, condensation, "
    "precipitation, and collection, giving a concrete real-world example for "
    "each of the four stages and how they connect."
)
MESSAGES = [{"role": "user", "content": PROMPT_TEXT}]


def _post_chat(
    endpoint,
    max_tokens=MAX_TOKENS,
    request_id=None,
    stream=False,
    messages=MESSAGES,
    session_id=None,
):
    """Send one chat completion through the deployment's HTTP ingress."""
    if isinstance(endpoint, tuple):
        host, port = endpoint
        url = f"http://{host}:{port}/v1/chat/completions"
    else:
        url = f"{endpoint}/v1/chat/completions"
    headers = {}
    if request_id:
        headers["X-Request-Id"] = request_id
    if session_id:
        headers["x-session-id"] = session_id
    with requests.post(
        url,
        json={
            "model": MODEL_ID,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0.0,
            "ignore_eos": True,
            "stream": stream,
        },
        headers=headers,
        stream=stream,
        timeout=300,
    ) as resp:
        assert resp.status_code == 200, resp.text
        if stream:
            for _ in resp.iter_content(chunk_size=None):
                pass


def _tokenize_chat(endpoint):
    """The engine's exact token ids for the chat-templated prompt."""
    host, port = endpoint
    resp = requests.post(
        f"http://{host}:{port}/tokenize",
        json={"model": MODEL_ID, "messages": MESSAGES, "add_generation_prompt": True},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["tokens"]


def _num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence (matches the indexer)."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


def _restart_serve(ingress_replicas_per_node):
    """Start a fresh controller with the ingress replica count.

    The controller reads RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE from its
    own environment at creation.
    """
    serve.shutdown()
    serve.start(
        controller_options={
            "runtime_env": {
                "env_vars": {
                    "RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE": str(
                        ingress_replicas_per_node
                    )
                }
            }
        }
    )


async def _broadcast(router, method, *args):
    return await router.broadcast(method, *args).results_async()


async def _ingress_replica_ids(router, ingress_replicas_per_node):
    """Every ingress replica's id. The per-node setting applies to each proxy
    node, so the total depends on the cluster's shape; read it off the
    broadcast rather than assuming it equals the per-node setting."""
    replica_ids = await _broadcast(router, "get_replica_id")
    assert len(set(replica_ids)) == len(replica_ids)
    assert len(replica_ids) >= ingress_replicas_per_node
    return replica_ids


async def _registered_workers(router, num_replicas, num_workers, timeout=120):
    """Wait until every ingress replica can schedule all ``num_workers`` engine
    workers and return their ids."""

    async def registered():
        per_replica = await _broadcast(router, "get_registered_worker_ids")
        return len(per_replica) == num_replicas and all(
            len(ids) == num_workers for ids in per_replica
        )

    await async_wait_for_condition(registered, timeout=timeout, retry_interval_ms=1000)
    return (await _broadcast(router, "get_registered_worker_ids"))[0]


async def _registered_worker(router, num_replicas):
    """Wait until every ingress replica can schedule the engine's worker and
    return the worker id."""
    return (await _registered_workers(router, num_replicas, num_workers=1))[0]


async def _backend_endpoint(handle):
    """Poll for the replica's backend HTTP (host, port); it is reported
    asynchronously after the replica starts its direct-ingress server."""
    for _ in range(60):
        async with handle.choose_replica() as selection:
            endpoint = selection._replica.backend_http_endpoint
        if endpoint is not None:
            return endpoint
        await asyncio.sleep(1.0)
    raise AssertionError("replica backend HTTP endpoint never became available")


@pytest.mark.parametrize("ingress_replicas_per_node", [1, 2], scope="class")
class TestIngressSynchronization:
    @pytest.fixture(scope="class")
    def deployed_handle(self, ingress_replicas_per_node):
        if not ray.is_initialized():
            ray.init(address="auto")
        _restart_serve(ingress_replicas_per_node=ingress_replicas_per_node)
        llm_config = build_kv_config(
            request_router_class=KVAwareRouter,
            kv_events_port_base=KV_EVENTS_PORT_BASE,
        )
        with patch_ingress():
            handle = serve.run(build_kv_app(llm_config), name=APP_NAME)
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    async def test_booked_load(self, ingress_replicas_per_node, deployed_handle):
        """One ingress selects atomically, then every ingress converges through
        booking, prefill completion, and request completion."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        replica_ids = await _ingress_replica_ids(router, ingress_replicas_per_node)
        num_ingress_replicas = len(replica_ids)
        worker_id = await _registered_worker(router, num_ingress_replicas)

        loads = await _broadcast(router, "get_worker_load", worker_id)
        assert all(load["active_requests"] == 0 for load in loads)

        token_ids = list(range(64))
        selection = await router.select_worker.remote(
            "probe", token_ids, [worker_id], 32
        )
        assert selection["worker_id"] == worker_id

        async def reservation_converged():
            states = await _broadcast(router, "get_request_lifecycle", "probe")
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return (
                len(states) == num_ingress_replicas
                and all(state is not None for state in states)
                and all(load["active_requests"] == 1 for load in loads)
                and len({load["potential_prefill_tokens"] for load in loads}) == 1
                and loads[0]["potential_prefill_tokens"] > 0
            )

        await async_wait_for_condition(
            reservation_converged, timeout=30, retry_interval_ms=100
        )

        await _broadcast(
            router, "on_lifecycle_events", [("on_prefill_complete", ("probe",))]
        )

        async def prefill_converged():
            states = await _broadcast(router, "get_request_lifecycle", "probe")
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return all(
                state is not None and state["prefill_completed"] for state in states
            ) and all(
                load["active_requests"] == 1 and load["potential_prefill_tokens"] == 0
                for load in loads
            )

        await async_wait_for_condition(
            prefill_converged, timeout=30, retry_interval_ms=100
        )

        await _broadcast(
            router, "on_lifecycle_events", [("on_request_completed", ("probe",))]
        )

        async def completion_converged():
            states = await _broadcast(router, "get_request_lifecycle", "probe")
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return all(state is None for state in states) and all(
                load["active_requests"] == 0 for load in loads
            )

        await async_wait_for_condition(
            completion_converged, timeout=30, retry_interval_ms=100
        )

    @pytest.mark.asyncio
    async def test_real_request_load(self, ingress_replicas_per_node, deployed_handle):
        """A streamed request is eventually booked and freed on every ingress."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        num_ingress_replicas = len(
            await _ingress_replica_ids(router, ingress_replicas_per_node)
        )
        worker_id = await _registered_worker(router, num_ingress_replicas)
        # Atomic reservation happens in LLMRouter while HAProxy routes the
        # request. A backend HTTP endpoint bypasses that selection path, so it
        # cannot exercise the reservation or its ingress-to-ingress broadcast.
        endpoint = get_application_url(app_name=APP_NAME, use_localhost=True)

        # A long generation keeps the request in flight while we observe it.
        request_errors = []

        def post_chat():
            try:
                _post_chat(
                    endpoint,
                    max_tokens=1024,
                    request_id="token-load-inflight-1",
                    stream=True,
                )
            except Exception as e:
                request_errors.append(e)

        request_thread = threading.Thread(
            target=post_chat,
        )
        request_thread.start()
        try:

            async def is_active():
                loads = await _broadcast(router, "get_worker_load", worker_id)
                return len(loads) == num_ingress_replicas and all(
                    load["active_requests"] >= 1 for load in loads
                )

            await async_wait_for_condition(is_active, timeout=60, retry_interval_ms=200)
        finally:
            request_thread.join(timeout=300)
        assert not request_thread.is_alive()
        if request_errors:
            raise request_errors[0]

        async def is_cleared():
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return all(load["active_requests"] == 0 for load in loads)

        await async_wait_for_condition(is_cleared, timeout=30, retry_interval_ms=500)

    @pytest.mark.asyncio
    async def test_kv_events_indexed(self, ingress_replicas_per_node, deployed_handle):
        """Engine KV-cache events reach every ingress replica's indexer, so all
        replicas score the cached prefix alike."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        num_ingress_replicas = len(
            await _ingress_replica_ids(router, ingress_replicas_per_node)
        )
        worker_id = await _registered_worker(router, num_ingress_replicas)

        # Warm the engine's cache; it emits stored-block events every ingress
        # indexer subscribes to.
        endpoint = await _backend_endpoint(deployed_handle)
        _post_chat(endpoint)
        prompt_token_ids = _tokenize_chat(endpoint)
        prompt_blocks = _num_prompt_blocks(prompt_token_ids)
        assert prompt_blocks >= 1

        async def is_indexed():
            per_replica = await _broadcast(
                router, "get_kv_overlap_blocks", prompt_token_ids
            )
            return len(per_replica) == num_ingress_replicas and all(
                overlaps.get(worker_id) == prompt_blocks for overlaps in per_replica
            )

        await async_wait_for_condition(is_indexed, timeout=60, retry_interval_ms=500)


SESSION_APP_NAME = "session_affinity_test"
SESSION_KV_EVENTS_PORT_BASE = 21900
# Long enough for the pinning tests, short enough to wait out.
SESSION_TTL_S = 30
# Unpinned reservations that make the bound worker the costlier choice.
SKEW_REQUESTS = 8


def _token_ids(seed):
    """A distinct 64-token prompt, so KV overlap never decides a selection."""
    return list(range(seed * 1000, seed * 1000 + 64))


async def _session_deployment(router):
    """Every ingress replica id and the two engine workers' ids."""
    ingress_ids = await _ingress_replica_ids(router, 2)
    workers = await _registered_workers(
        router, len(ingress_ids), num_workers=2, timeout=300
    )
    return ingress_ids, workers


async def _select_on(router, replica_id, request_id, seed, allowed, session_id=None):
    """Select on one ingress replica and return the chosen worker id."""
    results = await _broadcast(
        router,
        "select_worker_on",
        replica_id,
        request_id,
        _token_ids(seed),
        allowed,
        32,
        None,
        session_id,
    )
    (selection,) = [result for result in results if result is not None]
    return selection["worker_id"]


async def _complete(router, *request_ids):
    """Complete requests on every ingress and wait until none tracks them."""
    await _broadcast(
        router,
        "on_lifecycle_events",
        [("on_request_completed", (request_id,)) for request_id in request_ids],
    )

    async def released():
        for request_id in request_ids:
            states = await _broadcast(router, "get_request_lifecycle", request_id)
            if any(state is not None for state in states):
                return False
        return True

    await async_wait_for_condition(released, timeout=30, retry_interval_ms=200)


async def _skew_load(router, replica_id, worker_id, prefix):
    """Book unpinned load on ``worker_id`` until every ingress sees it."""
    request_ids = [f"{prefix}-{i}" for i in range(SKEW_REQUESTS)]
    for i, request_id in enumerate(request_ids):
        await _select_on(router, replica_id, request_id, 100 + i, [worker_id])

    async def converged():
        loads = await _broadcast(router, "get_worker_load", worker_id)
        return all(load["active_requests"] == SKEW_REQUESTS for load in loads)

    await async_wait_for_condition(converged, timeout=30, retry_interval_ms=200)
    return request_ids


async def _wait_for_binding(router, session_id, worker_id, allowed):
    """Wait until every ingress routes ``session_id`` to ``worker_id``.

    Call it while load favors another worker, so only a replicated binding can
    explain the answer.
    """

    async def bound_everywhere():
        workers = await _broadcast(
            router, "get_session_worker", session_id, _token_ids(0), allowed
        )
        return all(worker == worker_id for worker in workers)

    await async_wait_for_condition(bound_everywhere, timeout=30, retry_interval_ms=200)


class TestSessionAffinity:
    """Session pinning through each ingress's real Dynamo selection service,
    with two engine replicas and two ingress replicas that replicate bindings
    to each other."""

    @pytest.fixture(scope="class", autouse=True)
    def deployment(self):
        if not ray.is_initialized():
            ray.init(address="auto")
        _restart_serve(ingress_replicas_per_node=2)
        llm_config = build_kv_config(
            request_router_class=KVAwareRouter,
            kv_events_port_base=SESSION_KV_EVENTS_PORT_BASE,
            num_replicas=2,
            request_router_kwargs={"session_affinity_ttl_s": SESSION_TTL_S},
        )
        with patch_ingress():
            serve.run(build_kv_app(llm_config), name=SESSION_APP_NAME)
        yield
        serve.shutdown()

    @pytest.mark.asyncio
    async def test_binding_reaches_every_ingress(self):
        """A session bound through one ingress stays on its worker through every
        ingress, even when load favors the other worker."""
        router = serve.get_deployment_handle("LLMRouter", app_name=SESSION_APP_NAME)
        ingress_ids, (other, bound) = await _session_deployment(router)
        workers = [other, bound]

        worker = await _select_on(router, ingress_ids[0], "a-1", 1, [bound], "a")
        assert worker == bound
        await _complete(router, "a-1")

        skew = await _skew_load(router, ingress_ids[0], bound, "a-skew")
        try:
            control = await _select_on(router, ingress_ids[-1], "a-control", 2, workers)
            assert control == other, "load should favor the unbound worker"
            await _complete(router, "a-control")

            await _wait_for_binding(router, "a", bound, workers)
            for turn, ingress_id in enumerate(ingress_ids, start=2):
                request_id = f"a-{turn}"
                worker = await _select_on(
                    router, ingress_id, request_id, turn + 10, workers, "a"
                )
                assert worker == bound, f"turn {turn} via {ingress_id} unpinned"
                await _complete(router, request_id)
        finally:
            await _complete(router, *skew)

    @pytest.mark.asyncio
    async def test_failover_rebind_reaches_every_ingress(self):
        """When Serve excludes the bound worker, the session moves to an allowed
        one, and every ingress follows the new binding."""
        router = serve.get_deployment_handle("LLMRouter", app_name=SESSION_APP_NAME)
        ingress_ids, (first, second) = await _session_deployment(router)
        workers = [first, second]

        worker = await _select_on(router, ingress_ids[0], "b-1", 1, [first], "b")
        assert worker == first
        await _complete(router, "b-1")
        worker = await _select_on(router, ingress_ids[0], "b-2", 2, [second], "b")
        assert worker == second
        await _complete(router, "b-2")

        # Load now favors ``first``, where the old binding pointed.
        skew = await _skew_load(router, ingress_ids[0], second, "b-skew")
        try:
            await _wait_for_binding(router, "b", second, workers)
            for turn, ingress_id in enumerate(ingress_ids, start=3):
                request_id = f"b-{turn}"
                worker = await _select_on(
                    router, ingress_id, request_id, turn + 10, workers, "b"
                )
                assert worker == second, f"turn {turn} via {ingress_id} unpinned"
                await _complete(router, request_id)
        finally:
            await _complete(router, *skew)

    @pytest.mark.asyncio
    async def test_http_session_turns_stay_pinned(self):
        """Chat turns sent over HTTP with ``x-session-id`` reach KVAwareRouter
        with their session and stay on one worker; an unpinned request follows
        load instead."""
        router = serve.get_deployment_handle("LLMRouter", app_name=SESSION_APP_NAME)
        ingress_ids, workers = await _session_deployment(router)
        endpoint = get_application_url(app_name=SESSION_APP_NAME, use_localhost=True)

        def chat(text, session_id=None):
            _post_chat(
                endpoint,
                max_tokens=8,
                messages=[{"role": "user", "content": text}],
                session_id=session_id,
            )

        async def routed(session_id):
            per_ingress = await _broadcast(router, "get_selections")
            return [
                selection["worker_id"]
                for log in per_ingress
                for selection in log
                if selection["session_id"] == session_id
            ]

        chat("Name three primary colors.", session_id="http")
        (bound,) = await routed("http")
        (other,) = [worker for worker in workers if worker != bound]

        skew = await _skew_load(router, ingress_ids[0], bound, "http-skew")
        try:
            await _wait_for_binding(router, "http", bound, workers)
            chat("Name three planets in the solar system.")
            assert await routed(None) == [other], "load should favor the other worker"

            for text in (
                "Name three musical instruments.",
                "Name three kinds of trees.",
                "Name three ocean animals.",
            ):
                chat(text, session_id="http")
            assert await routed("http") == [bound] * 4
        finally:
            await _complete(router, *skew)

    @pytest.mark.asyncio
    async def test_expired_session_rebinds(self):
        """A session bound on one ingress follows load on another ingress once
        it has been idle past the TTL."""
        router = serve.get_deployment_handle("LLMRouter", app_name=SESSION_APP_NAME)
        ingress_ids, (other, bound) = await _session_deployment(router)
        workers = [other, bound]

        skew = await _skew_load(router, ingress_ids[0], bound, "ttl-skew")
        try:
            worker = await _select_on(router, ingress_ids[0], "ttl-1", 1, [bound], "t")
            assert worker == bound
            await _complete(router, "ttl-1")
            await _wait_for_binding(router, "t", bound, workers)

            await asyncio.sleep(SESSION_TTL_S + 1)
            worker = await _select_on(router, ingress_ids[-1], "ttl-2", 2, workers, "t")
            assert worker == other, "expired session should follow load"
        finally:
            await _complete(router, *skew, "ttl-2")


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
