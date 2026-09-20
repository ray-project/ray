import asyncio
import json
import sys

import httpx
import pytest

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import (
    SignalActor,
    async_wait_for_condition,
    fetch_prometheus_metrics,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress import router as router_module
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    consistent_hash_deployment_config,
    requires_direct_streaming,
    run_app_through_haproxy,
    session_chat_response,
)


@requires_direct_streaming
class TestDirectStreamingConsistentHashRouting:
    """Session affinity over the full direct-streaming path.

    A request flows through HAProxy and the LLMRouter ``/internal/route``
    decision (ConsistentHashRouter) to a backend replica. The session id
    reaches the chosen replica, and one session pins to one replica.
    """

    @pytest.fixture(name="llm_config")
    def _llm_config(self):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id="test-model"))

    @pytest.fixture(name="base_url")
    def run_direct_streaming_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        llm_config = llm_config_with_mock_engine
        llm_config.deployment_config = consistent_hash_deployment_config()
        yield run_app_through_haproxy(
            build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        )

    def test_session_affinity(self, base_url):
        replicas = {
            session_chat_response(base_url, "test-session-id").headers["x-replica-id"]
            for _ in range(10)
        }
        assert len(replicas) == 1

    def test_different_sessions_spread(self, base_url):
        replicas = {
            session_chat_response(base_url, f"test-session-id-{i}").headers[
                "x-replica-id"
            ]
            for i in range(10)
        }
        assert len(replicas) > 1


@requires_direct_streaming
@pytest.mark.asyncio
async def test_ingress_router_fallback(
    shutdown_ray_and_serve, disable_placement_bundles, monkeypatch
):
    """Verify direct streaming falls back when the ingress router replica dies.

    Streaming and non-streaming requests must succeed while the router is unavailable,
    and routing must resume through the replacement replica.
    """
    # Set before starting Ray so the proxy actors inherit these settings.
    monkeypatch.setenv("RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY", "1")
    monkeypatch.setenv("RAY_SERVE_HAPROXY_METRICS_ENABLED", "0")
    monkeypatch.setenv("RAY_SERVE_INGRESS_REQUEST_ROUTER_METRICS_ENABLED", "1")
    monkeypatch.setenv("RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S", "0.01")
    metrics_port = find_free_port()
    ray.init(
        num_cpus=4,
        include_dashboard=False,
        _metrics_export_port=metrics_port,
        _system_config={"metrics_report_interval_ms": 100},
    )
    gate = SignalActor.remote()
    await gate.send.remote()

    class GatedLLMRouter(router_module.LLMRouter):
        async def __init__(self, *args, **kwargs):
            await gate.wait.remote()
            await super().__init__(*args, **kwargs)
            self._num_requests = 0

        async def _pick_replica(self, *args, **kwargs):
            replica = await super()._pick_replica(*args, **kwargs)
            self._num_requests += 1
            return replica

        async def get_num_requests(self):
            return self._num_requests

        async def get_actor(self):
            return ray.get_runtime_context().current_actor

    # Delay the replacement router's startup so we can test fallback while no
    # router replicas are available.
    monkeypatch.setattr(router_module, "LLMRouter", GatedLLMRouter)
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-model"),
        accelerator_type=None,
        runtime_env={
            "env_vars": {
                "RAYLLM_VLLM_ENGINE_CLS": "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine",
            }
        },
        deployment_config={
            "num_replicas": 2,
            "ray_actor_options": {"num_cpus": 0.1},
            "graceful_shutdown_timeout_s": 1,
            "graceful_shutdown_wait_loop_s": 0.1,
        },
        log_engine_metrics=False,
    )
    app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
    assert app._ingress_request_router is not None
    assert (
        app._bound_deployment._deployment_config.request_router_config.ingress_router_fallback
    )
    app_name = "llm-fallback-lifecycle"
    num_requests = 0
    try:
        serve.run(app, name=app_name)
        router = serve.get_deployment_handle("GatedLLMRouter", app_name=app_name)
        actor = await router.get_actor.remote()

        async def send_requests():
            nonlocal num_requests
            async with httpx.AsyncClient(
                timeout=20, limits=httpx.Limits(max_keepalive_connections=0)
            ) as client:

                async def send(i):
                    request_id = f"llm-lifecycle-{i}"
                    response = await client.post(
                        "http://127.0.0.1:8000/v1/completions",
                        json={
                            "model": "test-model",
                            "prompt": f"hello {i}",
                            "max_tokens": 3,
                            "stream": bool(i % 2),
                            "request_id": request_id,
                        },
                        headers={"x-request-id": request_id},
                    )
                    assert response.status_code == 200, response.text
                    assert response.headers["x-replica-id"]
                    if i % 2:
                        events = [
                            line.removeprefix("data: ")
                            for line in response.text.splitlines()
                            if line
                        ]
                        assert events[-1] == "[DONE]"
                        chunks = [json.loads(event) for event in events[:-1]]
                        assert all(chunk["id"] == request_id for chunk in chunks)
                        assert (
                            "".join(chunk["choices"][0]["text"] for chunk in chunks)
                            == "test_0 test_1 test_2"
                        )
                        assert chunks[-1]["choices"][0]["finish_reason"] == "stop"
                    else:
                        body = response.json()
                        assert body["id"] == request_id
                        assert body["choices"][0]["text"] == "test_0 test_1 test_2"
                        assert body["usage"]["prompt_tokens"] == 2

                await asyncio.gather(
                    *(send(i) for i in range(num_requests, num_requests + 8))
                )
                num_requests += 8

        def has_fallbacks():
            metrics = fetch_prometheus_metrics([f"127.0.0.1:{metrics_port}"])
            return any(
                sample.labels.get("application") == app_name
                and sample.labels["reason"] == "router_unavailable"
                and sample.value >= 8
                for sample in metrics.get(
                    "ray_serve_haproxy_ingress_router_fallbacks_total", []
                )
            )

        await send_requests()
        assert await router.get_num_requests.remote() == 8
        await gate.send.remote(clear=True)
        ray.kill(actor, no_restart=True)

        stop = asyncio.Event()

        async def send_traffic():
            while not stop.is_set():
                await send_requests()
                await asyncio.sleep(0.1)

        task = asyncio.create_task(send_traffic())
        try:
            # Traffic continues across both the dead endpoint and empty router
            # pool. Only observations are polled; request failures fail the task.
            await async_wait_for_condition(has_fallbacks, timeout=60)
            await gate.send.remote()

            async def router_recovered():
                return await router.get_num_requests.remote() >= 8

            await async_wait_for_condition(router_recovered, timeout=60)
        finally:
            stop.set()
            await task
    finally:
        await gate.send.remote()
        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
