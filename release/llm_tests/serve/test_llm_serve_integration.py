import os
import openai
import pytest
import requests
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import ray
import ray.cloudpickle
from ray import serve
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import (
    LLMConfig,
    LLMServer,
    ModelLoadingConfig,
    build_openai_app,
    build_pd_openai_app,
)
from ray.serve.llm.request_router import PrefixCacheAffinityRouter
from vllm import AsyncEngineArgs

from vllm.v1.engine.async_llm import AsyncLLM
from vllm.v1.metrics.ray_wrappers import RayPrometheusStatLogger
from vllm.sampling_params import SamplingParams
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus
from ray.serve._private.test_utils import wait_for_haproxy_routing_to_replica

from utils import shutdown_serve_and_wait_for_controller

S3_ARTIFACT_ASSETS_URL = (
    "https://air-example-data.s3.amazonaws.com/rayllm-ossci/assets/"
)

# Pooling models (classify/reward) are only served through vLLM's native ASGI
# app, which is used when direct streaming is enabled. The default OpenAiIngress
# path does not expose /classify or /pooling, so these tests only run when
# RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1.
direct_streaming_only = pytest.mark.skipif(
    os.environ.get("RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING", "0") != "1",
    reason="Pooling/classify endpoints are only served in direct-streaming mode "
    "(RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1).",
)


@pytest.mark.asyncio(scope="function")
async def test_engine_metrics():
    """
    Test that the stat logger can be created successfully.
    Keeping this test small to focus on instantiating the
    derived class correctly.
    """

    engine_args = AsyncEngineArgs(
        model="Qwen/Qwen2.5-0.5B-Instruct",
        dtype="auto",
        disable_log_stats=False,
        enforce_eager=True,
    )

    engine = AsyncLLM.from_engine_args(
        engine_args, stat_loggers=[RayPrometheusStatLogger]
    )

    for i, prompt in enumerate(["What is the capital of France?", "What is 2+2?"]):
        results = engine.generate(
            request_id=f"request-id-{i}",
            prompt=prompt,
            sampling_params=SamplingParams(max_tokens=10),
        )

        async for _ in results:
            pass


@pytest.mark.asyncio(scope="function")
async def test_engine_metrics_with_lora():
    """
    Test that the stat logger can be created successfully with LoRA configuration.
    This test validates LoRA-enabled engine initialization and basic functionality.
    """

    engine_args = AsyncEngineArgs(
        model="Qwen/Qwen2.5-0.5B-Instruct",  # Using smaller model for testing
        disable_log_stats=False,
        enforce_eager=True,
        enable_prefix_caching=True,
        max_model_len=512,
        max_lora_rank=64,
        enable_lora=True,
        max_loras=3,
        max_cpu_loras=5,
    )

    engine = AsyncLLM.from_engine_args(
        engine_args, stat_loggers=[RayPrometheusStatLogger]
    )

    for i, prompt in enumerate(["What is the capital of France?", "What is 2+2?"]):
        results = engine.generate(
            request_id=f"lora-request-id-{i}",
            prompt=prompt,
            sampling_params=SamplingParams(max_tokens=10),
        )

        async for _ in results:
            pass


@pytest.mark.asyncio(scope="function")
async def test_engine_metrics_with_spec_decode():
    """
    Test that the stat logger can be created successfully with speculative decoding configuration.
    This test validates speculative decoding engine initialization and basic functionality.
    """

    engine_args = AsyncEngineArgs(
        model="Qwen/Qwen2.5-0.5B-Instruct",
        dtype="auto",
        disable_log_stats=False,
        enforce_eager=True,
        trust_remote_code=True,
        enable_prefix_caching=True,
        max_model_len=256,
        speculative_config={
            "method": "ngram",
            "num_speculative_tokens": 5,
            "prompt_lookup_max": 4,
        },
    )

    engine = AsyncLLM.from_engine_args(
        engine_args, stat_loggers=[RayPrometheusStatLogger]
    )

    for i, prompt in enumerate(["What is the capital of France?", "What is 2+2?"]):
        results = engine.generate(
            request_id=f"spec-request-id-{i}",
            prompt=prompt,
            sampling_params=SamplingParams(max_tokens=10),
        )

        async for _ in results:
            pass


@direct_streaming_only
def test_lora_requests():
    """Serve cold and cached LoRA requests through the public HTTP endpoints."""
    model_id = "llama-test"
    adapter_id = f"{model_id}:llama-3.2-216M-lora-dummy"
    config = LLMConfig(
        model_loading_config=dict(
            model_id=model_id,
            model_source=dict(bucket_uri="s3://air-example-data/llama-3.2-216M-dummy"),
        ),
        deployment_config=dict(num_replicas=1),
        engine_kwargs=dict(
            enable_lora=True,
            max_lora_rank=16,
            max_model_len=256,
            gpu_memory_utilization=0.4,
            enforce_eager=True,
        ),
        lora_config=dict(dynamic_lora_loading_path="s3://air-example-data"),
    )
    try:
        serve.run(build_openai_app({"llm_configs": [config]}), blocking=False)
        wait_for_condition(is_default_app_running, timeout=300)

        with openai.OpenAI(
            base_url="http://localhost:8000/v1",
            api_key="unused",
            timeout=60,
            max_retries=0,
        ) as client:
            # Load the adapter through the public completions endpoint.
            response = client.completions.create(
                model=adapter_id,
                prompt="Hello",
                max_tokens=4,
                temperature=0,
                stream=False,
                extra_body={"ignore_eos": True},
            )
            assert response.model == adapter_id
            assert response.usage.completion_tokens == 4
            assert response.choices[0].finish_reason == "length"

            # Confirm the native API exposes the loaded adapter.
            assert adapter_id in {model.id for model in client.models.list().data}

            # Exercise base and cached adapter requests across both APIs.
            for create, prompt in [
                (client.completions.create, {"prompt": "Hello"}),
                (
                    client.chat.completions.create,
                    {"messages": [{"role": "user", "content": "Hello"}]},
                ),
            ]:
                for stream in (False, True):
                    for requested_model in (model_id, adapter_id):
                        response = create(
                            model=requested_model,
                            **prompt,
                            max_tokens=4,
                            temperature=0,
                            stream=stream,
                            extra_body={"ignore_eos": True},
                        )
                        if stream:
                            chunks = list(response)
                            assert chunks
                            assert {chunk.model for chunk in chunks} == {
                                requested_model
                            }
                            assert chunks[-1].choices[0].finish_reason == "length"
                        else:
                            assert response.model == requested_model
                            assert response.usage.completion_tokens == 4
                            assert response.choices[0].finish_reason == "length"
    finally:
        shutdown_serve_and_wait_for_controller()


@direct_streaming_only
def test_pd_lora_requests():
    """Serve cold and cached LoRA requests through direct-streaming P/D."""
    model_id = "llama-test"
    adapter_id = f"{model_id}:llama-3.2-216M-lora-dummy"
    config = LLMConfig(
        model_loading_config=dict(
            model_id=model_id,
            model_source=dict(bucket_uri="s3://air-example-data/llama-3.2-216M-dummy"),
        ),
        deployment_config=dict(num_replicas=1),
        engine_kwargs=dict(
            enable_lora=True,
            max_lora_rank=16,
            max_model_len=256,
            gpu_memory_utilization=0.4,
            enforce_eager=True,
            kv_transfer_config=dict(kv_connector="NixlConnector", kv_role="kv_both"),
        ),
        lora_config=dict(dynamic_lora_loading_path="s3://air-example-data"),
    )
    # P/D assigns decode a separate NIXL port.
    decode_config = config.model_copy(deep=True)
    try:
        serve.run(
            build_pd_openai_app(
                {
                    "prefill_config": config,
                    "decode_config": decode_config,
                }
            ),
            blocking=False,
        )
        wait_for_condition(is_default_app_running, timeout=300)
        wait_for_haproxy_routing_to_replica()

        with openai.OpenAI(
            base_url="http://localhost:8000/v1",
            api_key="unused",
            timeout=60,
            max_retries=0,
        ) as client:
            # Resolve the adapter through prefill and decode.
            response = client.completions.create(
                model=adapter_id,
                prompt="Hello",
                max_tokens=4,
                temperature=0,
                stream=False,
                extra_body={"ignore_eos": True},
            )
            assert response.model == adapter_id
            assert response.usage.completion_tokens == 4
            assert response.choices[0].finish_reason == "length"

            # A decode-only request can also return valid LoRA output. Require
            # the cold request to have resolved the adapter on remote prefill.
            controller = serve.context._get_global_client()._controller

            def prefill_has_adapter():
                deployments = ray.get(controller._all_running_replicas.remote())
                prefill_replicas = next(
                    replicas
                    for deployment, replicas in deployments.items()
                    if deployment.name.startswith("Prefill:")
                )
                assert len(prefill_replicas) == 1
                assert adapter_id in prefill_replicas[0].multiplexed_model_ids
                return True

            wait_for_condition(prefill_has_adapter)

            # Exercise a cached adapter in a streamed request.
            chunks = list(
                client.completions.create(
                    model=adapter_id,
                    prompt="Hello",
                    max_tokens=4,
                    temperature=0,
                    stream=True,
                    extra_body={"ignore_eos": True},
                )
            )
            assert chunks
            assert {chunk.model for chunk in chunks} == {adapter_id}
            assert chunks[-1].choices[0].finish_reason == "length"
    finally:
        shutdown_serve_and_wait_for_controller()


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


@pytest.mark.parametrize("model_name", ["deepseek-ai/DeepSeek-V2-Lite"])
def test_deepseek_model(model_name):
    """
    Test that the deepseek model can be loaded successfully.
    """
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model_name,
        ),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=2,
            gpu_memory_utilization=0.92,
            dtype="auto",
            max_num_seqs=40,
            max_model_len=8192,
            enable_chunked_prefill=True,
            enable_prefix_caching=True,
            enforce_eager=True,
            trust_remote_code=True,
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)
    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


@pytest.mark.parametrize("model_name", ["openai/whisper-small"])
def test_transcription_model(model_name):
    """
    Test that the transcription models can be loaded successfully.
    """
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model_name,
            model_source=model_name,
        ),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=4),
        ),
        engine_kwargs=dict(
            trust_remote_code=True,
            gpu_memory_utilization=0.9,
            enable_prefix_caching=True,
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)
    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


@pytest.mark.parametrize("model_name", ["BAAI/bge-small-en-v1.5"])
def test_embedding_model(model_name):
    """
    Test that embedding models can be loaded and serve embedding requests.
    """
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model_name,
        ),
        deployment_config=dict(
            num_replicas=1,
        ),
        engine_kwargs=dict(
            enforce_eager=True,
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)

    response = requests.post(
        "http://localhost:8000/v1/embeddings",
        json={
            "model": model_name,
            "input": "Hello, world!",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data
    assert len(data["data"]) > 0
    embedding = data["data"][0]["embedding"]
    assert isinstance(embedding, list)
    assert len(embedding) > 0
    assert all(isinstance(x, float) for x in embedding)

    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


@pytest.mark.parametrize("model_name", ["BAAI/bge-small-en-v1.5"])
def test_score_model(model_name):
    """
    Test that embedding models can serve score requests.
    """
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model_name,
        ),
        deployment_config=dict(
            num_replicas=1,
        ),
        engine_kwargs=dict(
            enforce_eager=True,
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)

    response = requests.post(
        "http://localhost:8000/v1/score",
        json={
            "model": model_name,
            "text_1": "What is the capital of France?",
            "text_2": ["Paris is the capital of France.", "Berlin is in Germany."],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 2
    for item in data["data"]:
        assert "score" in item
        assert isinstance(item["score"], float)

    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


def _validate_classify(item):
    assert isinstance(item["probs"], list)
    assert len(item["probs"]) > 0
    assert item["num_classes"] == len(item["probs"])


def _validate_pooling(item):
    # Reward models emit a per-token pooling vector; ensure it is non-empty.
    assert len(item["data"]) > 0


@direct_streaming_only
@pytest.mark.parametrize(
    "model_name,engine_kwargs,endpoint,validate_item",
    [
        pytest.param(
            "Qwen/Qwen3-Reranker-0.6B",
            dict(
                hf_overrides={
                    "architectures": ["Qwen3ForSequenceClassification"],
                    "classifier_from_token": ["no", "yes"],
                    "is_original_qwen3_reranker": True,
                },
            ),
            "/classify",
            _validate_classify,
            id="classify",
        ),
        pytest.param(
            "internlm/internlm2-1_8b-reward",
            dict(trust_remote_code=True),
            "/pooling",
            _validate_pooling,
            id="pooling",
        ),
    ],
)
def test_pooling_model(model_name, engine_kwargs, endpoint, validate_item):
    """Pooling models (classify/reward) are served via vLLM's native /classify
    and /pooling endpoints, which are only mounted in direct-streaming mode."""
    llm_config = LLMConfig(
        model_loading_config=dict(model_id=model_name),
        deployment_config=dict(num_replicas=1),
        engine_kwargs=dict(enforce_eager=True, max_model_len=512, **engine_kwargs),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)

    response = requests.post(
        f"http://localhost:8000{endpoint}",
        json={"model": model_name, "input": "The chef prepared a delicious meal."},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["object"] == "list"
    assert len(data["data"]) == 1
    validate_item(data["data"][0])

    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


@pytest.fixture
def remote_model_app(request):
    """
    Fixture that creates an app with a remote code model for testing.

    The remote_code parameter controls whether trust_remote_code is enabled.
    This helps avoid regressions for pickling issues for custom huggingface configs,
    since this custom code needs to be registered and imported across processes and workers.
    """
    remote_code = request.param

    base_config = {
        "model_loading_config": dict(
            model_id="hmellor/Ilama-3.2-1B",
        ),
        "deployment_config": dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        "engine_kwargs": dict(
            trust_remote_code=remote_code,
        ),
    }

    llm_config = LLMConfig(**base_config)
    app = build_openai_app({"llm_configs": [llm_config]})

    yield app

    # Cleanup
    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


class TestRemoteCode:
    """Tests for remote code model loading behavior."""

    @pytest.mark.parametrize("remote_model_app", [False], indirect=True)
    def test_remote_code_failure(self, remote_model_app):
        """
        Tests that a remote code model fails to load when trust_remote_code=False.

        If it loads successfully without remote code, the fixture should be changed to one that does require remote code.
        """
        app = remote_model_app
        with pytest.raises(RuntimeError, match="Deploying application default failed"):
            serve.run(app, blocking=False)

        def check_for_failed_deployment():
            """Check if the application deployment has failed."""
            try:
                default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
                return default_app.status == ApplicationStatus.DEPLOY_FAILED
            except (KeyError, AttributeError):
                return False

        # Wait for either failure or success (timeout after 2 minutes)
        try:
            wait_for_condition(check_for_failed_deployment, timeout=120)
        except TimeoutError:
            # If deployment didn't fail, check if it succeeded
            if is_default_app_running():
                pytest.fail(
                    "App deployed successfully without trust_remote_code=True. "
                    "This model may not actually require remote code. "
                    "Consider using a different model that requires remote code."
                )
            else:
                pytest.fail("Deployment did not fail or succeed within timeout period.")

    @pytest.mark.parametrize("remote_model_app", [True], indirect=True)
    def test_remote_code_success(self, remote_model_app):
        """
        Tests that a remote code model succeeds to load when trust_remote_code=True.
        """
        app = remote_model_app

        serve.run(app, blocking=False)

        # Wait for the application to be running (timeout after 5 minutes)
        wait_for_condition(is_default_app_running, timeout=300)


def test_nested_engine_kwargs_structured_outputs():
    """Regression test for https://github.com/ray-project/ray/pull/60380"""
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id="Qwen/Qwen2.5-0.5B-Instruct",
        ),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        engine_kwargs=dict(
            enforce_eager=True,
            max_model_len=512,
            structured_outputs_config={
                "backend": "xgrammar",
            },
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)
    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


def test_chat_completion_with_default_chat_template_kwargs():
    """Ensure mapping-valued vLLM frontend arguments remain dictionaries."""
    model_name = "Qwen/Qwen3-0.6B"
    llm_config = LLMConfig(
        model_loading_config=dict(model_id=model_name),
        deployment_config=dict(num_replicas=1),
        engine_kwargs=dict(
            enforce_eager=True,
            max_model_len=512,
            default_chat_template_kwargs={
                "enable_thinking": False,
            },
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=180)

    response = requests.post(
        "http://localhost:8000/v1/chat/completions",
        json={
            "model": model_name,
            "messages": [{"role": "user", "content": "Reply with hello."}],
            "max_tokens": 8,
            "temperature": 0,
        },
        timeout=120,
    )
    assert response.status_code == 200, response.text
    assert response.json()["choices"][0]["message"]["content"]

    shutdown_serve_and_wait_for_controller()
    time.sleep(1)


class ReplicaIdLLMServer(LLMServer):
    """vLLM responses carry no replica identity, so stamp the serving
    replica's id on responses."""

    async def __serve_build_asgi_app__(self):
        app = await super().__serve_build_asgi_app__()
        replica_id = serve.get_replica_context().replica_id.unique_id

        @app.middleware("http")
        async def add_replica_id_header(request, call_next):
            response = await call_next(request)
            response.headers["x-test-replica-id"] = replica_id
            return response

        return app


def _post_chat(text: str) -> str:
    """Send one chat request through HAProxy and return the serving replica's id."""
    response = requests.post(
        "http://localhost:8000/v1/chat/completions",
        json={
            "model": "qwen3-0.6b",
            "messages": [{"role": "user", "content": text}],
            # Routing is what these tests exercise; generate barely anything.
            "max_tokens": 1,
        },
        timeout=60,
    )
    assert response.status_code == 200, response.text
    return response.headers["x-test-replica-id"]


def _unique_prompt() -> str:
    """A prompt that shares no prefix with others, so routing must use the
    smallest-tenant tie-break instead of a prefix match."""
    return f"{uuid.uuid4().hex} unrelated request body."


def _prefix_group_text(group_id: str) -> str:
    """Fixed text per group, so a repeat matches its own group with rate 1.0."""
    return (
        f"{group_id} shares this long common preamble across every repeat "
        "in its conversation, establishing context. "
    ) * 4


@direct_streaming_only
class TestPrefixAffinityDirectStreaming:
    """Regression tests for https://github.com/ray-project/ray/pull/66489: on
    the pick-only path, ``on_request_routed`` never ran, so the
    PrefixCacheAffinityRouter prefix tree stayed empty and all traffic went
    to one replica."""

    @pytest.fixture(scope="class", autouse=True)
    def serve_app(self):
        """One deployment for all three prefix-affinity tests: four
        direct-streaming replicas routed by PrefixCacheAffinityRouter."""
        llm_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen3-0.6b",
                model_source="Qwen/Qwen3-0.6B",
            ),
            deployment_config=dict(
                autoscaling_config=dict(min_replicas=4, max_replicas=4),
                request_router_config=RequestRouterConfig(
                    request_router_class=PrefixCacheAffinityRouter
                ),
            ),
            engine_kwargs=dict(
                max_model_len=2048,
                enforce_eager=True,
                gpu_memory_utilization=0.4,
            ),
            placement_group_config={"bundles": [{"GPU": 1}]},
            server_cls=ReplicaIdLLMServer,
        )
        # Serve replicas can't import this test module, so ship
        # ReplicaIdLLMServer to them by value.
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        serve.run(build_openai_app({"llm_configs": [llm_config]}), blocking=False)
        wait_for_condition(is_default_app_running, timeout=300)
        # HAProxy's backends and LLMRouter's replica set update asynchronously
        # after the app reports RUNNING.
        wait_for_haproxy_routing_to_replica()
        yield
        shutdown_serve_and_wait_for_controller()
        time.sleep(1)

    def test_new_prompts_spread_across_replicas(self):
        """Unrelated prompts must spread across every replica. Before
        https://github.com/ray-project/ray/pull/66489, ``on_request_routed``
        never ran on the pick-only path, so the prefix tree stayed empty forever
        always produced the same replica ordering."""
        num_prompts = 60
        counts = Counter(_post_chat(_unique_prompt()) for _ in range(num_prompts))
        assert len(counts) == 4, (
            f"Expected all 4 replicas to receive at least one of "
            f"{num_prompts} unrelated prompts, got {dict(counts)}"
        )
        busiest, busiest_count = counts.most_common(1)[0]
        # The bug's signature was 100% of requests on one replica.
        assert busiest_count <= num_prompts * 0.5, (
            f"Replica {busiest} got {busiest_count}/{num_prompts} requests, "
            f"the all-to-one-replica regression: {dict(counts)}"
        )

    def test_new_prompts_spread_under_load(self):
        """The sequential spread test lets each pick see the previous pick's
        tree insert. With concurrent requests in flight, picks race those
        inserts, and spreading must still hold."""
        # The production incident saw 100% of requests on one replica at 16
        # concurrent requests; this is its 1,000 requests scaled down.
        num_prompts, concurrency = 200, 16
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            served = list(
                pool.map(lambda _: _post_chat(_unique_prompt()), range(num_prompts))
            )

        counts = Counter(served)
        assert len(counts) == 4, f"Replicas serving under load: {dict(counts)}"
        busiest, busiest_count = counts.most_common(1)[0]
        assert busiest_count <= num_prompts * 0.5, (
            f"Replica {busiest} got {busiest_count}/{num_prompts} requests "
            f"under load: {dict(counts)}"
        )

    def test_repeated_prompt_pins_to_one_replica(self):
        """Repeats of a prompt must pin to one replica. Without the pick-only
        ``on_request_routed`` call the tree stays empty and the router falls
        back to power-of-two picks, which spread but never pin."""
        num_groups, repeats = 4, 5
        groups = [f"group-{uuid.uuid4().hex}" for _ in range(num_groups)]
        group_replicas = {g: [] for g in groups}

        # Interleave groups (round-robin) rather than finishing one group
        # before starting the next, so affinity is proven against
        # concurrent unrelated inserts from sibling groups, not just a
        # quiet tree.
        for _ in range(repeats):
            for group_id in groups:
                group_replicas[group_id].append(
                    _post_chat(_prefix_group_text(group_id))
                )

        for group_id, replicas in group_replicas.items():
            assert len(set(replicas)) == 1, (
                f"{group_id}'s {repeats} repeats landed on "
                f"{len(set(replicas))} different replicas, not one: {replicas}"
            )


@pytest.mark.timeout(600)
@pytest.mark.parametrize(
    "model, modality",
    [
        ("Qwen/Qwen3-VL-2B-Instruct", "image"),
        ("Qwen/Qwen3-ASR-0.6B", "audio"),
    ],
    ids=["image", "audio"],
)
def test_pd_multimodal_with_tokenize_once(model, modality):
    """Multimodal P/D chat must work with pd_tokenize_once enabled."""
    if modality == "image":
        content = [
            {"type": "text", "text": "Describe the image briefly."},
            {
                "type": "image_url",
                "image_url": {"url": S3_ARTIFACT_ASSETS_URL + "cherry_blossom.jpg"},
            },
        ]
        limits = {"image": 1, "video": 0}
    else:
        content = [
            {"type": "text", "text": "Transcribe the audio."},
            {
                "type": "audio_url",
                "audio_url": {"url": S3_ARTIFACT_ASSETS_URL + "winning_call.ogg"},
            },
        ]
        limits = {"audio": 1}

    prefill_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=model, model_source=model),
        deployment_config={"num_replicas": 1},
        engine_kwargs={
            "tensor_parallel_size": 1,
            "max_model_len": 2048,
            "max_num_batched_tokens": 2048,
            "max_num_seqs": 2,
            "gpu_memory_utilization": 0.8,
            "enforce_eager": True,
            "limit_mm_per_prompt": limits,
            "mm_processor_kwargs": {"max_pixels": 224 * 224}
            if modality == "image"
            else {},
            # vLLM 0.29 indexes the submodel's otherwise empty architectures.
            # Remove after upgrading past vllm-project/vllm#58212.
            "hf_overrides": {"text_config": {"architectures": ["Qwen3ForCausalLM"]}}
            if modality == "image"
            else {},
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            },
        },
        experimental_configs={"NIXL_SIDE_CHANNEL_PORT_BASE": 15000},
    )
    decode_config = prefill_config.model_copy(deep=True)
    decode_config.experimental_configs = {
        "NIXL_SIDE_CHANNEL_PORT_BASE": 16000,
        "pd_tokenize_once": True,
    }
    app = build_pd_openai_app(
        {
            "prefill_config": prefill_config,
            "decode_config": decode_config,
        }
    )
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)

    with openai.OpenAI(
        base_url="http://localhost:8000/v1", api_key="test", timeout=120, max_retries=0
    ) as client:
        for stream in (False, True):
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": content}],
                max_tokens=32,
                temperature=0,
                stream=stream,
                stream_options={"include_usage": True} if stream else None,
                # Even if the caller asks prefill to return IDs, decode must
                # render the media instead of taking vLLM's token-only path.
                extra_body={"return_token_ids": True},
            )
            if stream:
                chunks = list(response)
                text = "".join(
                    choice.delta.content or ""
                    for chunk in chunks
                    for choice in chunk.choices
                )
                finish_reasons = [
                    choice.finish_reason
                    for chunk in chunks
                    for choice in chunk.choices
                    if choice.finish_reason
                ]
                usage = chunks[-1].usage
            else:
                text = response.choices[0].message.content
                finish_reasons = [response.choices[0].finish_reason]
                usage = response.usage
            assert text and text.strip()
            assert finish_reasons == ["stop"] or finish_reasons == ["length"]
            assert usage.prompt_tokens > 0
            assert usage.completion_tokens > 0

    shutdown_serve_and_wait_for_controller()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
