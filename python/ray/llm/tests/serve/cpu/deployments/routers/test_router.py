import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import openai
import pytest

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


class _DirectRouterReplica:
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.backend_http_endpoint = ("127.0.0.1", 8000)


def _new_direct_router(*, optimistic_load: bool = False):
    router = LLMRouter.__new__(LLMRouter)
    router._di_load_cache = {}
    router._di_optimistic_load = optimistic_load
    router._di_poll_interval_s = 0.05
    router._di_routing_policy = "pow2"
    router._di_round_robin_counter = 0
    return router


@pytest.fixture(name="llm_config")
def create_llm_config(stream_batching_interval_ms: Optional[int] = None):

    if stream_batching_interval_ms is not None:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "stream_batching_interval_ms": stream_batching_interval_ms,
            },
        )
    else:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )


@pytest.fixture(name="client")
def create_oai_client(llm_config: LLMConfig):
    ServerDeployment = serve.deployment(LLMServer)

    ingress_options = OpenAiIngress.get_deployment_options(llm_configs=[llm_config])
    ingress_cls = make_fastapi_ingress(OpenAiIngress)
    RouterDeployment = serve.deployment(ingress_cls, **ingress_options)
    server = ServerDeployment.bind(llm_config, engine_cls=MockVLLMEngine)
    router = RouterDeployment.bind(llm_deployments=[server])
    serve.run(router)

    client = openai.Client(base_url="http://localhost:8000/v1", api_key="foo")
    yield client

    serve.shutdown()


class TestDirectStreamingLLMRouter:
    def test_missing_load_entries_start_at_zero(self):
        router = _new_direct_router()
        hot = _DirectRouterReplica("hot")
        missing = _DirectRouterReplica("missing")
        router._set_replica_load(hot, 5)

        assert router._get_replica_load(missing) == 0
        assert router._choose_best_loaded_replica([hot, missing]) is missing

    def test_optimistic_load_balances_repeated_identical_candidates(self):
        router = _new_direct_router(optimistic_load=True)
        replica_a = _DirectRouterReplica("a")
        replica_b = _DirectRouterReplica("b")

        picks = [
            router._choose_best_loaded_replica([replica_a, replica_b]).replica_id
            for _ in range(4)
        ]

        assert picks == ["a", "b", "a", "b"]
        assert router._di_load_cache == {"a": 2, "b": 2}

    def test_poll_reconciliation_overwrites_optimistic_load(self):
        router = _new_direct_router(optimistic_load=True)
        replica_a = _DirectRouterReplica("a")
        replica_b = _DirectRouterReplica("b")

        assert router._choose_best_loaded_replica([replica_a, replica_b]) is replica_a
        assert router._di_load_cache["a"] == 1

        router._set_replica_load(replica_a, 7)

        assert router._di_load_cache["a"] == 7
        assert router._choose_best_loaded_replica([replica_a, replica_b]) is replica_b

    def test_non_optimistic_mode_does_not_increment_load(self):
        router = _new_direct_router(optimistic_load=False)
        replica_a = _DirectRouterReplica("a")
        replica_b = _DirectRouterReplica("b")

        picks = [
            router._choose_best_loaded_replica([replica_a, replica_b]).replica_id
            for _ in range(3)
        ]

        assert picks == ["a", "a", "a"]
        assert router._di_load_cache == {}

    def test_round_robin_candidates_wrap_in_stable_replica_order(self):
        router = _new_direct_router()
        replica_a = _DirectRouterReplica("a")
        replica_b = _DirectRouterReplica("b")
        replica_c = _DirectRouterReplica("c")

        picks = [
            [
                r.replica_id
                for r in router._choose_round_robin_candidates(
                    [replica_c, replica_a, replica_b]
                )
            ]
            for _ in range(4)
        ]

        assert picks == [["a", "b"], ["b", "c"], ["c", "a"], ["a", "b"]]

    def test_round_robin_policy_balances_first_choice_without_load(self):
        router = _new_direct_router()
        router._di_routing_policy = "round_robin"
        replica_a = _DirectRouterReplica("a")
        replica_b = _DirectRouterReplica("b")
        replica_c = _DirectRouterReplica("c")

        picks = []
        for _ in range(4):
            candidates = router._choose_round_robin_candidates(
                [replica_c, replica_a, replica_b]
            )
            picks.append(router._choose_best_loaded_replica(candidates).replica_id)

        assert picks == ["a", "b", "c", "a"]

    def test_router_policy_normalization(self):
        assert LLMRouter._normalize_routing_policy("pow2") == "pow2"
        assert LLMRouter._normalize_routing_policy("power-of-two") == "pow2"
        assert LLMRouter._normalize_routing_policy("round-robin") == "round_robin"
        assert LLMRouter._normalize_routing_policy("rr") == "round_robin"


class TestOpenAiIngress:
    @pytest.mark.parametrize("stream_batching_interval_ms", [None, 0, 10000])
    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.asyncio
    async def test_chat(self, stream_batching_interval_ms, client, stream):
        """Tests chat streaming with different stream_batching_interval_ms values.

        0ms super fast batching (no batching)
        10000ms basically should be equivalent to non-streaming
        None is default, which is some fixed non-zero value.
        """

        # Generate 1000 chunks
        n_tokens = 1000

        response = client.chat.completions.create(
            model="llm_model_id",
            messages=[dict(role="user", content="Hello")],
            stream=stream,
            max_tokens=n_tokens,
        )

        if stream:
            text = ""
            role = None
            for chunk in response:
                if chunk.choices[0].delta.role is not None and role is None:
                    role = chunk.choices[0].delta.role
                if chunk.choices[0].delta.content:
                    text += chunk.choices[0].delta.content
        else:
            text = response.choices[0].message.content
            role = response.choices[0].message.role

        assert role == "assistant"
        assert text.strip() == " ".join([f"test_{i}" for i in range(n_tokens)])

    @pytest.mark.parametrize("stream_batching_interval_ms", [None, 0, 10000])
    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.asyncio
    async def test_completion(self, stream_batching_interval_ms, client, stream):
        """Tests text completions streaming with different stream_batching_interval_ms values."""

        # Generate tokens
        n_tokens = 1000

        response = client.completions.create(
            model="llm_model_id",
            prompt="Hello",
            stream=stream,
            max_tokens=n_tokens,
        )

        if stream:
            text = ""
            for chunk in response:
                text += chunk.choices[0].text
        else:
            text = response.choices[0].text

        # The mock engine produces "test_0 test_1 test_2 ..." pattern
        expected_text = " ".join([f"test_{i}" for i in range(n_tokens)])
        assert text.strip() == expected_text

    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.asyncio
    async def test_tool_call(self, client, stream):
        response = client.chat.completions.create(
            model="llm_model_id",
            messages=[
                {
                    "role": "user",
                    "content": "Can you tell me what the temperate will be in Dallas, in fahrenheit?",
                },
                {
                    "content": None,
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "RBS92VTjJ",
                            "function": {
                                "arguments": '{"city": "Dallas", "state": "TX", "unit": "fahrenheit"}',
                                "name": "get_current_weather",
                            },
                            "type": "function",
                        }
                    ],
                },
                {
                    "role": "tool",
                    "content": "The weather in Dallas, TX is 85 degrees fahrenheit. It is partly cloudly, with highs in the 90's.",
                    "tool_call_id": "n3OMUpydP",
                },
            ],
            stream=stream,
            max_tokens=200,
        )

        if stream:
            text = ""
            role = None
            for chunk in response:
                if chunk.choices[0].delta.role is not None and role is None:
                    role = chunk.choices[0].delta.role
                if chunk.choices[0].delta.content:
                    text += chunk.choices[0].delta.content
        else:
            text = response.choices[0].message.content
            role = response.choices[0].message.role

        assert text

    @pytest.mark.asyncio
    async def test_check_health(self, llm_config: LLMConfig):
        """Test health check functionality."""

        server = MagicMock()
        server.llm_config = MagicMock()
        server.llm_config.remote = AsyncMock(return_value=llm_config)
        server.check_health = MagicMock()
        server.check_health.remote = AsyncMock()

        router = OpenAiIngress(llm_deployments=[server])

        await router.check_health()

    @pytest.mark.asyncio
    async def test_raw_request_info_passed_to_deployment_handle(
        self, llm_config: LLMConfig
    ):
        """Test that raw_request_info is passed to the deployment handle."""
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
            ChatCompletionResponse,
        )
        from ray.llm._internal.serve.core.protocol import RawRequestInfo

        # Track if raw_request_info was received
        captured_raw_request_infos = []

        # Create a mock deployment handle that captures raw_request_info
        async def mock_chat_generator(request, raw_request_info):
            captured_raw_request_infos.append(raw_request_info)
            # Return a valid response
            yield ChatCompletionResponse(
                id="test_id",
                choices=[
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hello!"},
                        "finish_reason": "stop",
                    }
                ],
                model="llm_model_id",
                object="chat.completion",
                usage={
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            )

        mock_handle = MagicMock()
        mock_handle.llm_config = MagicMock()
        mock_handle.llm_config.remote = AsyncMock(return_value=llm_config)
        mock_handle.chat = MagicMock()
        mock_handle.chat.remote = mock_chat_generator
        # Make options() return the same mock so chat.remote is preserved
        mock_handle.options.return_value = mock_handle

        # Create router with mock handle
        router = OpenAiIngress(llm_deployments=[mock_handle])
        await router._init_completed.wait()

        # Create a mock FastAPI request
        from starlette.datastructures import Headers

        mock_request = MagicMock()
        mock_headers = {
            "content-type": "application/json",
            "x-ray-serve-llm-test-header": "router-raw-request-info",
        }
        mock_request.headers = Headers(mock_headers)

        # Make a request through the router
        request_body = ChatCompletionRequest(
            model="llm_model_id",
            messages=[{"role": "user", "content": "Hello"}],
            stream=False,
        )

        await router.chat(request_body, mock_request)

        # Verify that raw_request_info was passed to the deployment handle
        assert len(captured_raw_request_infos) == 1
        assert isinstance(captured_raw_request_infos[0], RawRequestInfo)
        assert captured_raw_request_infos[0].headers == mock_headers


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
