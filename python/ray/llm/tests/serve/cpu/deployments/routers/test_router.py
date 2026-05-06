import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import openai
import orjson
import pytest

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.configs.openai_api_models import to_model_metadata
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


class _DirectRouterReplicaId:
    def __init__(self, unique_id: str, full_id: Optional[str] = None):
        self.unique_id = unique_id
        self._full_id = full_id or unique_id

    def to_full_id_str(self) -> str:
        return self._full_id


class _DirectRouterReplica:
    def __init__(self, unique_id: str, full_id: Optional[str] = None, port: int = 8000):
        self.replica_id = _DirectRouterReplicaId(unique_id, full_id)
        self.backend_http_endpoint = ("127.0.0.1", port)


def _new_direct_router():
    router = LLMRouter.__new__(LLMRouter)
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
    router = RouterDeployment.bind(
        llm_deployments={llm_config.model_id: server},
        model_cards={
            llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
        },
    )
    serve.run(router)

    client = openai.Client(base_url="http://localhost:8000/v1", api_key="foo")
    yield client

    serve.shutdown()


class TestDirectStreamingLLMRouter:
    @pytest.mark.asyncio
    async def test_route_endpoint_accepts_truncated_body_prefix(self):
        router = _new_direct_router()
        router._llm_configs = {"llm_model_id": MagicMock()}
        router._pick_replica = AsyncMock(
            return_value=("127.0.0.1", 9001, "DeploymentName#replica")
        )

        body = b'{"model":"llm_model_id","prompt":"' + (b"x" * 1024)
        messages = [{"type": "http.request", "body": body, "more_body": False}]
        sent = []

        async def receive():
            return messages.pop(0)

        async def send(message):
            sent.append(message)

        await router._handle_route_endpoint(
            {"headers": [(b"x-body-truncated", b"1058/90000")]},
            receive,
            send,
        )

        assert sent[0]["status"] == 200
        assert orjson.loads(sent[1]["body"]) == {
            "host": "127.0.0.1",
            "port": 9001,
            "replica_id": "DeploymentName#replica",
        }
        router._pick_replica.assert_awaited_once_with(
            "llm_model_id",
            request_body=body,
            body_truncated=True,
        )

    def test_round_robin_wraps_in_stable_replica_order(self):
        router = _new_direct_router()
        replica_a = _DirectRouterReplica("a")
        replica_b = _DirectRouterReplica("b")
        replica_c = _DirectRouterReplica("c")

        picks = [
            router._choose_round_robin_replica(
                [replica_c, replica_a, replica_b]
            ).replica_id.unique_id
            for _ in range(4)
        ]

        assert picks == ["a", "b", "c", "a"]

    def test_route_result_returns_full_replica_id_for_haproxy_mapping(self):
        router = _new_direct_router()
        replica = _DirectRouterReplica(
            "short-id",
            full_id="DeploymentName#short-id",
            port=9001,
        )

        assert router._replica_route_result(replica) == (
            "127.0.0.1",
            9001,
            "DeploymentName#short-id",
        )


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
        server.check_health = MagicMock()
        server.check_health.remote = AsyncMock()

        router = OpenAiIngress(
            llm_deployments={llm_config.model_id: server},
            model_cards={
                llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
            },
        )

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
        mock_handle.chat = MagicMock()
        mock_handle.chat.remote = mock_chat_generator
        # Make options() return the same mock so chat.remote is preserved
        mock_handle.options.return_value = mock_handle

        # Create router with mock handle
        router = OpenAiIngress(
            llm_deployments={llm_config.model_id: mock_handle},
            model_cards={
                llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
            },
        )

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
