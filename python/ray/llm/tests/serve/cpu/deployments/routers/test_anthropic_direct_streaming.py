import sys

import httpx
import pytest

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    requires_direct_streaming,
    run_app_through_haproxy,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import (
    MOCK_ANTHROPIC_INPUT_TOKENS,
    MOCK_ANTHROPIC_TEXT,
)

MODEL_ID = "test-model"


def _messages_body(*, stream: bool) -> dict:
    return {
        "model": MODEL_ID,
        "max_tokens": 32,
        "messages": [{"role": "user", "content": "hi"}],
        "stream": stream,
    }


def _sse_event_types(body: str) -> list[str]:
    return [
        line[len("event: ") :]
        for line in body.splitlines()
        if line.startswith("event: ")
    ]


@requires_direct_streaming
class TestAnthropicDirectStreaming:
    """Anthropic Messages API over the full direct-streaming path.

    A request flows through HAProxy and the LLMRouter ``/internal/route``
    decision to a backend replica that serves the engine-native ASGI app.
    """

    @pytest.fixture(name="llm_config")
    def _llm_config(self):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id=MODEL_ID))

    @pytest.fixture(name="base_url")
    def run_direct_streaming_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        llm_config = llm_config_with_mock_engine
        llm_config.deployment_config = {
            "num_replicas": 1,
            "ray_actor_options": {"num_cpus": 0.1},
        }
        yield run_app_through_haproxy(
            build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        )

    def test_messages_non_streaming(self, base_url):
        resp = httpx.post(
            f"{base_url}/v1/messages",
            json=_messages_body(stream=False),
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert payload["type"] == "message"
        assert payload["role"] == "assistant"
        assert payload["model"] == MODEL_ID
        assert payload["content"][0]["text"] == MOCK_ANTHROPIC_TEXT

    def test_messages_streaming(self, base_url):
        resp = httpx.post(
            f"{base_url}/v1/messages",
            json=_messages_body(stream=True),
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        assert resp.headers["content-type"].startswith("text/event-stream")
        events = _sse_event_types(resp.text)
        assert events.index("message_start") < events.index("content_block_delta")
        assert events.index("content_block_delta") < events.index("message_stop")
        assert MOCK_ANTHROPIC_TEXT in resp.text

    def test_count_tokens(self, base_url):
        resp = httpx.post(
            f"{base_url}/v1/messages/count_tokens",
            json={
                "model": MODEL_ID,
                "messages": [{"role": "user", "content": "hi"}],
            },
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["input_tokens"] == MOCK_ANTHROPIC_INPUT_TOKENS


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
