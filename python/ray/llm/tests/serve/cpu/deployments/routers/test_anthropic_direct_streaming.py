import sys
import time

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
    MOCK_ANTHROPIC_STREAM_CHUNK_DELAY_S,
    MOCK_ANTHROPIC_STREAM_CHUNKS,
)

MODEL_ID = "test-model"


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

    def test_anthropic_paths_pass_through_haproxy(self, base_url):
        """Engine-native Anthropic paths reach the replica through HAProxy.

        The OpenAPI test owns the contract that real vLLM registers these
        routes. This test only proves the paths pass through and that
        ``/v1/messages`` is not buffered before the response completes.
        """
        t_first = None
        lines = []
        with httpx.stream(
            "POST",
            f"{base_url}/v1/messages",
            json={
                "model": MODEL_ID,
                "max_tokens": 32,
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
            timeout=30,
        ) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            for line in resp.iter_lines():
                if not line:
                    continue
                if t_first is None:
                    t_first = time.monotonic()
                lines.append(line)
        t_done = time.monotonic()

        assert t_first is not None
        # The mock delays each chunk. A buffered hop delivers them together,
        # so the gap between the first line and stream completion collapses.
        assert t_done - t_first >= MOCK_ANTHROPIC_STREAM_CHUNK_DELAY_S / 2
        body = "\n".join(lines)
        for chunk in MOCK_ANTHROPIC_STREAM_CHUNKS:
            assert chunk in body

        count_resp = httpx.post(
            f"{base_url}/v1/messages/count_tokens",
            json={
                "model": MODEL_ID,
                "messages": [{"role": "user", "content": "hi"}],
            },
            timeout=30,
        )
        assert count_resp.status_code == 200, count_resp.text
        assert count_resp.json()["input_tokens"] == MOCK_ANTHROPIC_INPUT_TOKENS


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
