"""Tests that a request rejected by vLLM's request models is served as a 4xx."""

import sys

import openai
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
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


@pytest.fixture(name="client")
def create_oai_client():
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="llm_model_id"),
    )
    ingress_options = OpenAiIngress.get_deployment_options(llm_configs=[llm_config])
    RouterDeployment = serve.deployment(
        make_fastapi_ingress(OpenAiIngress), **ingress_options
    )
    server = serve.deployment(LLMServer).bind(llm_config, engine_cls=MockVLLMEngine)
    router = RouterDeployment.bind(
        llm_deployments={llm_config.model_id: server},
        model_cards={
            llm_config.model_id: to_model_metadata(llm_config.model_id, llm_config)
        },
    )
    serve.run(router)

    yield openai.Client(base_url="http://localhost:8000/v1", api_key="foo")

    serve.shutdown()


def test_invalid_top_logprobs_is_400(client):
    """``top_logprobs=-2`` is rejected by a ``mode="before"`` validator. The error
    is a ``VLLMError``, not a ``ValueError``, so pydantic does not wrap it and the
    ingress must map it explicitly or answer 500."""
    with pytest.raises(openai.BadRequestError) as exc_info:
        client.chat.completions.create(
            model="llm_model_id",
            messages=[dict(role="user", content="Hello")],
            top_logprobs=-2,
        )

    # A 500 would flatten the body to "Internal Server Error".
    assert "top_logprobs" in str(exc_info.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
