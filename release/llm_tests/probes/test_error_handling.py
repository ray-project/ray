import os

import openai
import pytest

from probes.models import is_completions_only_model, random_model
from probes.openai_client import create_async_client
from probes.query_utils import TextGenerationProbeQuerier
from probes.test_basic import get_prompt


# Only run token error tests on one model.
@pytest.mark.parametrize("model", [random_model])
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("chat", [True, False])
@pytest.mark.parametrize("long_query", [True, False])
@pytest.mark.asyncio
async def test_bad_token_handling(
    test_id: str,
    model: str,
    stream: bool,
    chat: bool,
    long_query: bool,
    max_tokens: int = 1,
):
    if is_completions_only_model(model) and chat:
        pytest.skip(f"Skipping chat test for completions only model {model}")

    async with create_async_client(
        api_key="RANDOM_API_KEY", base_url=os.environ["OPENAI_API_BASE"]
    ) as openai_async_client:
        deterministic_query = TextGenerationProbeQuerier(
            openai_async_client,
            default_configuration={"temperature": 0.0, "max_tokens": max_tokens},
        )
        params = get_prompt(test_id, chat, long_query)

        with pytest.raises(openai.AuthenticationError) as e:
            await deterministic_query.query(model, stream, chat=chat, **params)

        assert "401" in str(e.value), f"Expected 401 in exception, got {e.value}"
