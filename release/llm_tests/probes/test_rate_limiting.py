import asyncio
import logging
import os
from typing import TypeVar

import openai
import pytest

from probes.models import model_loader
from probes.query_utils import TextGenerationProbeQuerier

T = TypeVar("T")

logger = logging.getLogger(__name__)

# Change this to match the token's max rate-limit
prober_max_concurrency = os.getenv("PROBER_MAX_CONCURRENCY", 5)
prober_excess_requests = os.getenv(
    "PROBER_EXCESS_REQUESTS", min(10, prober_max_concurrency)
)


async def long_query(model: str, test_id: str, openai_async_client, sleep_time):
    await asyncio.sleep(sleep_time)
    querier = TextGenerationProbeQuerier(
        openai_async_client,
        # Make sure we don't retry any errors
        retryable_error_types=(),
        # Expecting ~10ms/token at BEST
        default_configuration={"temperature": 0.0, "max_tokens": 400},
    )
    try:
        await querier.query(
            model,
            stream=False,
            chat=False,
            prompt=f"{test_id} Tell me the story of the three little pigs",
        )
    except openai.RateLimitError:
        logger.exception("Rate limit error")
        return "rate-limited"
    except Exception:
        logger.exception("Unknown error")
        return "failed"
    return "passed"


# Only test rate limiting against the 70b model [because it has long responses]
@pytest.mark.parametrize("model", model_loader.rate_limiting_model_ids())
@pytest.mark.asyncio
@pytest.mark.xfail(
    reason="can be flaky on fast models; deprecated and removing rate-limiting"
)
async def test_rate_limiting(model: str, test_id: str, openai_async_client):
    results = await asyncio.gather(
        *(
            # 50ms gap between each request start
            long_query(model, test_id, openai_async_client, i * 50 / 1000)
            for i in range(prober_max_concurrency + prober_excess_requests)
        ),
    )

    assert "failed" not in results
    assert "rate-limited" in results
