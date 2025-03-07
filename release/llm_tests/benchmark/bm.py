# There is a dead-lock issue that arises due to gevent's monkey-patching
# https://github.com/ipython/ipython/issues/11730
# Fix: We do this import first before anything else
# ruff: noqa: E402
import gevent.monkey

gevent.monkey.patch_all()


from .locust import LLMLoadTester
from .configs import LoadTestConfig

from typing import List, Optional
import openai


def run_bm(
    api_url: str,
    api_key: Optional[str] = None,
    concurrency: Optional[List[int]] = None,
    run_time: str = "1m",
    prompt_tokens: int = 512,
    max_tokens: int = 64,
    stream: bool = False,
    summary_file: str = "./results.csv",
):
    if api_key is None:
        api_key = "NONE"

    # Get model_id
    client = openai.Client(base_url=f"{api_url}/v1", api_key=api_key)
    models = client.models.list().model_dump()["data"]

    if len(models) != 1:
        raise ValueError("The service is expected to have only one model.")

    model_id = models[0]["id"]
    results = []
    for n_users in concurrency:
        config = LoadTestConfig(
            host=api_url,
            api_key=api_key,
            provider="openai",
            model=model_id,
            stream=stream,
            prompt_tokens=prompt_tokens,
            max_tokens=max_tokens,
            users=n_users,
            run_time=run_time,
            summary_file=summary_file,
        )
        tester = LLMLoadTester(config)
        results.append(tester.run())

    return results
