# flake8: noqa
import sys

# The snippets below import from `summarizer`, the file name the docs show for
# the application code. Alias that name to this module so the file runs as-is.
sys.modules["summarizer"] = sys.modules[__name__]

# __testing_app_start__
# Filename: summarizer.py
from fastapi import FastAPI

from ray import serve


class Summarizer:
    """Business logic with no dependency on Ray Serve."""

    def __init__(self, max_words: int):
        self.max_words = max_words

    def summarize(self, text: str) -> str:
        return " ".join(text.split()[: self.max_words])


fastapi_app = FastAPI()


@serve.deployment
@serve.ingress(fastapi_app)
class SummarizerDeployment(Summarizer):
    """Serving layer that exposes the business logic over HTTP."""

    @fastapi_app.get("/summarize")
    async def summarize_route(self, text: str) -> str:
        return self.summarize(text)


app = SummarizerDeployment.bind(max_words=3)
# __testing_app_end__


# __testing_func_or_class_start__
@serve.deployment
class Doubler:
    def double(self, text: str) -> str:
        return f"{text} {text}"


def test_doubler_class():
    # Get the class that the deployment wraps and instantiate it without Ray.
    doubler_class = Doubler.func_or_class
    doubler = doubler_class()
    assert doubler.double("hi") == "hi hi"
# __testing_func_or_class_end__


# __testing_unit_start__
# Filename: test_summarizer.py
from summarizer import Summarizer


def test_summarize_truncates_text():
    summarizer = Summarizer(max_words=2)
    assert summarizer.summarize("one two three four") == "one two"
# __testing_unit_end__


# __testing_integration_start__
# Filename: test_summarizer_integration.py
import pytest
import requests

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle, DeploymentResponse

from summarizer import app


@pytest.fixture(scope="module")
def handle():
    handle: DeploymentHandle = serve.run(app)
    yield handle
    serve.shutdown()
    ray.shutdown()


def test_summarize_with_handle(handle: DeploymentHandle):
    response: DeploymentResponse = handle.summarize_route.remote(
        "one two three four"
    )
    assert response.result() == "one two three"


def test_summarize_with_http(handle: DeploymentHandle):
    response = requests.get(
        "http://localhost:8000/summarize", params={"text": "one two three four"}
    )
    response.raise_for_status()
    assert response.json() == "one two three"
# __testing_integration_end__


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
