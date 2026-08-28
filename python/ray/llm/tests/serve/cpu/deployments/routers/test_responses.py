import sys

import pytest

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ResponsesRequest,
    ResponsesResponse,
)


def test_responses_request_has_request_id():
    req = ResponsesRequest(model="m", input="hello")
    assert isinstance(req.request_id, str)
    assert req.request_id


def test_responses_response_importable():
    assert ResponsesResponse.__name__ == "ResponsesResponse"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
