import sys
from tempfile import NamedTemporaryFile

import pytest

from ray.dashboard.modules.job.common import JobSubmitRequest
from ray.dashboard.modules.job.utils import (
    parse_and_validate_request,
    redact_url_password,
    strip_keys_with_value_none,
)


# Polyfill anext() function for Python 3.9 compatibility
# May raise StopAsyncIteration.
async def anext_polyfill(iterator):
    return await iterator.__anext__()


# Use the built-in anext() for Python 3.10+, otherwise use our polyfilled function
if sys.version_info < (3, 10):
    anext = anext_polyfill


@pytest.fixture
def tmp():
    with NamedTemporaryFile() as f:
        yield f.name


def test_strip_keys_with_value_none():
    d = {"a": 1, "b": None, "c": 3}
    assert strip_keys_with_value_none(d) == {"a": 1, "c": 3}
    d = {"a": 1, "b": 2, "c": 3}
    assert strip_keys_with_value_none(d) == d
    d = {"a": 1, "b": None, "c": None}
    assert strip_keys_with_value_none(d) == {"a": 1}


def test_redact_url_password():
    url = "http://user:password@host:port"
    assert redact_url_password(url) == "http://user:<redacted>@host:port"
    url = "http://user:password@host:port?query=1"
    assert redact_url_password(url) == "http://user:<redacted>@host:port?query=1"
    url = "http://user:password@host:port?query=1&password=2"
    assert (
        redact_url_password(url)
        == "http://user:<redacted>@host:port?query=1&password=2"
    )
    url = "https://user:password@127.0.0.1:8080"
    assert redact_url_password(url) == "https://user:<redacted>@127.0.0.1:8080"
    url = "https://user:password@host:port?query=1"
    assert redact_url_password(url) == "https://user:<redacted>@host:port?query=1"
    url = "https://user:password@host:port?query=1&password=2"
    assert (
        redact_url_password(url)
        == "https://user:<redacted>@host:port?query=1&password=2"
    )


# Mock for aiohttp.web.Request, which should not be constructed directly.
class MockRequest:
    def __init__(self, **kwargs):
        self._json = kwargs

    async def json(self):
        return self._json


@pytest.mark.asyncio
async def test_mock_request():
    request = MockRequest(a=1, b=2)
    assert await request.json() == {"a": 1, "b": 2}
    request = MockRequest(a=1, b=None)
    assert await request.json() == {"a": 1, "b": None}


# async test
@pytest.mark.asyncio
class TestParseAndValidateRequest:
    async def test_basic(self):
        request = MockRequest(entrypoint="echo hi")
        expected = JobSubmitRequest(entrypoint="echo hi")
        assert await parse_and_validate_request(request, JobSubmitRequest) == expected

    async def test_forward_compatibility(self):
        request = MockRequest(entrypoint="echo hi", new_client_field=None)
        expected = JobSubmitRequest(entrypoint="echo hi")
        assert await parse_and_validate_request(request, JobSubmitRequest) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
