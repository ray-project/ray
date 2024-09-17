import sys
from tempfile import NamedTemporaryFile

import pytest

from ray.dashboard.modules.job.common import JobSubmitRequest
from ray.dashboard.modules.job.utils import (
    file_tail_iterator,
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


class TestIterLine:
    @pytest.mark.asyncio
    async def test_invalid_type(self):
        with pytest.raises(TypeError, match="path must be a string"):
            await anext(file_tail_iterator(1))

    @pytest.mark.asyncio
    async def test_file_not_created(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None
        f = open(tmp, "w")
        f.write("hi\n")
        f.flush()
        assert await anext(it) is not None

    @pytest.mark.asyncio
    async def test_wait_for_newline(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")
        f.write("no_newline_yet")
        assert await anext(it) is None
        f.write("\n")
        f.flush()
        assert await anext(it) == ["no_newline_yet\n"]

    @pytest.mark.asyncio
    async def test_multiple_lines(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        num_lines = 10
        for i in range(num_lines):
            s = f"{i}\n"
            f.write(s)
            f.flush()
            assert await anext(it) == [s]

        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        # Write lines in batches of 10, check that we get them back in batches.
        for _ in range(100):
            num_lines = 10
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert await anext(it) == [f"{i}\n" for i in range(10)]

        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_max_line_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        # Write lines in batches of 50, check that we get them back in batches of 10.
        for _ in range(100):
            num_lines = 50
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert await anext(it) == [f"{i}\n" for i in range(10)]
            assert await anext(it) == [f"{i}\n" for i in range(10, 20)]
            assert await anext(it) == [f"{i}\n" for i in range(20, 30)]
            assert await anext(it) == [f"{i}\n" for i in range(30, 40)]
            assert await anext(it) == [f"{i}\n" for i in range(40, 50)]

        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_max_char_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert await anext(it) is None

        f = open(tmp, "w")

        # Write a single line that is 60k characters
        f.write(f"{'1234567890' * 6000}\n")
        # Write a 4 lines that are 10k characters each
        for _ in range(4):
            f.write(f"{'1234567890' * 500}\n")
        f.flush()

        # First line will come in a batch of its own
        assert await anext(it) == [f"{'1234567890' * 6000}\n"]
        # Other 4 lines will be batched together
        assert (
            await anext(it)
            == [
                f"{'1234567890' * 500}\n",
            ]
            * 4
        )
        assert await anext(it) is None

    @pytest.mark.asyncio
    async def test_delete_file(self):
        with NamedTemporaryFile() as tmp:
            it = file_tail_iterator(tmp.name)
            f = open(tmp.name, "w")

            assert await anext(it) is None

            f.write("hi\n")
            f.flush()

            assert await anext(it) == ["hi\n"]

        # Calls should continue returning None after file deleted.
        assert await anext(it) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
