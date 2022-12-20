from tempfile import NamedTemporaryFile
import sys

import pytest

from ray.dashboard.modules.job.common import JobSubmitRequest
from ray.dashboard.modules.job.utils import (
    file_tail_iterator,
    strip_keys_with_value_none,
    parse_and_validate_request,
)


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
    def test_invalid_type(self):
        with pytest.raises(TypeError, match="path must be a string"):
            next(file_tail_iterator(1))

    def test_file_not_created(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) is None
        f = open(tmp, "w")
        f.write("hi\n")
        f.flush()
        assert next(it) is not None

    def test_wait_for_newline(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) is None

        f = open(tmp, "w")
        f.write("no_newline_yet")
        assert next(it) is None
        f.write("\n")
        f.flush()
        assert next(it) == ["no_newline_yet\n"]

    def test_multiple_lines(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) is None

        f = open(tmp, "w")

        num_lines = 10
        for i in range(num_lines):
            s = f"{i}\n"
            f.write(s)
            f.flush()
            assert next(it) == [s]

        assert next(it) is None

    def test_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) is None

        f = open(tmp, "w")

        # Write lines in batches of 10, check that we get them back in batches.
        for _ in range(100):
            num_lines = 10
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert next(it) == [f"{i}\n" for i in range(10)]

        assert next(it) is None

    def test_max_line_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) is None

        f = open(tmp, "w")

        # Write lines in batches of 50, check that we get them back in batches of 10.
        for _ in range(100):
            num_lines = 50
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert next(it) == [f"{i}\n" for i in range(10)]
            assert next(it) == [f"{i}\n" for i in range(10, 20)]
            assert next(it) == [f"{i}\n" for i in range(20, 30)]
            assert next(it) == [f"{i}\n" for i in range(30, 40)]
            assert next(it) == [f"{i}\n" for i in range(40, 50)]

        assert next(it) is None

    def test_max_char_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) is None

        f = open(tmp, "w")

        # Write a single line that is over 60000 characters,
        # check we get it in batches of 20000
        f.write(f"{'1234567890' * 6000}\n")
        f.flush()

        assert next(it) == ["1234567890" * 2000]
        assert next(it) == ["1234567890" * 2000]
        assert next(it) == ["1234567890" * 2000]
        assert next(it) == ["\n"]
        assert next(it) is None

        # Write a 10 lines where last line is over 20000 characters,
        # check we get it in batches of 20000
        for i in range(9):
            f.write(f"{i}\n")
        f.write(f"{'1234567890' * 2000}\n")
        f.flush()

        first_nine_lines = [f"{i}\n" for i in range(9)]
        first_nine_lines_length = sum(len(line) for line in first_nine_lines)
        assert next(it) == first_nine_lines + [
            f"{'1234567890' * 2000}"[0:-first_nine_lines_length]
        ]
        # Remainder of last line
        assert next(it) == [f"{'1234567890' * 2000}"[-first_nine_lines_length:] + "\n"]
        assert next(it) is None

    def test_delete_file(self):
        with NamedTemporaryFile() as tmp:
            it = file_tail_iterator(tmp.name)
            f = open(tmp.name, "w")

            assert next(it) is None

            f.write("hi\n")
            f.flush()

            assert next(it) == ["hi\n"]

        # Calls should continue returning None after file deleted.
        assert next(it) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
