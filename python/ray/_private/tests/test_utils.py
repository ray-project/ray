import sys
from tempfile import NamedTemporaryFile
import time
import pytest
from ray._private.utils import (
    async_file_tail_iterator,
    file_tail_iterator,
    sync_file_tail_iterator,
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
    filename = None
    with NamedTemporaryFile() as f:
        filename = f.name
    yield filename


class TestIterLine:
    def test_invalid_type(self):
        with pytest.raises(TypeError, match="path must be a string"):
            next(file_tail_iterator(1))

    def test_file_not_created(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) == (False, None)
        f = open(tmp, "w")
        f.write("hi\n")
        f.flush()
        assert next(it) == (True, ["hi\n"])

    def test_wait_for_newline(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) == (False, None)

        f = open(tmp, "w")
        f.write("no_newline_yet")
        assert next(it) == (True, None)
        f.write("\n")
        f.flush()
        assert next(it) == (True, ["no_newline_yet\n"])

    def test_multiple_lines(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) == (False, None)

        f = open(tmp, "w")

        num_lines = 10
        for i in range(num_lines):
            s = f"{i}\n"
            f.write(s)
            f.flush()
            assert next(it) == (True, [s])

        assert next(it) == (True, None)

    def test_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) == (False, None)

        f = open(tmp, "w")

        # Write lines in batches of 10, check that we get them back in batches.
        for _ in range(100):
            num_lines = 10
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert next(it) == (True, [f"{i}\n" for i in range(10)])

        assert next(it) == (True, None)

    def test_max_line_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) == (False, None)

        f = open(tmp, "w")

        # Write lines in batches of 50, check that we get them back in batches of 10.
        for _ in range(100):
            num_lines = 50
            for i in range(num_lines):
                f.write(f"{i}\n")
            f.flush()

            assert next(it) == (True, [f"{i}\n" for i in range(10)])
            assert next(it) == (True, [f"{i}\n" for i in range(10, 20)])
            assert next(it) == (True, [f"{i}\n" for i in range(20, 30)])
            assert next(it) == (True, [f"{i}\n" for i in range(30, 40)])
            assert next(it) == (True, [f"{i}\n" for i in range(40, 50)])

        assert next(it) == (True, None)

    def test_max_char_batching(self, tmp):
        it = file_tail_iterator(tmp)
        assert next(it) == (False, None)

        f = open(tmp, "w")

        # Write a single line that is 60k characters
        f.write(f"{'1234567890' * 6000}\n")
        # Write a 4 lines that are 10k characters each
        for _ in range(4):
            f.write(f"{'1234567890' * 500}\n")
        f.flush()

        # First line will come in a batch of its own
        assert next(it) == (True, [f"{'1234567890' * 6000}\n"])
        # Other 4 lines will be batched together
        assert next(it) == (
            True,
            [
                f"{'1234567890' * 500}\n",
            ]
            * 4,
        )
        assert next(it) == (True, None)

    def test_delete_file(self):
        with NamedTemporaryFile() as tmp:
            it = file_tail_iterator(tmp.name)
            f = open(tmp.name, "w")

            assert next(it) == (True, None)

            f.write("hi\n")
            f.flush()

            assert next(it) == (True, ["hi\n"])

        # Calls should continue returning None after file deleted.
        assert next(it) == (False, None)

    @pytest.mark.asyncio
    async def test_wait_on_EOF_async(self, tmp):
        it = async_file_tail_iterator(tmp)
        assert await anext(it) is None
        file_existance_start_time = time.perf_counter()
        assert await anext(it) is None
        file_existance_end_time = time.perf_counter()
        assert file_existance_end_time - file_existance_start_time < 0.1

        open(tmp, "w")
        assert await anext(it) is None
        EOF_start_time = time.perf_counter()
        assert await anext(it) is None
        EOF_end_time = time.perf_counter()
        assert EOF_end_time - EOF_start_time > 0.8

    def test_wati_on_EOF_sync(self, tmp):
        it = sync_file_tail_iterator(tmp)
        assert next(it) is None
        file_existance_start_time = time.perf_counter()
        assert next(it) is None
        file_existance_end_time = time.perf_counter()
        assert file_existance_end_time - file_existance_start_time < 0.1

        open(tmp, "w")
        assert next(it) is None
        EOF_start_time = time.perf_counter()
        assert next(it) is None
        EOF_end_time = time.perf_counter()
        assert EOF_end_time - EOF_start_time > 0.8


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
