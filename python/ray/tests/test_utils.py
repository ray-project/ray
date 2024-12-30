# coding: utf-8
"""
Unit/Integration Testing for python/_private/utils.py

This currently expects to work for minimal installs.
"""

from tempfile import NamedTemporaryFile
import time
import pytest
import logging
from ray._private.utils import (
    async_file_tail_iterator,
    file_tail_iterator,
    get_or_create_event_loop,
    parse_pg_formatted_resources_to_original,
    sync_file_tail_iterator,
    try_import_each_module,
    get_current_node_cpu_model_name,
)
from unittest.mock import patch, mock_open
import sys

logger = logging.getLogger(__name__)


def test_get_or_create_event_loop_existing_event_loop():
    import asyncio
    import warnings

    # With running event loop
    expect_loop = asyncio.new_event_loop()
    expect_loop.set_debug(True)
    asyncio.set_event_loop(expect_loop)
    with warnings.catch_warnings():
        # Assert no deprecating warnings raised for python>=3.10
        warnings.simplefilter("error")
        actual_loop = get_or_create_event_loop()

        assert actual_loop == expect_loop, "Loop should not be recreated."


def test_get_or_create_event_loop_new_event_loop():
    import warnings

    with warnings.catch_warnings():
        # Assert no deprecating warnings raised for python>=3.10
        warnings.simplefilter("error")
        loop = get_or_create_event_loop()
        assert loop is not None, "new event loop should be created."


def test_try_import_each_module():
    mock_sys_modules = sys.modules.copy()
    modules_to_import = ["json", "logging", "re"]
    fake_module = "fake_import_does_not_exist"

    for lib in modules_to_import:
        if lib in mock_sys_modules:
            del mock_sys_modules[lib]

    with patch("ray._private.utils.logger.exception") as mocked_log_exception:
        with patch.dict(sys.modules, mock_sys_modules):
            patched_sys_modules = sys.modules

            try_import_each_module(
                modules_to_import[:1] + [fake_module] + modules_to_import[1:]
            )

            # Verify modules are imported.
            for lib in modules_to_import:
                assert (
                    lib in patched_sys_modules
                ), f"lib {lib} not in {patched_sys_modules}"

            # Verify import error is printed.
            found = False
            for args, _ in mocked_log_exception.call_args_list:
                found = any(fake_module in arg for arg in args)
                if found:
                    break

            assert found, (
                "Did not find print call with import "
                f"error {mocked_log_exception.call_args_list}"
            )


def test_parse_pg_formatted_resources():
    out = parse_pg_formatted_resources_to_original(
        {"CPU_group_e765be422c439de2cd263c5d9d1701000000": 1, "memory": 100}
    )
    assert out == {"CPU": 1, "memory": 100}

    out = parse_pg_formatted_resources_to_original(
        {
            "memory_group_4da1c24ac25bec85bc817b258b5201000000": 100.0,
            "memory_group_0_4da1c24ac25bec85bc817b258b5201000000": 100.0,
            "CPU_group_0_4da1c24ac25bec85bc817b258b5201000000": 1.0,
            "CPU_group_4da1c24ac25bec85bc817b258b5201000000": 1.0,
        }
    )
    assert out == {"CPU": 1, "memory": 100}


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Doesn't support non-linux"
)
def test_get_current_node_cpu_model_name():
    with patch(
        "builtins.open", mock_open(read_data="processor: 0\nmodel name: Intel Xeon")
    ):
        assert get_current_node_cpu_model_name() == "Intel Xeon"


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
    import os

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
