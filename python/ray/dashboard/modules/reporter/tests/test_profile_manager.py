import os
import sys
import tempfile
import time
from unittest.mock import patch

import pytest

import ray
from ray.dashboard.modules.reporter.profile_manager import MemoryProfilingManager
from ray.dashboard.tests.conftest import *  # noqa


@pytest.fixture
def setup_memory_profiler():
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_profiler = MemoryProfilingManager(tmpdir)

        @ray.remote
        class Actor:
            def getpid(self):
                return os.getpid()

            def long_run(self):
                print("Long-running task began.")
                time.sleep(1000)
                print("Long-running task completed.")

        actor = Actor.remote()

        yield actor, memory_profiler


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No memray on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX, requires memray & lldb installed in osx image",
)
class TestMemoryProfiling:
    async def test_basic_attach_profiler(self, setup_memory_profiler, shutdown_only):
        # test basic attach profiler to running process
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, profiler_filename, message = await memory_profiler.attach_profiler(
            pid, verbose=True
        )

        assert success, message
        assert f"Success attaching memray to process {pid}" in message
        assert profiler_filename in os.listdir(memory_profiler.profile_dir_path)

    async def test_profiler_multiple_attach(self, setup_memory_profiler, shutdown_only):
        # test multiple attaches
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, profiler_filename, message = await memory_profiler.attach_profiler(
            pid, verbose=True
        )

        assert success, message
        assert f"Success attaching memray to process {pid}" in message
        assert profiler_filename in os.listdir(memory_profiler.profile_dir_path)

        success, _, message = await memory_profiler.attach_profiler(pid)
        assert success, message
        assert f"Success attaching memray to process {pid}" in message

    async def test_detach_profiler_successful(
        self, setup_memory_profiler, shutdown_only
    ):
        # test basic detach profiler
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, _, message = await memory_profiler.attach_profiler(pid, verbose=True)
        assert success, message

        success, message = await memory_profiler.detach_profiler(pid, verbose=True)
        assert success, message
        assert f"Success detaching memray from process {pid}" in message

    async def test_detach_profiler_without_attach(
        self, setup_memory_profiler, shutdown_only
    ):
        # test detach profiler from unattached process
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())

        success, message = await memory_profiler.detach_profiler(pid)
        assert not success, message
        assert "Failed to execute" in message
        assert "no previous `memray attach`" in message

    async def test_profiler_memray_not_installed(
        self, setup_memory_profiler, shutdown_only
    ):
        # test profiler when memray is not installed
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())

        with patch("shutil.which", return_value=None):
            success, _, message = await memory_profiler.attach_profiler(pid)
            assert not success
            assert "memray is not installed" in message

    async def test_profiler_attach_process_not_found(
        self, setup_memory_profiler, shutdown_only
    ):
        # test basic attach profiler to non-existing process
        _, memory_profiler = setup_memory_profiler
        pid = 123456
        success, _, message = await memory_profiler.attach_profiler(pid)
        assert not success, message
        assert "Failed to execute" in message
        assert "The given process ID does not exist" in message

    async def test_profiler_get_profiler_result(
        self, setup_memory_profiler, shutdown_only
    ):
        # test get profiler result from running process
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, profiler_filename, message = await memory_profiler.attach_profiler(
            pid, verbose=True
        )
        assert success, message
        assert f"Success attaching memray to process {pid}" in message

        # get profiler result in flamegraph and table format
        supported_formats = ["flamegraph", "table"]
        unsupported_formats = ["json"]
        for format in supported_formats + unsupported_formats:
            success, message = await memory_profiler.get_profile_result(
                pid, profiler_filename=profiler_filename, format=format
            )
            if format in supported_formats:
                assert success, message
                assert f"{format} report" in message.decode("utf-8")
            else:
                assert not success, message
                assert f"{format} is not supported" in message

    async def test_profiler_result_not_exist(
        self, setup_memory_profiler, shutdown_only
    ):
        # test get profiler result from unexisting process
        _, memory_profiler = setup_memory_profiler
        pid = 123456
        profiler_filename = "non-existing-file"

        success, message = await memory_profiler.get_profile_result(
            pid, profiler_filename=profiler_filename, format=format
        )
        assert not success, message
        assert f"process {pid} has not been profiled" in message


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
