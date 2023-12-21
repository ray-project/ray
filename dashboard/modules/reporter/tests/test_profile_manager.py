import sys
import pytest
import shutil
import os
import time
from unittest.mock import patch

import ray
from ray.dashboard.tests.conftest import *  # noqa
from ray.dashboard.modules.reporter.profile_manager import MemoryProfilingManager


@pytest.fixture
def setup_memory_profiler():
    profiler_result_path = os.getcwd()
    memory_profiler = MemoryProfilingManager(profiler_result_path)

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

    cleanup_dir = memory_profiler.profile_dir_path
    if os.path.exists(cleanup_dir):
        shutil.rmtree(cleanup_dir)


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No memray on Windows.")
class TestMemoryProfiling:
    async def test_basic_attach_profiler(self, setup_memory_profiler, shutdown_only):
        # test basic attach profiler to running process
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, profiler_filename, message = await memory_profiler.attach_profiler(pid)

        assert success
        assert f"Success attaching memray to process {pid}" in message
        assert profiler_filename in os.listdir(memory_profiler.profile_dir_path)

    async def test_profiler_multiple_attach(self, setup_memory_profiler, shutdown_only):
        # test multiple attaches
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, profiler_filename, message = await memory_profiler.attach_profiler(pid)

        assert success
        assert f"Success attaching memray to process {pid}" in message
        assert profiler_filename in os.listdir(memory_profiler.profile_dir_path)

        success, _, message = await memory_profiler.attach_profiler(pid)
        assert success
        assert f"Success attaching memray to process {pid}" in message

    async def test_detach_profiler_successful(
        self, setup_memory_profiler, shutdown_only
    ):
        # test basic detach profiler
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, _, message = await memory_profiler.attach_profiler(pid)
        assert success

        success, message = await memory_profiler.detach_profiler(pid)
        assert success
        assert f"Success detaching memray from process {pid}" in message

    async def test_detach_profiler_without_attach(
        self, setup_memory_profiler, shutdown_only
    ):
        # test detach profiler from unattached process
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())

        success, message = await memory_profiler.detach_profiler(pid)
        assert not success
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
        assert not success
        assert "Failed to execute" in message
        assert "The given process ID does not exist" in message

    async def test_profiler_get_profiler_result(
        self, setup_memory_profiler, shutdown_only
    ):
        # test get profiler result from running process
        actor, memory_profiler = setup_memory_profiler
        pid = ray.get(actor.getpid.remote())
        actor.long_run.remote()
        success, profiler_filename, message = await memory_profiler.attach_profiler(pid)
        assert success
        assert f"Success attaching memray to process {pid}" in message

        # get profiler result in flamegraph and table format
        supported_formats = ["flamegraph", "table"]
        unsupported_formats = ["json"]
        for format in supported_formats + unsupported_formats:
            success, message = await memory_profiler.get_profile_result(
                pid, profiler_filename=profiler_filename, format=format
            )
            if format in supported_formats:
                assert success
                assert f"{format} report" in message.decode("utf-8")
            else:
                assert not success
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
        assert not success
        assert f"process {pid} has not been profiled" in message


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
