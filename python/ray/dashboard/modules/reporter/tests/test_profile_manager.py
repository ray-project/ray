import os
import sys
import tempfile
import time
from unittest.mock import AsyncMock, patch

import pytest

import ray
from ray.dashboard.modules.reporter.profile_manager import (
    CpuProfilingManager,
    MemoryProfilingManager,
)
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


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
class TestCpuProfiling:
    async def _capture_pyspy_cmd(self, **cpu_profile_kwargs):
        """Run cpu_profile with subprocess execution mocked out and return the
        py-spy command that would have been executed.

        We patch ``asyncio.create_subprocess_exec`` (the same primitive the
        manager uses) and have the fake process exit non-zero so that
        ``cpu_profile`` short-circuits before attempting to read the (never
        created) output file. The command is fully constructed before the
        subprocess is spawned, so the captured args are valid regardless.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_profiler = CpuProfilingManager(tmpdir)

            fake_process = AsyncMock()
            fake_process.communicate.return_value = (b"", b"boom")
            fake_process.returncode = 1

            with patch(
                "ray.dashboard.modules.reporter.profile_manager.shutil.which",
                return_value="/fake/py-spy",
            ), patch(
                "ray.dashboard.modules.reporter.profile_manager."
                "_can_passwordless_sudo",
                new=AsyncMock(return_value=False),
            ), patch(
                "asyncio.create_subprocess_exec",
                new=AsyncMock(return_value=fake_process),
            ) as mock_exec:
                await cpu_profiler.cpu_profile(pid=12345, **cpu_profile_kwargs)

            assert mock_exec.call_count == 1
            # create_subprocess_exec(*cmd, ...) -> positional args are the cmd.
            return list(mock_exec.call_args.args)

    async def test_cpu_profile_idle_flag_added(self):
        # idle=True should append `--idle` to the py-spy command.
        cmd = await self._capture_pyspy_cmd(idle=True)
        assert "--idle" in cmd

    async def test_cpu_profile_idle_not_added_by_default(self):
        # By default (idle=False) the `--idle` flag should be absent.
        cmd = await self._capture_pyspy_cmd()
        assert "--idle" not in cmd

    async def test_cpu_profile_subprocesses_flag_added(self):
        # subprocesses=True should append `--subprocesses` to the py-spy command.
        cmd = await self._capture_pyspy_cmd(subprocesses=True)
        assert "--subprocesses" in cmd

    async def test_cpu_profile_subprocesses_not_added_by_default(self):
        # By default the `--subprocesses` flag should be absent.
        cmd = await self._capture_pyspy_cmd()
        assert "--subprocesses" not in cmd


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
class TestTraceDump:
    async def _capture_pyspy_cmd(self, **trace_dump_kwargs):
        """Run trace_dump with subprocess execution mocked out and return the
        py-spy command that would have been executed.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_profiler = CpuProfilingManager(tmpdir)

            fake_process = AsyncMock()
            fake_process.communicate.return_value = (b"", b"boom")
            fake_process.returncode = 1

            with patch(
                "ray.dashboard.modules.reporter.profile_manager.shutil.which",
                return_value="/fake/py-spy",
            ), patch(
                "ray.dashboard.modules.reporter.profile_manager."
                "_can_passwordless_sudo",
                new=AsyncMock(return_value=False),
            ), patch(
                "asyncio.create_subprocess_exec",
                new=AsyncMock(return_value=fake_process),
            ) as mock_exec:
                await cpu_profiler.trace_dump(pid=12345, **trace_dump_kwargs)

            assert mock_exec.call_count == 1
            return list(mock_exec.call_args.args)

    async def test_trace_dump_subprocesses_flag_added(self):
        # subprocesses=True should append `--subprocesses` to the py-spy dump command.
        cmd = await self._capture_pyspy_cmd(subprocesses=True)
        assert "dump" in cmd
        assert "--subprocesses" in cmd

    async def test_trace_dump_subprocesses_not_added_by_default(self):
        # By default the `--subprocesses` flag should be absent.
        cmd = await self._capture_pyspy_cmd()
        assert "--subprocesses" not in cmd


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
