"""Unit tests for the GPU profiler manager.

All GPU and dynolog dependencies are mocked out.
This test just verifies that commands are launched correctly and that
validations are correctly performed.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from ray.dashboard.modules.reporter.gpu_profile_manager import GpuProfilingManager


@pytest.fixture
def mock_node_has_gpus(monkeypatch):
    monkeypatch.setattr(GpuProfilingManager, "node_has_gpus", lambda cls: True)
    yield


@pytest.fixture
def mock_dynolog_binaries(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda cmd: f"/usr/bin/fake_{cmd}")
    yield


@pytest.fixture
def mock_subprocess_popen(monkeypatch):
    mock_popen = MagicMock()
    mock_proc = MagicMock()
    mock_popen.return_value = mock_proc

    monkeypatch.setattr("subprocess.Popen", mock_popen)
    yield (mock_popen, mock_proc)


LOCALHOST = "127.0.0.1"


@pytest.fixture
def mock_asyncio_create_subprocess_exec(monkeypatch):
    mock_create_subprocess_exec = AsyncMock()
    mock_async_proc = mock_create_subprocess_exec.return_value = AsyncMock()
    mock_async_proc.communicate.return_value = b"mock stdout", b"mock stderr"
    mock_async_proc.returncode = 0
    monkeypatch.setattr("asyncio.create_subprocess_exec", mock_create_subprocess_exec)
    yield (mock_create_subprocess_exec, mock_async_proc)


def test_enabled(tmp_path, mock_node_has_gpus, mock_dynolog_binaries):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    assert gpu_profiler.enabled


def test_disabled_no_gpus(tmp_path, monkeypatch):
    monkeypatch.setattr(
        GpuProfilingManager, "node_has_gpus", classmethod(lambda cls: False)
    )
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    assert not gpu_profiler.enabled


def test_disabled_no_dynolog_bin(tmp_path, mock_node_has_gpus):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    assert not gpu_profiler.enabled


def test_start_monitoring_daemon(
    tmp_path, mock_node_has_gpus, mock_dynolog_binaries, mock_subprocess_popen
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)

    mocked_popen, mocked_proc = mock_subprocess_popen
    mocked_proc.pid = 123
    mocked_proc.poll.return_value = None

    gpu_profiler.start_monitoring_daemon()
    assert gpu_profiler.is_monitoring_daemon_running

    assert mocked_popen.call_count == 1
    assert mocked_popen.call_args[0][0] == [
        "/usr/bin/fake_dynolog",
        "--enable_ipc_monitor",
        "--port",
        str(gpu_profiler._DYNOLOG_PORT),
    ]

    # "Terminate" the daemon
    mocked_proc.poll.return_value = 0
    assert not gpu_profiler.is_monitoring_daemon_running


@pytest.mark.asyncio
async def test_gpu_profile_disabled(tmp_path):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    assert not gpu_profiler.enabled

    success, output = await gpu_profiler.gpu_profile(pid=123, num_iterations=1)
    assert not success
    assert output == gpu_profiler._DISABLED_ERROR_MESSAGE.format(
        ip_address=gpu_profiler._ip_address
    )


@pytest.mark.asyncio
async def test_gpu_profile_without_starting_daemon(
    tmp_path, mock_node_has_gpus, mock_dynolog_binaries
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    assert not gpu_profiler.is_monitoring_daemon_running

    with pytest.raises(RuntimeError, match="start_monitoring_daemon"):
        await gpu_profiler.gpu_profile(pid=123, num_iterations=1)


@pytest.mark.asyncio
async def test_gpu_profile_with_dead_daemon(
    tmp_path, mock_node_has_gpus, mock_dynolog_binaries, mock_subprocess_popen
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    gpu_profiler.start_monitoring_daemon()

    mocked_popen, mocked_proc = mock_subprocess_popen
    mocked_proc.pid = 123
    # "Terminate" the daemon
    mocked_proc.poll.return_value = 0
    assert not gpu_profiler.is_monitoring_daemon_running

    success, output = await gpu_profiler.gpu_profile(pid=456, num_iterations=1)
    assert not success
    print(output)
    assert "GPU monitoring daemon" in output


@pytest.mark.asyncio
async def test_gpu_profile_on_dead_process(
    tmp_path,
    monkeypatch,
    mock_node_has_gpus,
    mock_dynolog_binaries,
    mock_subprocess_popen,
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    gpu_profiler.start_monitoring_daemon()

    _, mocked_proc = mock_subprocess_popen
    mocked_proc.pid = 123
    mocked_proc.poll.return_value = None

    monkeypatch.setattr(GpuProfilingManager, "is_pid_alive", lambda cls, pid: False)

    success, output = await gpu_profiler.gpu_profile(pid=456, num_iterations=1)
    assert not success
    assert output == gpu_profiler._DEAD_PROCESS_ERROR_MESSAGE.format(
        pid=456, ip_address=gpu_profiler._ip_address
    )


@pytest.mark.asyncio
async def test_gpu_profile_no_matched_processes(
    tmp_path,
    monkeypatch,
    mock_node_has_gpus,
    mock_dynolog_binaries,
    mock_subprocess_popen,
    mock_asyncio_create_subprocess_exec,
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    gpu_profiler.start_monitoring_daemon()

    # Mock the daemon process
    _, mocked_daemon_proc = mock_subprocess_popen
    mocked_daemon_proc.pid = 123
    mocked_daemon_proc.poll.return_value = None

    monkeypatch.setattr(GpuProfilingManager, "is_pid_alive", lambda cls, pid: True)

    # Mock the asyncio.create_subprocess_exec
    (
        mocked_create_subprocess_exec,
        mocked_async_proc,
    ) = mock_asyncio_create_subprocess_exec
    mocked_async_proc.communicate.return_value = (
        f"{gpu_profiler._NO_PROCESSES_MATCHED_ERROR_MESSAGE_PREFIX}".encode(),
        b"dummy stderr",
    )
    process_pid = 456
    num_iterations = 1
    success, output = await gpu_profiler.gpu_profile(
        pid=process_pid, num_iterations=num_iterations
    )

    assert mocked_create_subprocess_exec.call_count == 1

    assert not success
    assert output == gpu_profiler._NO_PROCESSES_MATCHED_ERROR_MESSAGE.format(
        pid=process_pid, ip_address=gpu_profiler._ip_address
    )


@pytest.mark.asyncio
async def test_gpu_profile_timeout(
    tmp_path,
    monkeypatch,
    mock_node_has_gpus,
    mock_dynolog_binaries,
    mock_subprocess_popen,
    mock_asyncio_create_subprocess_exec,
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    gpu_profiler.start_monitoring_daemon()

    # Mock the daemon process
    _, mocked_daemon_proc = mock_subprocess_popen
    mocked_daemon_proc.pid = 123
    mocked_daemon_proc.poll.return_value = None

    monkeypatch.setattr(GpuProfilingManager, "is_pid_alive", lambda cls, pid: True)

    process_pid = 456
    num_iterations = 1
    task = asyncio.create_task(
        gpu_profiler.gpu_profile(
            pid=process_pid, num_iterations=num_iterations, _timeout_s=0.1
        )
    )

    await asyncio.sleep(0.2)
    success, output = await task
    assert not success
    assert "timed out" in output


@pytest.mark.asyncio
async def test_gpu_profile_process_dies_during_profiling(
    tmp_path,
    monkeypatch,
    mock_node_has_gpus,
    mock_dynolog_binaries,
    mock_subprocess_popen,
    mock_asyncio_create_subprocess_exec,
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    gpu_profiler.start_monitoring_daemon()

    # Mock the daemon process
    _, mocked_daemon_proc = mock_subprocess_popen
    mocked_daemon_proc.pid = 123
    mocked_daemon_proc.poll.return_value = None

    monkeypatch.setattr(GpuProfilingManager, "is_pid_alive", lambda cls, pid: True)

    process_pid = 456
    num_iterations = 1
    task = asyncio.create_task(
        gpu_profiler.gpu_profile(pid=process_pid, num_iterations=num_iterations)
    )

    monkeypatch.setattr(GpuProfilingManager, "is_pid_alive", lambda cls, pid: False)

    await asyncio.sleep(0.2)

    success, output = await task
    assert not success
    assert output == gpu_profiler._DEAD_PROCESS_ERROR_MESSAGE.format(
        pid=process_pid, ip_address=gpu_profiler._ip_address
    )


@pytest.mark.asyncio
async def test_gpu_profile_success(
    tmp_path,
    monkeypatch,
    mock_node_has_gpus,
    mock_dynolog_binaries,
    mock_subprocess_popen,
    mock_asyncio_create_subprocess_exec,
):
    gpu_profiler = GpuProfilingManager(tmp_path, ip_address=LOCALHOST)
    gpu_profiler.start_monitoring_daemon()

    # Mock the daemon process
    _, mocked_daemon_proc = mock_subprocess_popen
    mocked_daemon_proc.pid = 123
    mocked_daemon_proc.poll.return_value = None

    monkeypatch.setattr(GpuProfilingManager, "is_pid_alive", lambda cls, pid: True)
    monkeypatch.setattr(
        GpuProfilingManager, "_get_trace_filename", lambda cls: "dummy_trace.json"
    )
    dumped_trace_filepath = gpu_profiler._profile_dir_path / "dummy_trace.json"
    dumped_trace_filepath.touch()

    # Mock the asyncio.create_subprocess_exec
    (
        mocked_create_subprocess_exec,
        mocked_async_proc,
    ) = mock_asyncio_create_subprocess_exec
    process_pid = 456
    num_iterations = 1
    success, output = await gpu_profiler.gpu_profile(
        pid=process_pid, num_iterations=num_iterations
    )

    # Verify the command was launched correctly
    assert mocked_create_subprocess_exec.call_count == 1
    profile_launch_args = list(mocked_create_subprocess_exec.call_args[0])
    assert profile_launch_args[:6] == [
        "/usr/bin/fake_dyno",
        "--port",
        str(gpu_profiler._DYNOLOG_PORT),
        "gputrace",
        "--pids",
        str(process_pid),
    ]

    assert "--log-file" in profile_launch_args
    profile_log_file_arg = profile_launch_args[
        profile_launch_args.index("--log-file") + 1
    ]
    assert Path(profile_log_file_arg).is_relative_to(tmp_path)

    assert "--iterations" in profile_launch_args
    assert profile_launch_args[profile_launch_args.index("--iterations") + 1] == str(
        num_iterations
    )

    assert success
    assert output == str(dumped_trace_filepath.relative_to(tmp_path))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
