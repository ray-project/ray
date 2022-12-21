import io
import os
import re
import subprocess
import sys
import tempfile
import time
import logging
from collections import Counter, defaultdict
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import ray
from ray._private import ray_constants
from ray._private.log_monitor import (
    LOG_NAME_UPDATE_INTERVAL_S,
    RAY_LOG_MONITOR_MANY_FILES_THRESHOLD,
    LogFileInfo,
    LogMonitor,
)
from ray._private.test_utils import (
    get_log_batch,
    get_log_message,
    init_log_pubsub,
    run_string_as_driver,
    wait_for_condition,
)
from ray.cross_language import java_actor_class


def set_logging_config(monkeypatch, max_bytes, backup_count):
    monkeypatch.setenv("RAY_ROTATION_MAX_BYTES", str(max_bytes))
    monkeypatch.setenv("RAY_ROTATION_BACKUP_COUNT", str(backup_count))


def test_reopen_changed_inode(tmp_path):
    """Make sure that when we reopen a file because the inode has changed, we
    open to the right location."""

    path1 = tmp_path / "file"
    path2 = tmp_path / "changed_file"

    with open(path1, "w") as f:
        for i in range(1000):
            print(f"{i}", file=f)

    with open(path2, "w") as f:
        for i in range(2000):
            print(f"{i}", file=f)

    file_info = LogFileInfo(
        filename=path1,
        size_when_last_opened=0,
        file_position=0,
        file_handle=None,
        is_err_file=False,
        job_id=None,
        worker_pid=None,
    )

    file_info.reopen_if_necessary()
    for _ in range(1000):
        file_info.file_handle.readline()

    orig_file_pos = file_info.file_handle.tell()
    file_info.file_position = orig_file_pos

    # NOTE: On windows, an open file can't be deleted.
    file_info.file_handle.close()
    os.remove(path1)
    os.rename(path2, path1)

    file_info.reopen_if_necessary()

    assert file_info.file_position == orig_file_pos
    assert file_info.file_handle.tell() == orig_file_pos


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_deleted_file_does_not_throw_error(tmp_path):
    filename = tmp_path / "file"

    Path(filename).touch()

    file_info = LogFileInfo(
        filename=filename,
        size_when_last_opened=0,
        file_position=0,
        file_handle=None,
        is_err_file=False,
        job_id=None,
        worker_pid=None,
    )

    file_info.reopen_if_necessary()

    os.remove(filename)

    file_info.reopen_if_necessary()


def test_log_rotation_config(ray_start_cluster, monkeypatch):
    cluster = ray_start_cluster
    max_bytes = 100
    backup_count = 3

    # Create a cluster.
    set_logging_config(monkeypatch, max_bytes, backup_count)
    head_node = cluster.add_node(num_cpus=0)
    # Set a different env var for a worker node.
    set_logging_config(monkeypatch, 0, 0)
    worker_node = cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()

    config = head_node.logging_config
    assert config["log_rotation_max_bytes"] == max_bytes
    assert config["log_rotation_backup_count"] == backup_count
    config = worker_node.logging_config
    assert config["log_rotation_max_bytes"] == 0
    assert config["log_rotation_backup_count"] == 0


def test_log_file_exists(shutdown_only):
    """Verify all log files exist as specified in
    https://docs.ray.io/en/master/ray-observability/ray-logging.html#logging-directory-structure # noqa
    """
    ray.init(num_cpus=1)
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    log_rotating_component = [
        (ray_constants.PROCESS_TYPE_DASHBOARD, [".log", ".err"]),
        (ray_constants.PROCESS_TYPE_DASHBOARD_AGENT, [".log"]),
        (ray_constants.PROCESS_TYPE_LOG_MONITOR, [".log", ".err"]),
        (ray_constants.PROCESS_TYPE_MONITOR, [".log", ".out", ".err"]),
        (ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER, [".log"]),
        (ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER, [".log"]),
        # Below components are not log rotating now.
        (ray_constants.PROCESS_TYPE_RAYLET, [".out", ".err"]),
        (ray_constants.PROCESS_TYPE_GCS_SERVER, [".out", ".err"]),
        (ray_constants.PROCESS_TYPE_WORKER, [".out", ".err"]),
    ]

    # Run the basic workload.
    @ray.remote
    def f():
        for i in range(10):
            print(f"test {i}")

    # Create a runtime env to make sure dashboard agent is alive.
    ray.get(f.options(runtime_env={"env_vars": {"A": "a", "B": "b"}}).remote())

    paths = list(log_dir_path.iterdir())

    def component_and_suffix_exists(component, paths):
        component, suffixes = component
        for path in paths:
            filename = path.stem
            suffix = path.suffix
            if component in filename:
                # core-worker log also contains "worker keyword". We ignore this case.
                if (
                    component == ray_constants.PROCESS_TYPE_WORKER
                    and ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER in filename
                ):
                    continue
                if suffix in suffixes:
                    return True
                else:
                    # unexpected suffix.
                    return False

        return False

    for component in log_rotating_component:
        assert component_and_suffix_exists(component, paths), (component, paths)


def test_log_rotation(shutdown_only, monkeypatch):
    max_bytes = 1
    backup_count = 3
    set_logging_config(monkeypatch, max_bytes, backup_count)
    ray.init(num_cpus=1)
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    log_rotating_component = [
        ray_constants.PROCESS_TYPE_DASHBOARD,
        ray_constants.PROCESS_TYPE_DASHBOARD_AGENT,
        ray_constants.PROCESS_TYPE_LOG_MONITOR,
        ray_constants.PROCESS_TYPE_MONITOR,
        ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER,
        ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER,
        # Below components are not log rotating now.
        # ray_constants.PROCESS_TYPE_RAYLET,
        # ray_constants.PROCESS_TYPE_GCS_SERVER,
        # ray_constants.PROCESS_TYPE_WORKER,
    ]

    # Run the basic workload.
    @ray.remote
    def f():
        for i in range(10):
            print(f"test {i}")

    # Create a runtime env to make sure dashboard agent is alive.
    ray.get(f.options(runtime_env={"env_vars": {"A": "a", "B": "b"}}).remote())

    paths = list(log_dir_path.iterdir())

    def component_exist(component, paths):
        for path in paths:
            filename = path.stem
            if component in filename:
                return True
        return False

    def component_file_only_one_log_entry(component):
        """Since max_bytes is 1, the log file should
        only have at most one log entry.
        """
        for path in paths:
            if not component_exist(component, [path]):
                continue

            with open(path) as file:
                found = False
                for line in file:
                    if re.match(r"^\[?\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d", line):
                        if found:
                            return False
                        found = True
        return True

    for component in log_rotating_component:
        assert component_exist(component, paths), paths
        assert component_file_only_one_log_entry(component)

    # Check if the backup count is respected.
    file_cnts = defaultdict(int)
    for path in paths:
        filename = path.name
        parts = filename.split(".")
        if len(parts) == 3:
            filename_without_suffix = parts[0]
            file_cnts[filename_without_suffix] += 1
    for filename, file_cnt in file_cnts.items():
        assert file_cnt <= backup_count, (
            f"{filename} has files that are more than "
            f"backup count {backup_count}, file count: {file_cnt}"
        )


def test_periodic_event_stats(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={"event_stats_print_interval_ms": 100, "event_stats": True},
    )
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    # Run the basic workload.
    @ray.remote
    def f():
        pass

    ray.get(f.remote())

    paths = list(log_dir_path.iterdir())

    def is_event_loop_stats_found(path):
        found = False
        with open(path) as f:
            event_loop_stats_identifier = "Event stats"
            for line in f.readlines():
                if event_loop_stats_identifier in line:
                    found = True
        return found

    for path in paths:
        # Need to remove suffix to avoid reading log rotated files.
        if "python-core-driver" in str(path):
            wait_for_condition(lambda: is_event_loop_stats_found(path))
        if "raylet.out" in str(path):
            wait_for_condition(lambda: is_event_loop_stats_found(path))
        if "gcs_server.out" in str(path):
            wait_for_condition(lambda: is_event_loop_stats_found(path))


def test_worker_id_names(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={"event_stats_print_interval_ms": 100, "event_stats": True},
    )
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    # Run the basic workload.
    @ray.remote
    def f():
        print("hello")

    ray.get(f.remote())

    paths = list(log_dir_path.iterdir())

    ids = []
    for path in paths:
        if "python-core-worker" in str(path):
            pattern = ".*-([a-f0-9]*).*"
        elif "worker" in str(path):
            pattern = ".*worker-([a-f0-9]*)-.*-.*"
        else:
            continue
        worker_id = re.match(pattern, str(path)).group(1)
        ids.append(worker_id)
    counts = Counter(ids).values()
    for count in counts:
        # There should be a "python-core-.*.log", "worker-.*.out",
        # and "worker-.*.err"
        assert count == 3


def test_log_pid_with_hex_job_id(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)

    def submit_job():
        # Connect a driver to the Ray cluster.
        ray.init(address=cluster.address, ignore_reinit_error=True)
        p = init_log_pubsub()
        # It always prints the monitor messages.
        logs = get_log_message(p, 1)

        @ray.remote
        def f():
            print("remote func")

        ray.get(f.remote())

        def matcher(log_batch):
            return log_batch["task_name"] == "f"

        logs = get_log_batch(p, 1, matcher=matcher)
        # It should logs with pid of hex job id instead of None
        assert logs[0]["pid"] is not None
        ray.shutdown()

    # NOTE(xychu): loop ten times to make job id from 01000000 to 0a000000,
    #              in order to trigger hex pattern
    for _ in range(10):
        submit_job()


def test_ignore_windows_access_violation(ray_start_regular_shared):
    @ray.remote
    def print_msg():
        print("Windows fatal exception: access violation\n")

    @ray.remote
    def print_after(_obj):
        print("done")

    p = init_log_pubsub()
    print_after.remote(print_msg.remote())
    msgs = get_log_message(
        p, num=3, timeout=1, job_id=ray.get_runtime_context().job_id.hex()
    )

    assert len(msgs) == 1, msgs
    assert msgs[0][0] == "done"


def test_log_redirect_to_stderr(shutdown_only, capfd):

    log_components = {
        ray_constants.PROCESS_TYPE_DASHBOARD: "Dashboard head grpc address",
        ray_constants.PROCESS_TYPE_DASHBOARD_AGENT: "Dashboard agent grpc address",
        ray_constants.PROCESS_TYPE_GCS_SERVER: "Loading job table data",
        # No log monitor output if all components are writing to stderr.
        ray_constants.PROCESS_TYPE_LOG_MONITOR: "",
        ray_constants.PROCESS_TYPE_MONITOR: "Starting monitor using ray installation",
        ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER: "worker server started",
        ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER: "driver server started",
        # TODO(Clark): Add coverage for Ray Client.
        # ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER: "Starting Ray Client server",
        ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER: "",
        ray_constants.PROCESS_TYPE_RAYLET: "Starting object store with directory",
        # No reaper process run (kernel fate-sharing).
        ray_constants.PROCESS_TYPE_REAPER: "",
        # No reporter process run.
        ray_constants.PROCESS_TYPE_REPORTER: "",
        # No web UI process run.
        ray_constants.PROCESS_TYPE_WEB_UI: "",
        # Unused.
        ray_constants.PROCESS_TYPE_WORKER: "",
    }

    script = """
import os
from pathlib import Path

import ray

os.environ["RAY_LOG_TO_STDERR"] = "1"
ray.init()

session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
session_path = Path(session_dir)
log_dir_path = session_path / "logs"

# Run the basic workload.
@ray.remote
def f():
    for i in range(10):
        print(f"test {{i}}")

ray.get(f.remote())

log_component_names = {}

# Confirm that no log files are created for any of the components.
paths = list(path.stem for path in log_dir_path.iterdir())
assert set(log_component_names).isdisjoint(set(paths)), paths
""".format(
        str(list(log_components.keys()))
    )
    stderr = run_string_as_driver(script)

    # Make sure that the expected startup log records for each of the
    # components appears in the stderr stream.
    # stderr = capfd.readouterr().err
    for component, canonical_record in log_components.items():
        if not canonical_record:
            # Process not run or doesn't generate logs; skip.
            continue
        assert canonical_record in stderr, stderr
        if component == ray_constants.PROCESS_TYPE_REDIS_SERVER:
            # Redis doesn't expose hooks for custom log formats, so we aren't able to
            # inject the Redis server component name into the log records.
            continue
        # NOTE: We do a prefix match instead of including the enclosing right
        # parentheses since some components, like the core driver and worker, add a
        # unique ID suffix.
        assert f"({component}" in stderr, stderr


def test_segfault_stack_trace(ray_start_cluster, capsys):
    @ray.remote
    def f():
        import ctypes

        ctypes.string_at(0)

    with pytest.raises(
        ray.exceptions.WorkerCrashedError, match="The worker died unexpectedly"
    ):
        ray.get(f.remote())

    stderr = capsys.readouterr().err
    assert (
        "*** SIGSEGV received at" in stderr
    ), f"C++ stack trace not found in stderr: {stderr}"
    assert (
        "Fatal Python error: Segmentation fault" in stderr
    ), f"Python stack trace not found in stderr: {stderr}"


@pytest.mark.skipif(
    sys.platform == "win32" or sys.platform == "darwin",
    reason="TODO(simon): Failing on Windows and OSX.",
)
def test_log_java_worker_logs(shutdown_only, capsys):
    tmp_dir = tempfile.mkdtemp()
    print("using tmp_dir", tmp_dir)
    with open(os.path.join(tmp_dir, "MyClass.java"), "w") as f:
        f.write(
            """
public class MyClass {
    public int printToLog(String line) {
        System.err.println(line);
        return 0;
    }
}
        """
        )
    subprocess.check_call(["javac", "MyClass.java"], cwd=tmp_dir)
    subprocess.check_call(["jar", "-cf", "myJar.jar", "MyClass.class"], cwd=tmp_dir)

    ray.init(
        job_config=ray.job_config.JobConfig(code_search_path=[tmp_dir]),
    )

    handle = java_actor_class("MyClass").remote()
    ray.get(handle.printToLog.remote("here's my random line!"))

    def check():
        out, err = capsys.readouterr()
        out += err
        with capsys.disabled():
            print(out)
        return "here's my random line!" in out

    wait_for_condition(check)


"""
Unit testing log monitor.
"""


def create_file(dir, filename, content):
    f = dir / filename
    f.write_text(content)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failing on Windows",
)
def test_log_monitor(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    # Create an old dir.
    (log_dir / "old").mkdir()
    worker_id = "6df6d5dd8ca5215658e4a8f9a569a9d98e27094f9cc35a4ca43d272c"
    job_id = "01000000"
    dead_pid = "47660"
    alive_pid = "12345"

    def proc_alive(pid):
        return pid != int(dead_pid)

    mock_publisher = MagicMock()
    log_monitor = LogMonitor(str(log_dir), mock_publisher, proc_alive, max_files_open=5)

    # files
    worker_out_log_file = f"worker-{worker_id}-{job_id}-{dead_pid}.out"
    worker_err_log_file = f"worker-{worker_id}-{job_id}-{dead_pid}.err"
    monitor = "monitor.log"
    raylet_out = "raylet.out"
    raylet_err = "raylet.err"
    gcs_server_err = "gcs_server.1.err"

    contents = "123"

    create_file(log_dir, raylet_err, contents)
    create_file(log_dir, raylet_out, contents)
    create_file(log_dir, gcs_server_err, contents)
    create_file(log_dir, monitor, contents)
    create_file(log_dir, worker_out_log_file, contents)
    create_file(log_dir, worker_err_log_file, contents)

    """
    Test files are updated.
    """
    log_monitor.update_log_filenames()

    assert len(log_monitor.open_file_infos) == 0
    assert len(log_monitor.closed_file_infos) == 5
    assert log_monitor.can_open_more_files is True
    assert len(log_monitor.log_filenames) == 5

    def file_exists(log_filenames, filename):
        for f in log_filenames:
            if filename in f:
                return True
        return False

    assert file_exists(log_monitor.log_filenames, raylet_err)
    assert not file_exists(log_monitor.log_filenames, raylet_out)
    assert file_exists(log_monitor.log_filenames, gcs_server_err)
    assert file_exists(log_monitor.log_filenames, monitor)
    assert file_exists(log_monitor.log_filenames, worker_out_log_file)
    assert file_exists(log_monitor.log_filenames, worker_err_log_file)

    def get_file_info(file_infos, filename):
        for file_info in file_infos:
            if filename in file_info.filename:
                return file_info
        assert False, "Shouldn't reach."

    raylet_err_info = get_file_info(log_monitor.closed_file_infos, raylet_err)
    gcs_server_err_info = get_file_info(log_monitor.closed_file_infos, gcs_server_err)
    monitor_info = get_file_info(log_monitor.closed_file_infos, monitor)
    worker_out_log_file_info = get_file_info(
        log_monitor.closed_file_infos, worker_out_log_file
    )
    worker_err_log_file_info = get_file_info(
        log_monitor.closed_file_infos, worker_err_log_file
    )

    assert raylet_err_info.is_err_file
    assert gcs_server_err_info.is_err_file
    assert not monitor_info.is_err_file
    assert not worker_out_log_file_info.is_err_file
    assert worker_err_log_file_info.is_err_file

    assert worker_out_log_file_info.job_id == job_id
    assert worker_err_log_file_info.job_id == job_id
    assert worker_out_log_file_info.worker_pid == int(dead_pid)
    assert worker_out_log_file_info.worker_pid == int(dead_pid)

    """
    Test files are opened.
    """
    log_monitor.open_closed_files()
    assert len(log_monitor.open_file_infos) == 5
    assert len(log_monitor.closed_file_infos) == 0
    assert not log_monitor.can_open_more_files

    """
    Test files are published.
    """

    assert log_monitor.check_log_files_and_publish_updates()
    assert raylet_err_info.worker_pid == "raylet"
    assert gcs_server_err_info.worker_pid == "gcs_server"
    assert monitor_info.worker_pid == "autoscaler"

    assert mock_publisher.publish_logs.call_count

    for file_info in log_monitor.open_file_infos:
        mock_publisher.publish_logs.assert_any_call(
            {
                "ip": log_monitor.ip,
                "pid": file_info.worker_pid,
                "job": file_info.job_id,
                "is_err": file_info.is_err_file,
                "lines": [contents],
                "actor_name": file_info.actor_name,
                "task_name": file_info.task_name,
            }
        )
    # If there's no new update, it should return False.
    assert not log_monitor.check_log_files_and_publish_updates()

    # Test max lines read == 99 is repsected.
    lines = "1\n" * int(1.5 * ray_constants.LOG_MONITOR_NUM_LINES_TO_READ)
    with open(raylet_err_info.filename, "a") as f:
        # Write 150 more lines.
        f.write(lines)

    assert log_monitor.check_log_files_and_publish_updates()
    mock_publisher.publish_logs.assert_any_call(
        {
            "ip": log_monitor.ip,
            "pid": raylet_err_info.worker_pid,
            "job": raylet_err_info.job_id,
            "is_err": raylet_err_info.is_err_file,
            "lines": ["1" for _ in range(ray_constants.LOG_MONITOR_NUM_LINES_TO_READ)],
            "actor_name": file_info.actor_name,
            "task_name": file_info.task_name,
        }
    )

    """
    Test files are closed.
    """
    # log_monitor.open_closed_files() should close all files
    # if it cannot open new files.
    new_worker_err_file = f"worker-{worker_id}-{job_id}-{alive_pid}.err"
    create_file(log_dir, new_worker_err_file, contents)
    log_monitor.update_log_filenames()

    # System logs are not closed.
    # - raylet, gcs, monitor
    # Dead workers are not tracked anymore. They will be moved to old folder.
    # - dead pid out & err
    # alive worker is going to be newly opened.
    log_monitor.open_closed_files()
    assert len(log_monitor.open_file_infos) == 2
    assert log_monitor.can_open_more_files
    # Two dead workers are not tracked anymore, and they will be in the old folder.
    # monitor.err and gcs_server.1.err have not been updated, so they remain closed.
    assert len(log_monitor.closed_file_infos) == 2
    assert len(list((log_dir / "old").iterdir())) == 2


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failing on Windows",
)
def test_log_monitor_actor_task_name(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    worker_id = "6df6d5dd8ca5215658e4a8f9a569a9d98e27094f9cc35a4ca43d272c"
    job_id = "01000000"
    pid = "47660"

    mock_publisher = MagicMock()
    log_monitor = LogMonitor(
        str(log_dir), mock_publisher, lambda _: True, max_files_open=5
    )
    worker_out_log_file = f"worker-{worker_id}-{job_id}-{pid}.out"
    first_line = "First line\n"
    create_file(log_dir, worker_out_log_file, first_line)
    log_monitor.update_log_filenames()
    log_monitor.open_closed_files()
    assert len(log_monitor.open_file_infos) == 1
    file_info = log_monitor.open_file_infos[0]

    # Test task name updated.
    task_name = "task"
    with open(file_info.filename, "a") as f:
        # Write 150 more lines.
        f.write(f"{ray_constants.LOG_PREFIX_TASK_NAME}{task_name}\n")
        f.write("line")
    log_monitor.check_log_files_and_publish_updates()
    assert file_info.task_name == task_name
    assert file_info.actor_name is None
    mock_publisher.publish_logs.assert_any_call(
        {
            "ip": log_monitor.ip,
            "pid": file_info.worker_pid,
            "job": file_info.job_id,
            "is_err": file_info.is_err_file,
            "lines": ["line"],
            "actor_name": None,
            "task_name": task_name,
        }
    )

    # Test the actor name is updated.
    actor_name = "actor"
    with open(file_info.filename, "a") as f:
        # Write 150 more lines.
        f.write(f"{ray_constants.LOG_PREFIX_ACTOR_NAME}{actor_name}\n")
        f.write("line2")
    log_monitor.check_log_files_and_publish_updates()
    assert file_info.task_name is None
    assert file_info.actor_name == actor_name
    mock_publisher.publish_logs.assert_any_call(
        {
            "ip": log_monitor.ip,
            "pid": file_info.worker_pid,
            "job": file_info.job_id,
            "is_err": file_info.is_err_file,
            "lines": ["line2"],
            "actor_name": actor_name,
            "task_name": None,
        }
    )


@pytest.fixture
def mock_timer():
    f = time.time
    time.time = MagicMock()
    yield time.time
    time.time = f


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failing on Windows",
)
def test_log_monitor_update_backpressure(tmp_path, mock_timer):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    mock_publisher = MagicMock()
    log_monitor = LogMonitor(
        str(log_dir), mock_publisher, lambda _: True, max_files_open=5
    )

    current = 0
    mock_timer.return_value = current

    log_monitor.log_filenames = []
    # When threshold < RAY_LOG_MONITOR_MANY_FILES_THRESHOLD, update should happen.
    assert log_monitor.should_update_filenames(current)
    # Add a new file.
    log_monitor.log_filenames = [
        "raylet.out" for _ in range(RAY_LOG_MONITOR_MANY_FILES_THRESHOLD)
    ]
    # If the threshold is met, we should update the file after
    # LOG_NAME_UPDATE_INTERVAL_S.
    assert not log_monitor.should_update_filenames(current)
    mock_timer.return_value = LOG_NAME_UPDATE_INTERVAL_S - 0.1
    assert not log_monitor.should_update_filenames(current)
    mock_timer.return_value = LOG_NAME_UPDATE_INTERVAL_S
    assert not log_monitor.should_update_filenames(current)
    mock_timer.return_value = LOG_NAME_UPDATE_INTERVAL_S + 0.1
    assert log_monitor.should_update_filenames(current)


def test_repr_inheritance():
    """Tests that a subclass's repr is used in logging."""
    logger = logging.getLogger(__name__)

    class MyClass:
        def __repr__(self) -> str:
            return "ThisIsMyCustomActorName"

        def do(self):
            logger.warning("text")

    class MySubclass(MyClass):
        pass

    my_class_remote = ray.remote(MyClass)
    my_subclass_remote = ray.remote(MySubclass)

    f = io.StringIO()
    with redirect_stderr(f):
        my_class_actor = my_class_remote.remote()
        ray.get(my_class_actor.do.remote())
        # Wait a little to be sure that we have captured the output
        time.sleep(1)
        print("", flush=True)
        print("", flush=True, file=sys.stderr)
        f = f.getvalue()
        assert "ThisIsMyCustomActorName" in f and "MySubclass" not in f

    f2 = io.StringIO()
    with redirect_stderr(f2):
        my_subclass_actor = my_subclass_remote.remote()
        ray.get(my_subclass_actor.do.remote())
        # Wait a little to be sure that we have captured the output
        time.sleep(1)
        print("", flush=True, file=sys.stderr)
        f2 = f2.getvalue()
        assert "ThisIsMyCustomActorName" in f2 and "MySubclass" not in f2


if __name__ == "__main__":
    import sys

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
