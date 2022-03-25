import os
import re
import sys

from datetime import datetime
from collections import defaultdict, Counter
from pathlib import Path
import subprocess
import tempfile
import pytest

import ray
from ray.cross_language import java_actor_class
from ray import ray_constants
from ray._private.test_utils import (
    get_log_batch,
    wait_for_condition,
    init_log_pubsub,
    get_log_message,
    run_string_as_driver,
)


def set_logging_config(monkeypatch, max_bytes, backup_count):
    monkeypatch.setenv("RAY_ROTATION_MAX_BYTES", str(max_bytes))
    monkeypatch.setenv("RAY_ROTATION_BACKUP_COUNT", str(backup_count))


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


def test_log_rotation(shutdown_only, monkeypatch):
    max_bytes = 1
    backup_count = 3
    set_logging_config(monkeypatch, max_bytes, backup_count)
    ray.init(num_cpus=1)
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
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

    ray.get(f.remote())

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
        assert component_exist(component, paths)
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
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
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
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
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


def test_log_monitor_backpressure(ray_start_cluster, monkeypatch):
    update_interval = 3
    monkeypatch.setenv("LOG_NAME_UPDATE_INTERVAL_S", str(update_interval))
    # Intentionally set low to trigger the backpressure condition.
    monkeypatch.setenv("RAY_LOG_MONITOR_MANY_FILES_THRESHOLD", "1")
    expected_str = "abcxyz"

    def matcher(line):
        return line == expected_str

    # Test log monitor still works with backpressure.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    # Connect a driver to the Ray cluster.
    ray.init(address=cluster.address)
    p = init_log_pubsub()

    @ray.remote
    class Actor:
        def print(self):
            print(expected_str)

    now = datetime.now()
    a = Actor.remote()
    ray.get(a.print.remote())
    logs = get_log_message(p, 1, matcher=matcher)
    assert logs[0][0] == expected_str
    # Since the log file update is delayed,
    # it should take more than update_interval
    # to publish a message for a new worker.
    assert (datetime.now() - now).seconds >= update_interval

    now = datetime.now()
    a = Actor.remote()
    ray.get(a.print.remote())
    logs = get_log_message(p, 1, matcher=matcher)
    assert logs[0][0] == expected_str
    assert (datetime.now() - now).seconds >= update_interval


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

session_dir = ray.worker.global_worker.node.address_info["session_dir"]
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


if __name__ == "__main__":
    import sys

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
