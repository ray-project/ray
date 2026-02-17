import os
import shutil
import sys
import time
import uuid

import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import check_call_ray


def unix_socket_create_path(name):
    unix = sys.platform != "win32"
    return (
        os.path.join(ray._common.utils.get_default_ray_temp_dir(), name)
        if unix
        else None
    )


def unix_socket_verify(unix_socket):
    if sys.platform != "win32":
        assert os.path.exists(unix_socket), "Socket not found: " + unix_socket


def unix_socket_delete(unix_socket):
    unix = sys.platform != "win32"
    return os.remove(unix_socket) if unix else None


@pytest.fixture
def delete_default_temp_dir():
    def delete_default_temp_dir_once():
        shutil.rmtree(ray._common.utils.get_default_ray_temp_dir(), ignore_errors=True)
        return not os.path.exists(ray._common.utils.get_default_ray_temp_dir())

    wait_for_condition(delete_default_temp_dir_once)
    yield


def test_tempdir_created_successfully(delete_default_temp_dir, shutdown_only):
    temp_dir = os.path.join(
        "/tmp/test", uuid.uuid4().hex[:-10]
    )  # truncate the uuid to avoid the socket path length limit
    ray.init(_temp_dir=temp_dir)
    assert os.path.exists(temp_dir), "Specified temp dir not found."
    assert not os.path.exists(
        ray._common.utils.get_default_ray_temp_dir()
    ), "Default temp dir should not exist."
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_tempdir_commandline(delete_default_temp_dir):
    temp_dir = os.path.join(
        "/tmp/test", uuid.uuid4().hex[:-10]
    )  # truncate the uuid to avoid the socket path length limit
    check_call_ray(
        [
            "start",
            "--head",
            "--temp-dir=" + temp_dir,
            "--port",
            "0",
        ]
    )
    assert os.path.exists(temp_dir), "Specified temp dir not found."
    assert not os.path.exists(
        ray._common.utils.get_default_ray_temp_dir()
    ), "Default temp dir should not exist."
    check_call_ray(["stop"])
    shutil.rmtree(
        temp_dir,
        ignore_errors=True,
    )


def test_tempdir_long_path():
    if sys.platform != "win32":
        # Test AF_UNIX limits for sockaddr_un->sun_path on POSIX OSes
        maxlen = 104 if sys.platform.startswith("darwin") else 108
        temp_dir = os.path.join(
            ray._common.utils.get_default_ray_temp_dir(), "z" * maxlen
        )
        with pytest.raises(OSError):
            ray.init(_temp_dir=temp_dir)  # path should be too long


def test_raylet_tempfiles(shutdown_only):
    expected_socket_files = (
        {"plasma_store", "raylet"} if sys.platform != "win32" else set()
    )

    ray.init(num_cpus=0)
    node = ray._private.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    log_files_expected = {
        "log_monitor.log",
        "monitor.log",
        "raylet.out",
        "raylet.err",
        "gcs_server.out",
        "gcs_server.err",
        "dashboard.log",
        "dashboard_agent.log",
    }

    def check_all_log_file_exists():
        log_files = set(os.listdir(node.get_logs_dir_path()))
        for expected in log_files_expected:
            if expected not in log_files:
                raise RuntimeError(f"File {expected} not found!")
        return True

    wait_for_condition(check_all_log_file_exists)
    # Get the list of log files again since the previous one
    # might have the stale information.
    log_files = set(os.listdir(node.get_logs_dir_path()))
    assert log_files_expected.issubset(log_files)
    assert log_files.issuperset(log_files_expected)

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files.issuperset(expected_socket_files)
    ray.shutdown()

    ray.init(num_cpus=2)
    node = ray._private.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    time.sleep(3)  # wait workers to start
    log_files = set(os.listdir(node.get_logs_dir_path()))

    assert log_files.issuperset(log_files_expected)

    # Check numbers of worker log file.
    assert sum(1 for filename in log_files if filename.startswith("worker")) == 4

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files.issuperset(expected_socket_files)


def test_tempdir_privilege(shutdown_only):
    tmp_dir = ray._common.utils.get_default_ray_temp_dir()
    os.makedirs(tmp_dir, exist_ok=True)
    os.chmod(tmp_dir, 0o000)
    ray.init(num_cpus=1)
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    assert os.path.exists(session_dir), "Specified socket path not found."


def test_session_dir_uniqueness():
    session_dirs = set()
    for i in range(2):
        ray.init(num_cpus=1)
        session_dirs.add(ray._private.worker._global_node.get_session_dir_path)
        ray.shutdown()
    assert len(session_dirs) == 2


def test_head_temp_dir_shared_with_worker(delete_default_temp_dir):
    """Test that head node temp_dir is shared with worker node when only head temp_dir is specified."""
    head_temp_dir = os.path.join(
        ray._common.utils.get_default_ray_temp_dir(),
        uuid.uuid4().hex[
            :-10
        ],  # truncate the uuid to avoid the socket path length limit
    )

    # Start head node with temp-dir specified
    check_call_ray(
        [
            "start",
            "--head",
            f"--temp-dir={head_temp_dir}",
            "--port",
            str(ray_constants.DEFAULT_PORT),
        ]
    )

    # Start worker node without specifying temp-dir
    check_call_ray(
        [
            "start",
            f"--address=127.0.0.1:{ray_constants.DEFAULT_PORT}",
        ]
    )

    try:
        # Connect to the cluster
        ray.init(address="auto")

        # Verify both head and worker nodes exist
        wait_for_condition(lambda: len(ray.nodes()) == 2, timeout=10)
        nodes = ray.nodes()
        assert len(nodes) == 2, "Expected 2 nodes in the cluster"

        # Check that both nodes' temp directories are under the head's temp_dir
        # The session_latest should exist in head_temp_dir
        assert os.path.isdir(
            os.path.join(head_temp_dir, "session_latest")
        ), "Head node session directory not found in specified temp_dir"

        # Both nodes should be using the same temp directory (head's temp_dir)
        assert os.path.isfile(
            os.path.join(head_temp_dir, "ray_current_cluster")
        ), "Cluster info file not found in head temp_dir"

        ray.shutdown()
    finally:
        check_call_ray(["stop"])
        shutil.rmtree(head_temp_dir, ignore_errors=True)


def test_worker_temp_dir_different_from_head(delete_default_temp_dir):
    """Test that worker node can have a different temp_dir when only worker temp_dir is specified."""
    worker_temp_dir = os.path.join(
        ray._common.utils.get_default_ray_temp_dir(),
        uuid.uuid4().hex[
            :-10
        ],  # truncate the uuid to avoid the socket path length limit
    )

    # Start head node without specifying temp-dir
    check_call_ray(
        [
            "start",
            "--head",
            "--port",
            str(ray_constants.DEFAULT_PORT),
        ]
    )

    # Start worker node with temp-dir specified
    check_call_ray(
        [
            "start",
            f"--address=127.0.0.1:{ray_constants.DEFAULT_PORT}",
            f"--temp-dir={worker_temp_dir}",
        ]
    )

    try:
        # Connect to the cluster
        ray.init(address="auto")

        # Verify both head and worker nodes exist
        wait_for_condition(lambda: len(ray.nodes()) == 2, timeout=10)
        nodes = ray.nodes()
        assert len(nodes) == 2, "Expected 2 nodes in the cluster"

        # Verify worker temp-dir was created at the specified location
        assert os.path.isfile(
            os.path.join(worker_temp_dir, "ray_current_cluster")
        ), "Worker cluster info file not found in specified temp_dir"
        assert os.path.isdir(
            os.path.join(worker_temp_dir, "session_latest")
        ), "Worker session directory not found in specified temp_dir"

        # Verify head node is using default temp_dir (different from worker)
        default_head_temp_dir = ray._common.utils.get_default_ray_temp_dir()
        assert os.path.exists(
            default_head_temp_dir
        ), "Head node should be using default temp_dir"

        # Ensure they are different directories
        assert (
            worker_temp_dir != default_head_temp_dir
        ), "Worker temp_dir should be different from head node's default temp_dir"

        ray.shutdown()
    finally:
        check_call_ray(["stop"])
        shutil.rmtree(worker_temp_dir, ignore_errors=True)


def test_both_nodes_different_temp_dirs(delete_default_temp_dir):
    """Test that head and worker can have different temp_dirs when both are specified."""
    head_temp_dir = os.path.join(
        ray._common.utils.get_default_ray_temp_dir(),
        uuid.uuid4().hex[
            :-10
        ],  # truncate the uuid to avoid the socket path length limit
    )
    worker_temp_dir = os.path.join(
        ray._common.utils.get_default_ray_temp_dir(),
        uuid.uuid4().hex[
            :-10
        ],  # truncate the uuid to avoid the socket path length limit
    )

    # Ensure directories are different
    assert (
        head_temp_dir != worker_temp_dir
    ), "Test setup error: directories should be different"

    # Start head node with temp-dir specified
    check_call_ray(
        [
            "start",
            "--head",
            f"--temp-dir={head_temp_dir}",
            "--port",
            str(ray_constants.DEFAULT_PORT),
        ]
    )

    # Start worker node with a different temp-dir specified
    check_call_ray(
        [
            "start",
            f"--address=127.0.0.1:{ray_constants.DEFAULT_PORT}",
            f"--temp-dir={worker_temp_dir}",
        ]
    )

    try:
        # Connect to the cluster
        ray.init(address="auto")

        # Verify both head and worker nodes exist
        wait_for_condition(lambda: len(ray.nodes()) == 2, timeout=10)
        nodes = ray.nodes()
        assert len(nodes) == 2, "Expected 2 nodes in the cluster"

        # Verify head temp-dir was created at the specified location
        assert os.path.isfile(
            os.path.join(head_temp_dir, "ray_current_cluster")
        ), "Head cluster info file not found in specified temp_dir"
        assert os.path.isdir(
            os.path.join(head_temp_dir, "session_latest")
        ), "Head session directory not found in specified temp_dir"

        # Verify worker temp-dir was created at the specified location
        assert os.path.isfile(
            os.path.join(worker_temp_dir, "ray_current_cluster")
        ), "Worker cluster info file not found in specified temp_dir"
        assert os.path.isdir(
            os.path.join(worker_temp_dir, "session_latest")
        ), "Worker session directory not found in specified temp_dir"

        # Verify both directories exist and are different
        assert (
            head_temp_dir != worker_temp_dir
        ), "Head and worker temp_dirs should be different"
        assert os.path.exists(head_temp_dir), "Head temp_dir should exist"
        assert os.path.exists(worker_temp_dir), "Worker temp_dir should exist"

        ray.shutdown()
    finally:
        check_call_ray(["stop"])
        shutil.rmtree(head_temp_dir, ignore_errors=True)
        shutil.rmtree(worker_temp_dir, ignore_errors=True)


def test_resolve_user_ray_temp_dir_from_gcs(delete_default_temp_dir):
    """Test that resolve_user_ray_temp_dir correctly retrieves temp_dir from GCS.

    This test verifies that resolve_user_ray_temp_dir can correctly fetch temp_dir
    from GCS node info both before and after ray.init() is called.
    """
    import ray._common.utils

    head_temp_dir = os.path.join(
        ray._common.utils.get_default_ray_temp_dir(),
        uuid.uuid4().hex[
            :-10
        ],  # truncate the uuid to avoid the socket path length limit
    )

    # Start head node with temp-dir specified
    check_call_ray(
        [
            "start",
            "--head",
            f"--temp-dir={head_temp_dir}",
            "--port",
            str(ray_constants.DEFAULT_PORT),
        ]
    )

    try:
        ray.init(address="auto")
        wait_for_condition(lambda: len(ray.nodes()) == 1, timeout=10)
        nodes = ray.nodes()
        assert len(nodes) == 1, "Expected 1 node in the cluster"
        node_id = nodes[0]["NodeID"]
        gcs_client = ray._private.worker.global_worker.gcs_client
        ray.shutdown()

        # test WITHOUT ray.init() (fetch temp_dir from GCS)
        resolved_temp_dir = ray._common.utils.resolve_user_ray_temp_dir(
            gcs_client, node_id=node_id
        )
        assert resolved_temp_dir == head_temp_dir, (
            f"Expected temp_dir from GCS to be {head_temp_dir}, "
            f"but got {resolved_temp_dir}"
        )

        # test WITH ray.init() (fetch temp_dir from runtime context)
        ray.init(address="auto")
        resolved_temp_dir = ray._common.utils.resolve_user_ray_temp_dir(
            gcs_client, node_id=node_id
        )
        assert resolved_temp_dir == head_temp_dir, (
            f"Expected temp_dir from runtime context to be {head_temp_dir}, "
            f"but got {resolved_temp_dir}"
        )

        ray.shutdown()
    finally:
        check_call_ray(["stop"])
        shutil.rmtree(head_temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
