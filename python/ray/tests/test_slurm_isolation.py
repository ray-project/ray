"""
Tests for Slurm isolation feature using --isolation-id.

This test file verifies that the isolation-id feature solves three key issues
when running multiple Ray clusters on the same node (common in Slurm environments):

1. Log/Temp directory isolation - Different clusters use different directories
2. ray stop isolation - Can stop only processes from a specific cluster
   (via matching the isolation temp directory path in process command lines)
3. Port conflict avoidance - Different clusters use different temp directories
   (port management is handled by the isolated directory structure)

Usage Examples:
---------------

# Start an isolated Ray cluster (manually specified)
ray start --head --isolation-id=my_job_123

# Start an isolated Ray cluster (auto-detect in Slurm environment)
export SLURM_JOB_ID=12345
ray start --head  # Automatically uses isolation-id=12345

# Stop only a specific cluster
ray stop --isolation-id=my_job_123

# In Slurm environment, auto-stop current job's cluster
ray stop  # Automatically uses SLURM_JOB_ID
"""

import os
import shutil
import sys
import time
import uuid

import pytest

import ray
import ray._common.utils
from ray._private.test_utils import check_call_ray

import psutil


@pytest.fixture
def cleanup_ray():
    """Fixture to ensure Ray is stopped before and after tests."""
    # Stop any existing Ray processes
    try:
        check_call_ray(["stop", "--force"])
    except Exception:
        pass
    yield
    # Cleanup after test
    try:
        check_call_ray(["stop", "--force"])
    except Exception:
        pass


class TestMultiNodeCluster:
    """
    Test isolation with multi-node clusters (head + worker nodes).

    These tests verify that:
    1. A cluster with 1 head + 2 workers can be properly isolated
    2. Two multi-node clusters can run simultaneously and be stopped independently
    3. Stopping all clusters leaves no residual Ray processes
    """

    def _get_pids_by_temp_dir(self, temp_dir):
        """Get PIDs of processes that have temp_dir in their command line."""
        pids = set()
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                cmdline = " ".join(proc.cmdline() or [])
                if temp_dir in cmdline:
                    pids.add(proc.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return pids

    def test_multi_node_cluster_with_workers(self, cleanup_ray):
        """
        Test that a cluster with 1 head + 2 workers can be isolated and stopped.

        Scenario:
        - Start 1 head node with isolation-id
        - Start 2 worker nodes connecting to the head
        - Use ray.nodes() to verify exactly 3 nodes are in the cluster
        - Stop the cluster and verify all processes are terminated
        """
        isolation_id = f"multinode_{uuid.uuid4().hex[:8]}"
        base_temp_dir = ray._common.utils.get_ray_temp_dir()
        temp_dir = os.path.join(base_temp_dir, f"isolation_{isolation_id}")

        try:
            # Start head node with a fixed port for workers to connect
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id}",
                    "--port",
                    "6380",
                ]
            )
            time.sleep(2)

            # Start worker node 1
            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id}",
                    "--address=127.0.0.1:6380",
                ]
            )
            time.sleep(1)

            # Start worker node 2
            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id}",
                    "--address=127.0.0.1:6380",
                ]
            )
            time.sleep(2)

            # Use ray.nodes() to verify the cluster has exactly 3 nodes
            ray.init(address="127.0.0.1:6380")
            try:
                nodes = ray.nodes()
                alive_nodes = [n for n in nodes if n.get("Alive", False)]
                assert (
                    len(alive_nodes) == 3
                ), f"Expected 3 nodes (1 head + 2 workers), got {len(alive_nodes)}"

                # Define and run a simple task
                @ray.remote
                def add(a, b):
                    return a + b

                result = ray.get(add.remote(1, 2))
                assert result == 3, f"Task result should be 3, got {result}"

                # Define and use a simple actor
                @ray.remote
                class Counter:
                    def __init__(self):
                        self.value = 0

                    def increment(self):
                        self.value += 1
                        return self.value

                    def get_value(self):
                        return self.value

                counter = Counter.remote()
                assert ray.get(counter.increment.remote()) == 1
                assert ray.get(counter.increment.remote()) == 2
                assert ray.get(counter.get_value.remote()) == 2
            finally:
                ray.shutdown()

            # Get all PIDs for cleanup verification
            cluster_pids = self._get_pids_by_temp_dir(temp_dir)

            # Stop the cluster using isolation-id
            check_call_ray(["stop", f"--isolation-id={isolation_id}"])
            time.sleep(3)

            # Verify all cluster processes are stopped
            for pid in cluster_pids:
                assert not psutil.pid_exists(pid), f"PID {pid} should have been stopped"

        finally:
            check_call_ray(["stop", "--force"])
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_two_multi_node_clusters_stop_either(self, cleanup_ray):
        """
        Test that two multi-node clusters can be stopped independently.

        Scenario:
        - Start cluster 1: 1 head + 1 worker (isolation_id_1)
        - Start cluster 2: 1 head + 1 worker (isolation_id_2)
        - Stop cluster 2
        - Verify cluster 1 is still running
        - Stop cluster 1
        - Verify all processes are stopped
        """
        isolation_id_1 = f"cluster1_{uuid.uuid4().hex[:8]}"
        isolation_id_2 = f"cluster2_{uuid.uuid4().hex[:8]}"
        base_temp_dir = ray._common.utils.get_ray_temp_dir()
        temp_dir_1 = os.path.join(base_temp_dir, f"isolation_{isolation_id_1}")
        temp_dir_2 = os.path.join(base_temp_dir, f"isolation_{isolation_id_2}")

        try:
            # === Start Cluster 1 (head + 1 worker) ===
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_1}",
                    "--port",
                    "6381",
                ]
            )
            time.sleep(2)

            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id_1}",
                    "--address=127.0.0.1:6381",
                ]
            )
            time.sleep(2)

            # === Start Cluster 2 (head + 1 worker) ===
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_2}",
                    "--port",
                    "6382",
                ]
            )
            time.sleep(2)

            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id_2}",
                    "--address=127.0.0.1:6382",
                ]
            )
            time.sleep(2)

            # Get PIDs for both clusters
            cluster1_pids = self._get_pids_by_temp_dir(temp_dir_1)
            cluster2_pids = self._get_pids_by_temp_dir(temp_dir_2)

            assert (
                len(cluster1_pids) >= 2
            ), f"Cluster 1 should have processes, found {len(cluster1_pids)}"
            assert (
                len(cluster2_pids) >= 2
            ), f"Cluster 2 should have processes, found {len(cluster2_pids)}"

            # Verify both clusters are running
            for pid in cluster1_pids:
                assert psutil.pid_exists(pid), f"Cluster 1 PID {pid} should exist"
            for pid in cluster2_pids:
                assert psutil.pid_exists(pid), f"Cluster 2 PID {pid} should exist"

            # === Stop Cluster 2 first (test stopping the second one) ===
            check_call_ray(["stop", f"--isolation-id={isolation_id_2}"])
            time.sleep(3)

            # Cluster 2 should be stopped
            for pid in cluster2_pids:
                assert not psutil.pid_exists(
                    pid
                ), f"Cluster 2 PID {pid} should have been stopped"

            # Cluster 1 should still be running
            for pid in cluster1_pids:
                assert psutil.pid_exists(
                    pid
                ), f"Cluster 1 PID {pid} should still be running after stopping Cluster 2"

            # === Now stop Cluster 1 ===
            check_call_ray(["stop", f"--isolation-id={isolation_id_1}"])
            time.sleep(3)

            # Cluster 1 should be stopped
            for pid in cluster1_pids:
                assert not psutil.pid_exists(
                    pid
                ), f"Cluster 1 PID {pid} should have been stopped"

        finally:
            check_call_ray(["stop", "--force"])
            shutil.rmtree(temp_dir_1, ignore_errors=True)
            shutil.rmtree(temp_dir_2, ignore_errors=True)

    def test_stop_all_clusters_no_residual_processes(self, cleanup_ray):
        """
        Test that stopping all clusters leaves no residual Ray processes.

        Scenario:
        - Start cluster 1: 1 head + 1 worker
        - Start cluster 2: 1 head + 1 worker
        - Stop both clusters using their isolation-ids
        - Verify zero Ray processes remain on the system
        """
        isolation_id_1 = f"cluster1_{uuid.uuid4().hex[:8]}"
        isolation_id_2 = f"cluster2_{uuid.uuid4().hex[:8]}"
        base_temp_dir = ray._common.utils.get_ray_temp_dir()
        temp_dir_1 = os.path.join(base_temp_dir, f"isolation_{isolation_id_1}")
        temp_dir_2 = os.path.join(base_temp_dir, f"isolation_{isolation_id_2}")

        try:
            # Start Cluster 1
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_1}",
                    "--port",
                    "6383",
                ]
            )
            time.sleep(2)

            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id_1}",
                    "--address=127.0.0.1:6383",
                ]
            )
            time.sleep(2)

            # Start Cluster 2
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_2}",
                    "--port",
                    "6384",
                ]
            )
            time.sleep(2)

            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id_2}",
                    "--address=127.0.0.1:6384",
                ]
            )
            time.sleep(2)

            # Verify both clusters have processes
            cluster1_pids = self._get_pids_by_temp_dir(temp_dir_1)
            cluster2_pids = self._get_pids_by_temp_dir(temp_dir_2)
            all_cluster_pids = cluster1_pids | cluster2_pids

            assert (
                len(all_cluster_pids) >= 4
            ), f"Expected at least 4 total processes, found {len(all_cluster_pids)}"

            # Stop both clusters
            check_call_ray(["stop", f"--isolation-id={isolation_id_1}"])
            check_call_ray(["stop", f"--isolation-id={isolation_id_2}"])
            time.sleep(3)

            # Verify all cluster processes are stopped
            for pid in all_cluster_pids:
                assert not psutil.pid_exists(pid), f"PID {pid} should have been stopped"

            # Double-check: no Ray processes with these temp dirs should exist
            remaining_pids_1 = self._get_pids_by_temp_dir(temp_dir_1)
            remaining_pids_2 = self._get_pids_by_temp_dir(temp_dir_2)

            assert (
                len(remaining_pids_1) == 0
            ), f"No processes should remain for cluster 1, found {remaining_pids_1}"
            assert (
                len(remaining_pids_2) == 0
            ), f"No processes should remain for cluster 2, found {remaining_pids_2}"

        finally:
            check_call_ray(["stop", "--force"])
            shutil.rmtree(temp_dir_1, ignore_errors=True)
            shutil.rmtree(temp_dir_2, ignore_errors=True)


class TestIsolatedClusterWorkloads:
    """
    Test that Ray tasks and actors work correctly on isolated clusters.

    These tests verify that:
    1. Tasks and actors can run on an isolated cluster
    2. Multiple isolated clusters can run independent workloads simultaneously
    3. Temp directories are truly isolated (no cross-cluster interference)
    """

    def _get_pids_by_temp_dir(self, temp_dir):
        """Get PIDs of processes that have temp_dir in their command line."""
        pids = set()
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                cmdline = " ".join(proc.cmdline() or [])
                if temp_dir in cmdline:
                    pids.add(proc.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return pids

    def test_ray_task_and_actor_on_isolated_cluster(self, cleanup_ray):
        """
        Test that Ray tasks and actors work on an isolated cluster.

        Scenario:
        - Start an isolated cluster (1 head + 1 worker)
        - Submit a simple Ray task
        - Create a Ray actor and call its method
        - Verify both execute correctly
        """
        isolation_id = f"workload_{uuid.uuid4().hex[:8]}"
        base_temp_dir = ray._common.utils.get_ray_temp_dir()
        temp_dir = os.path.join(base_temp_dir, f"isolation_{isolation_id}")

        try:
            # Start isolated cluster
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id}",
                    "--port",
                    "6385",
                ]
            )
            time.sleep(2)

            check_call_ray(
                [
                    "start",
                    f"--isolation-id={isolation_id}",
                    "--address=127.0.0.1:6385",
                ]
            )
            time.sleep(2)

            # Connect to the cluster
            ray.init(address="127.0.0.1:6385")
            try:
                # Define and run a simple task
                @ray.remote
                def add(a, b):
                    return a + b

                result = ray.get(add.remote(1, 2))
                assert result == 3, f"Task result should be 3, got {result}"

                # Define and use a simple actor
                @ray.remote
                class Counter:
                    def __init__(self):
                        self.value = 0

                    def increment(self):
                        self.value += 1
                        return self.value

                    def get_value(self):
                        return self.value

                counter = Counter.remote()
                assert ray.get(counter.increment.remote()) == 1
                assert ray.get(counter.increment.remote()) == 2
                assert ray.get(counter.get_value.remote()) == 2

            finally:
                ray.shutdown()

            # Stop the cluster
            check_call_ray(["stop", f"--isolation-id={isolation_id}"])

        finally:
            check_call_ray(["stop", "--force"])
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_two_clusters_run_independent_workloads(self, cleanup_ray):
        """
        Test that two isolated clusters can run independent workloads simultaneously.

        Scenario:
        - Start cluster 1 and cluster 2 with different isolation-ids
        - Submit tasks to both clusters concurrently
        - Verify each cluster processes its own tasks independently
        - Verify temp directories are truly isolated
        """
        isolation_id_1 = f"cluster1_{uuid.uuid4().hex[:8]}"
        isolation_id_2 = f"cluster2_{uuid.uuid4().hex[:8]}"
        base_temp_dir = ray._common.utils.get_ray_temp_dir()
        temp_dir_1 = os.path.join(base_temp_dir, f"isolation_{isolation_id_1}")
        temp_dir_2 = os.path.join(base_temp_dir, f"isolation_{isolation_id_2}")

        try:
            # Start Cluster 1
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_1}",
                    "--port",
                    "6386",
                ]
            )
            time.sleep(2)

            # Start Cluster 2
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_2}",
                    "--port",
                    "6387",
                ]
            )
            time.sleep(2)

            # Verify both temp directories exist and are different
            assert os.path.exists(temp_dir_1), "Cluster 1 temp dir should exist"
            assert os.path.exists(temp_dir_2), "Cluster 2 temp dir should exist"
            assert temp_dir_1 != temp_dir_2, "Clusters should have different temp dirs"

            # Run workload on Cluster 1
            ray.init(address="127.0.0.1:6386")
            try:
                # Define and run a simple task
                @ray.remote
                def add(a, b):
                    return a + b

                result = ray.get(add.remote(1, 2))
                assert result == 3, f"Task result should be 3, got {result}"

                # Define and use a simple actor
                @ray.remote
                class Counter:
                    def __init__(self):
                        self.value = 0

                    def increment(self):
                        self.value += 1
                        return self.value

                    def get_value(self):
                        return self.value

                counter = Counter.remote()
                assert ray.get(counter.increment.remote()) == 1
                assert ray.get(counter.increment.remote()) == 2
                assert ray.get(counter.get_value.remote()) == 2
            finally:
                ray.shutdown()

            # Run workload on Cluster 2
            ray.init(address="127.0.0.1:6387")
            try:
                # Define and run a simple task
                @ray.remote
                def add(a, b):
                    return a + b

                result = ray.get(add.remote(3, 4))
                assert result == 7, f"Task result should be 7, got {result}"

                # Define and use a simple actor
                @ray.remote
                class Counter:
                    def __init__(self):
                        self.value = 0

                    def increment(self):
                        self.value += 1
                        return self.value

                    def get_value(self):
                        return self.value

                counter = Counter.remote()
                assert ray.get(counter.increment.remote()) == 1
                assert ray.get(counter.increment.remote()) == 2
                assert ray.get(counter.get_value.remote()) == 2
            finally:
                ray.shutdown()

            # Verify the session directories are different (isolation proof)
            # Note: RAY_SESSION_DIR may not be set in all cases, so we also
            # verify via the filesystem
            session_latest_1 = os.path.join(temp_dir_1, "session_latest")
            session_latest_2 = os.path.join(temp_dir_2, "session_latest")

            if os.path.exists(session_latest_1) and os.path.exists(session_latest_2):
                real_session_1 = os.path.realpath(session_latest_1)
                real_session_2 = os.path.realpath(session_latest_2)
                assert (
                    real_session_1 != real_session_2
                ), "Clusters should have different session directories"

            # Verify logs are in separate directories
            logs_dir_1 = (
                os.path.join(os.path.realpath(session_latest_1), "logs")
                if os.path.exists(session_latest_1)
                else None
            )
            logs_dir_2 = (
                os.path.join(os.path.realpath(session_latest_2), "logs")
                if os.path.exists(session_latest_2)
                else None
            )

            if logs_dir_1 and logs_dir_2:
                assert (
                    logs_dir_1 != logs_dir_2
                ), "Logs should be in different directories"

            # Stop both clusters
            check_call_ray(["stop", f"--isolation-id={isolation_id_1}"])
            check_call_ray(["stop", f"--isolation-id={isolation_id_2}"])

        finally:
            check_call_ray(["stop", "--force"])
            shutil.rmtree(temp_dir_1, ignore_errors=True)
            shutil.rmtree(temp_dir_2, ignore_errors=True)

    def test_temp_dir_isolation_with_file_operations(self, cleanup_ray):
        """
        Test that temp directory isolation prevents cross-cluster file interference.

        Scenario:
        - Start two clusters with different isolation-ids
        - Each cluster writes a file to its session directory via a task
        - Verify each cluster can only see its own files
        """
        isolation_id_1 = f"filetest1_{uuid.uuid4().hex[:8]}"
        isolation_id_2 = f"filetest2_{uuid.uuid4().hex[:8]}"
        base_temp_dir = ray._common.utils.get_ray_temp_dir()
        temp_dir_1 = os.path.join(base_temp_dir, f"isolation_{isolation_id_1}")
        temp_dir_2 = os.path.join(base_temp_dir, f"isolation_{isolation_id_2}")

        try:
            # Start Cluster 1
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_1}",
                    "--port",
                    "6388",
                ]
            )
            time.sleep(2)

            # Start Cluster 2
            check_call_ray(
                [
                    "start",
                    "--head",
                    f"--isolation-id={isolation_id_2}",
                    "--port",
                    "6389",
                ]
            )
            time.sleep(2)

            # Run workload on Cluster 1
            ray.init(address="127.0.0.1:6388")
            try:
                # Define and run a simple task
                @ray.remote
                def add(a, b):
                    return a + b

                result = ray.get(add.remote(1, 2))
                assert result == 3, f"Task result should be 3, got {result}"

                # Define and use a simple actor
                @ray.remote
                class Counter:
                    def __init__(self):
                        self.value = 0

                    def increment(self):
                        self.value += 1
                        return self.value

                    def get_value(self):
                        return self.value

                counter = Counter.remote()
                assert ray.get(counter.increment.remote()) == 1
                assert ray.get(counter.increment.remote()) == 2
                assert ray.get(counter.get_value.remote()) == 2
            finally:
                ray.shutdown()

            # Run workload on Cluster 2
            ray.init(address="127.0.0.1:6389")
            try:
                # Define and run a simple task
                @ray.remote
                def add(a, b):
                    return a + b

                result = ray.get(add.remote(3, 4))
                assert result == 7, f"Task result should be 7, got {result}"

                # Define and use a simple actor
                @ray.remote
                class Counter:
                    def __init__(self):
                        self.value = 0

                    def increment(self):
                        self.value += 1
                        return self.value

                    def get_value(self):
                        return self.value

                counter = Counter.remote()
                assert ray.get(counter.increment.remote()) == 1
                assert ray.get(counter.increment.remote()) == 2
                assert ray.get(counter.get_value.remote()) == 2
            finally:
                ray.shutdown()

            # Verify session directories are isolated
            session_1 = os.path.realpath(os.path.join(temp_dir_1, "session_latest"))
            session_2 = os.path.realpath(os.path.join(temp_dir_2, "session_latest"))
            assert session_1 != session_2, "Session directories should be different"

            # The isolation is proven by the directory structure:
            # /tmp/ray/isolation_{id1}/session_xxx vs /tmp/ray/isolation_{id2}/session_yyy
            assert (
                isolation_id_1 in session_1
            ), "Session 1 should contain isolation_id_1"
            assert (
                isolation_id_2 in session_2
            ), "Session 2 should contain isolation_id_2"
            assert (
                isolation_id_1 not in session_2
            ), "Session 2 should not contain isolation_id_1"
            assert (
                isolation_id_2 not in session_1
            ), "Session 1 should not contain isolation_id_2"

        finally:
            check_call_ray(["stop", "--force"])
            shutil.rmtree(temp_dir_1, ignore_errors=True)
            shutil.rmtree(temp_dir_2, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
