import sys
import random
import string

import ray

import numpy as np
import pytest
import time

from ray.data.impl.progress_bar import ProgressBar
from ray._private.test_utils import get_all_log_message
from ray.util.placement_group import placement_group


def assert_no_system_failure(p, total_lines, timeout):
    # Get logs for 20 seconds.
    logs = get_all_log_message(p, total_lines, timeout=timeout)
    for log in logs:
        assert "SIG" not in log, ("There's the segfault or SIGBART reported.")
        assert "Check failed" not in log, (
            "There's the check failure reported.")


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_chaos_cluster", [{
        "kill_interval": 3,
        "timeout": 45,
        "head_resources": {
            "CPU": 0
        },
        "worker_node_types": {
            "cpu_node": {
                "resources": {
                    "CPU": 8,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 4,
            },
        },
    }],
    indirect=True)
def test_chaos_task_retry(ray_start_chaos_cluster, log_pubsub):
    chaos_test_thread = ray_start_chaos_cluster
    p = log_pubsub
    chaos_test_thread.start()

    # Chaos testing.
    @ray.remote(max_retries=-1)
    def task():
        def generate_data(size_in_kb=10):
            return np.zeros(1024 * size_in_kb, dtype=np.uint8)

        a = ""
        for _ in range(100000):
            a = a + random.choice(string.ascii_letters)
        return generate_data(size_in_kb=50)

    @ray.remote(max_retries=-1)
    def invoke_nested_task():
        time.sleep(0.8)
        return ray.get(task.remote())

    # 50MB of return values.
    TOTAL_TASKS = 300

    pb = ProgressBar("Chaos test sanity check", TOTAL_TASKS)
    results = [invoke_nested_task.remote() for _ in range(TOTAL_TASKS)]
    start = time.time()
    pb.block_until_complete(results)
    runtime_with_failure = time.time() - start
    print(f"Runtime when there are many failures: {runtime_with_failure}")
    pb.close()

    chaos_test_thread.join()
    assert_no_system_failure(p, 10000, 10)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_chaos_cluster", [{
        "kill_interval": 30,
        "timeout": 30,
        "head_resources": {
            "CPU": 0
        },
        "worker_node_types": {
            "cpu_node": {
                "resources": {
                    "CPU": 8,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 4,
            },
        },
    }],
    indirect=True)
def test_chaos_actor_retry(ray_start_chaos_cluster, log_pubsub):
    chaos_test_thread = ray_start_chaos_cluster
    # p = log_pubsub
    chaos_test_thread.start()

    # Chaos testing.
    @ray.remote(num_cpus=1, max_restarts=-1, max_task_retries=-1)
    class Actor:
        def __init__(self):
            self.letter_dict = set()

        def add(self, letter):
            self.letter_dict.add(letter)

        def get(self):
            return self.letter_dict

    NUM_CPUS = 32
    TOTAL_TASKS = 300

    pb = ProgressBar("Chaos test sanity check", TOTAL_TASKS * NUM_CPUS)
    actors = [Actor.remote() for _ in range(NUM_CPUS)]
    results = []
    for a in actors:
        results.extend([a.add.remote(str(i)) for i in range(TOTAL_TASKS)])
    start = time.time()
    pb.fetch_until_complete(results)
    runtime_with_failure = time.time() - start
    print(f"Runtime when there are many failures: {runtime_with_failure}")
    pb.close()
    chaos_test_thread.join()
    # TODO(sang): Currently, there are lots of SIGBART with
    # plasma client failures. Fix it.
    # assert_no_system_failure(p, 10000, 10)


def test_chaos_defer(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        m.setenv("RAY_grpc_based_resource_broadcast", "true")
        # defer for 100s
        m.setenv(
            "RAY_testing_asio_delay_ms",
            "NodeResourceInfoGcsService.grpc_client.UpdateResources=100000")
        m.setenv("RAY_event_stats", "true")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=16, object_store_memory=1e9)
        cluster.wait_for_nodes()
        ray.init(address="auto")
        cluster.add_node(num_cpus=16, num_gpus=1)

        bundle = [{"GPU": 1}, {"CPU": 1}]
        pg = placement_group(bundle)
        ray.get(pg.ready())

        @ray.remote
        def g():
            return "g"

        print(ray.get([g.options(placement_group=pg).remote()]))


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
