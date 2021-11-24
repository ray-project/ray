import sys
import random
import string
import os

import ray

import numpy as np
import pytest
import time

from ray.experimental import shuffle
from ray.tests.conftest import _ray_start_chaos_cluster
from ray.data.impl.progress_bar import ProgressBar
from ray._private.test_utils import get_log_message
from ray.exceptions import RayTaskError, ObjectLostError


def assert_no_system_failure(p, timeout):
    # Get all logs for 20 seconds.
    logs = get_log_message(p, timeout=timeout)
    for log in logs:
        assert "SIG" not in log, ("There's the segfault or SIGBART reported.")
        assert "Check failed" not in log, (
            "There's the check failure reported.")


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_chaos_cluster", [{
        "kill_interval": 5,
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
def test_chaos_task_retry(ray_start_chaos_cluster):
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

    # TODO(sang): Enable this again.
    # assert_no_system_failure(p, 10)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "ray_start_chaos_cluster", [{
        "kill_interval": 5,
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
def test_chaos_actor_retry(ray_start_chaos_cluster):
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

    # TODO(sang): Currently, there are lots of SIGBART with
    # plasma client failures. Fix it.
    # assert_no_system_failure(p, 10)


@pytest.fixture
def set_kill_interval(request):
    lineage_reconstruction_enabled, kill_interval = request.param
    if lineage_reconstruction_enabled:
        os.environ["RAY_lineage_pinning_enabled"] = "1"
    request.param = {
        "kill_interval": kill_interval,
        "head_resources": {
            "CPU": 1
        },
        "worker_node_types": {
            "cpu_node": {
                "resources": {
                    "CPU": 2,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 4,
            },
        },
        # We just want the autoscaler to restart nodes, not take them down.
        "idle_timeout_minutes": 10,
    }
    cluster = _ray_start_chaos_cluster(request)
    while True:
        try:
            yield (lineage_reconstruction_enabled, kill_interval, next(cluster))
        except StopIteration:
            break
    if lineage_reconstruction_enabled:
        del os.environ["RAY_lineage_pinning_enabled"]


@pytest.mark.parametrize("set_kill_interval", [(True, None), (True, 30), (False, None), (False, 30)], indirect=True)
def test_nonstreaming_shuffle(set_kill_interval):
    lineage_reconstruction_enabled, kill_interval, _ = set_kill_interval
    try:
        # Create our own tracker so that it gets scheduled onto the head node.
        tracker = shuffle._StatusTracker.remote()
        ray.get(tracker.get_progress.remote())
        assert len(ray.nodes()) == 1, (
            "Tracker actor may have been scheduled to remote node "
            "and may get killed during the test")

        shuffle.run(
            ray_address="auto",
            no_streaming=True,
            num_partitions=200,
            partition_size=10e6,
            tracker=tracker)
    except (RayTaskError, ObjectLostError):
        assert kill_interval is not None


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
