import json
import os
import ray
from ray import test_utils
import time
import tqdm

if "SMOKE_TEST" in os.environ:
    MAX_RUNNING_TASKS_IN_CLUSTER = 100
else:
    MAX_RUNNING_TASKS_IN_CLUSTER = 10000

sleep_time = 300

def test_max_running_tasks():
    counter = test_utils.Semaphore.remote(0)
    blocker = test_utils.Semaphore.remote(0)

    cpus_per_task = 0.25

    @ray.remote(num_cpus=cpus_per_task)
    def task(counter, blocker):
        time.sleep(sleep_time)

    refs = [
        task.remote(counter, blocker)
        for _ in tqdm.trange(MAX_RUNNING_TASKS_IN_CLUSTER, desc="Launching tasks")
    ]

    max_cpus = ray.cluster_resources()["CPU"]
    min_cpus_available = max_cpus
    for _ in tqdm.trange(int(sleep_time / 0.1), desc="Waiting"):
        try:
            cur_cpus = ray.available_resources().get("CPU", 0)
            min_cpus_available = min(min_cpus_available, cur_cpus)
        except Exception:
            # There are race conditions `.get` can fail if a new heartbeat
            # comes at the same time.
            pass
        time.sleep(0.1)

    # There are some relevant magic numbers in this check. 10k tasks each
    # require 1/4 cpus. Therefore, ideally 2.5k cpus will be used.
    err_str = f"Only {max_cpus - min_cpus_available}/{max_cpus} cpus used."
    threshold = MAX_RUNNING_TASKS_IN_CLUSTER * cpus_per_task * 0.9
    assert max_cpus - min_cpus_available > threshold, err_str

    for _ in tqdm.trange(
            MAX_RUNNING_TASKS_IN_CLUSTER,
            desc="Ensuring all tasks have finished"):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None

def no_resource_leaks():
    return ray.available_resources() == ray.cluster_resources()


ray.init(address="auto")


test_utils.wait_for_condition(no_resource_leaks)
start_time = time.time()
test_max_running_tasks()
end_time = time.time()
test_utils.wait_for_condition(no_resource_leaks)

rate = MAX_RUNNING_TASKS_IN_CLUSTER / (end_time - start_time - sleep_time)

print(f"Sucess! Started {MAX_RUNNING_TASKS_IN_CLUSTER} tasks in {end_time - start_time}s. ({rate} tasks/s)")

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "tasks_per_second": rate,
        "num_tasks": MAX_RUNNING_TASKS_IN_CLUSTER,
        "time": end_time - start_time,
        "success": "1"
    }
    json.dump(results, out_file)
