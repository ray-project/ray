import click
import json
import os
import ray
import ray._private.test_utils as test_utils
import time
import tqdm

from ray.experimental.state.api import summarize_tasks
from dashboard_test import DashboardTestAtScale
from ray._private.state_api_test_utils import (
    StateAPICallSpec,
    periodic_invoke_state_apis_with_actor,
    summarize_worker_startup_time,
)

sleep_time = 300


def test_max_running_tasks(num_tasks):
    cpus_per_task = 0.25

    @ray.remote(num_cpus=cpus_per_task)
    def task():
        time.sleep(sleep_time)

    def time_up(start_time):
        return time.time() - start_time >= sleep_time

    refs = [task.remote() for _ in tqdm.trange(num_tasks, desc="Launching tasks")]

    max_cpus = ray.cluster_resources()["CPU"]
    min_cpus_available = max_cpus
    start_time = time.time()
    for _ in tqdm.trange(int(sleep_time / 0.1), desc="Waiting"):
        try:
            cur_cpus = ray.available_resources().get("CPU", 0)
            min_cpus_available = min(min_cpus_available, cur_cpus)
        except Exception:
            # There are race conditions `.get` can fail if a new heartbeat
            # comes at the same time.
            pass
        if time_up(start_time):
            print(f"Time up for sleeping {sleep_time} seconds")
            break
        time.sleep(0.1)

    # There are some relevant magic numbers in this check. 10k tasks each
    # require 1/4 cpus. Therefore, ideally 2.5k cpus will be used.
    used_cpus = max_cpus - min_cpus_available
    err_str = f"Only {used_cpus}/{max_cpus} cpus used."
    # 1500 tasks. Note that it is a pretty low threshold, and the
    # performance should be tracked via perf dashboard.
    threshold = num_tasks * cpus_per_task * 0.60
    print(f"{used_cpus}/{max_cpus} used.")
    assert used_cpus > threshold, err_str

    for _ in tqdm.trange(num_tasks, desc="Ensuring all tasks have finished"):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None

    return used_cpus


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


@click.command()
@click.option("--num-tasks", required=True, type=int, help="Number of tasks to launch.")
@click.option(
    "--smoke-test",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, it's a smoke test",
)
def test(num_tasks, smoke_test):
    addr = ray.init(address="auto")

    test_utils.wait_for_condition(no_resource_leaks)
    monitor_actor = test_utils.monitor_memory_usage()
    dashboard_test = DashboardTestAtScale(addr)

    def not_none(res):
        return res is not None

    api_caller = periodic_invoke_state_apis_with_actor(
        apis=[StateAPICallSpec(summarize_tasks, not_none)],
        call_interval_s=4,
        print_result=True,
    )

    start_time = time.time()
    used_cpus = test_max_running_tasks(num_tasks)
    end_time = time.time()
    ray.get(monitor_actor.stop_run.remote())
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")
    ray.get(api_caller.stop.remote())

    del api_caller
    del monitor_actor
    test_utils.wait_for_condition(no_resource_leaks)

    try:
        summarize_worker_startup_time()
    except Exception as e:
        print("Failed to summarize worker startup time.")
        print(e)

    rate = num_tasks / (end_time - start_time - sleep_time)
    print(
        f"Success! Started {num_tasks} tasks in {end_time - start_time}s. "
        f"({rate} tasks/s)"
    )

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "tasks_per_second": rate,
            "num_tasks": num_tasks,
            "time": end_time - start_time,
            "used_cpus": used_cpus,
            "success": "1",
            "_peak_memory": round(used_gb, 2),
            "_peak_process_memory": usage,
        }
        if not smoke_test:
            results["perf_metrics"] = [
                {
                    "perf_metric_name": "tasks_per_second",
                    "perf_metric_value": rate,
                    "perf_metric_type": "THROUGHPUT",
                },
                {
                    "perf_metric_name": "used_cpus_by_deadline",
                    "perf_metric_value": used_cpus,
                    "perf_metric_type": "THROUGHPUT",
                },
            ]
        dashboard_test.update_release_test_result(results)
        json.dump(results, out_file)


if __name__ == "__main__":
    test()
