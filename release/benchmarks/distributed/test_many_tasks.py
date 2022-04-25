import click
import json
import os
import ray
import ray._private.test_utils as test_utils
import time
import tqdm

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
    err_str = f"Only {max_cpus - min_cpus_available}/{max_cpus} cpus used."
    threshold = num_tasks * cpus_per_task * 0.75
    assert max_cpus - min_cpus_available > threshold, err_str

    for _ in tqdm.trange(num_tasks, desc="Ensuring all tasks have finished"):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None


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
    ray.init(address="auto")

    test_utils.wait_for_condition(no_resource_leaks)
    monitor_actor = test_utils.monitor_memory_usage()
    start_time = time.time()
    test_max_running_tasks(num_tasks)
    end_time = time.time()
    ray.get(monitor_actor.stop_run.remote())
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")
    del monitor_actor
    test_utils.wait_for_condition(no_resource_leaks)

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
                }
            ]
        json.dump(results, out_file)


if __name__ == "__main__":
    test()
