from asyncio import wait_for
import click
import ray
import ray._private.test_utils as test_utils
import time
import tqdm

from ray.experimental.state.api import list_tasks


def run_tasks(sleep_time: int, num_tasks: int):
    @ray.remote(num_cpus=1)
    def task():
        time.sleep(sleep_time)

    refs = [task.remote() for _ in tqdm.trange(num_tasks, desc="Launching tasks")]

    return refs


def invoke_state_api(verify_cb, state_api_fn, **kwargs):
    res = state_api_fn(**kwargs)

    assert verify_cb(res), "Calling State API failed"


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
    # monitor_actor = test_utils.monitor_memory_usage()
    # start_time = time.time()

    # No tasks
    invoke_state_api(lambda res: len(res) == 0, list_tasks)

    # Run some long-running tasks
    sleep_time = 3
    task_refs = run_tasks(sleep_time, num_tasks)
    invoke_state_api(lambda res: len(res) == num_tasks, list_tasks)

    # Wait until all tasks finish
    for _ in tqdm.trange(num_tasks, desc="Ensuring all tasks have finished"):
        done, task_refs = ray.wait(task_refs)
        assert ray.get(done[0]) is None

    invoke_state_api(lambda res: len(res) == 0, list_tasks)

    # TODO: dumping test results
    # end_time = time.time()
    # ray.get(monitor_actor.stop_run.remote())
    # used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    # print(f"Peak memory usage: {round(used_gb, 2)}GB")
    # print(f"Peak memory usage per processes:\n {usage}")
    # del monitor_actor
    # test_utils.wait_for_condition(no_resource_leaks)

    # rate = num_tasks / (end_time - start_time - sleep_time)
    # print(
    #     f"Success! Started {num_tasks} tasks in {end_time - start_time}s. "
    #     f"({rate} tasks/s)"
    # )

    # if "TEST_OUTPUT_JSON" in os.environ:
    #     out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    #     results = {
    #         "tasks_per_second": rate,
    #         "num_tasks": num_tasks,
    #         "time": end_time - start_time,
    #         "success": "1",
    #         "_peak_memory": round(used_gb, 2),
    #         "_peak_process_memory": usage,
    #     }
    #     if not smoke_test:
    #         results["perf_metrics"] = [
    #             {
    #                 "perf_metric_name": "tasks_per_second",
    #                 "perf_metric_value": rate,
    #                 "perf_metric_type": "THROUGHPUT",
    #             }
    #         ]
    #     json.dump(results, out_file)


if __name__ == "__main__":
    test()
