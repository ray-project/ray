import click
import json
import ray
from ray._private.state_api_test_utils import (
    StateAPICallSpec,
    periodic_invoke_state_apis_with_actor,
)
import ray._private.test_utils as test_utils
from ray._private.worker import RayContext
import time
import os

from ray.experimental.state.api import (
    list_actors,
    list_nodes,
    list_objects,
    list_tasks,
)


# Run some long-running tasks
def run_shuffle(ctx: RayContext, num_partitions: int, partition_size: float):
    import subprocess

    commands = [
        "python",
        "-m",
        "ray.experimental.shuffle",
        f"--num-partitions={num_partitions}",
        f"--partition-size={partition_size}",
        f"--ray-address={ctx.address_info['gcs_address']}",
        "--no-streaming",
    ]

    subprocess.check_call(commands)


def run_workload(fn, **kwargs):

    monitor_actor = test_utils.monitor_memory_usage()

    start = time.perf_counter()
    fn(**kwargs)
    end = time.perf_counter()
    # Collect mem usage
    ray.get(monitor_actor.stop_run.remote())
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())

    results = {
        "duration": end - start,
        "peak_memory": round(used_gb, 2),
        "peak_process_memory": usage,
    }
    return results


@click.command()
@click.option(
    "--smoke-test",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, it's a smoke test",
)
@click.option("--num-partitions", type=int, default=100, help="number of partitions")
@click.option("--partition-size", type=str, default="20e6", help="partition size")
@click.option(
    "--call-interval-s", type=int, default=2, help="interval of state api calls"
)
def test(
    smoke_test,
    num_partitions,
    partition_size,
    call_interval_s,
):
    # Stage 0: Warm-up with a shuffle workload
    if not smoke_test:
        ctx = ray.init(log_to_driver=False)
        run_workload(
            run_shuffle,
            ctx=ctx,
            num_partitions=num_partitions // 10,
            partition_size=partition_size,
        )
        ray.shutdown()
    else:
        num_partitions = 100

    # Stage 1: Run with state APIs
    start_time = time.perf_counter()
    ctx = ray.init()

    def not_none(res):
        return res is not None

    apis = [
        StateAPICallSpec(list_nodes, not_none),
        StateAPICallSpec(list_objects, not_none),
        StateAPICallSpec(list_tasks, not_none),
        StateAPICallSpec(list_actors, not_none),
    ]

    api_caller = periodic_invoke_state_apis_with_actor(
        apis=apis, call_interval_s=call_interval_s, print_interval_s=30
    )

    stats_with_state_apis = run_workload(
        run_shuffle,
        ctx=ctx,
        num_partitions=num_partitions,
        partition_size=partition_size,
    )

    ray.get(api_caller.stop.remote())
    print(json.dumps(ray.get(api_caller.get_stats.remote()), indent=2))
    ray.shutdown()

    # Stage 2: Run without API generator
    ctx = ray.init()
    stats = run_workload(
        run_shuffle,
        ctx=ctx,
        num_partitions=num_partitions,
        partition_size=partition_size,
    )

    end_time = time.perf_counter()

    # Dumping results
    results = {
        "time": end_time - start_time,
        "success": "1",
        "perf_metrics": [
            {
                "perf_metric_name": "state_api_extra_latency_sec",
                "perf_metric_value": stats_with_state_apis["duration"]
                - stats["duration"],
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "state_api_extra_mem",
                "perf_metric_value": stats_with_state_apis["peak_memory"]
                - stats["peak_memory"],
                "perf_metric_type": "MEMORY",
            },
        ],
    }

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        json.dump(results, out_file)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    test()
