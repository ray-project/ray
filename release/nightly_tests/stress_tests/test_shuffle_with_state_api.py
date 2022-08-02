import click
import json
import os

import ray

from ray.experimental.state.api import (
    list_actors,
    list_nodes,
    list_objects,
    list_tasks,
    summarize_actors,
    summarize_objects,
    summarize_tasks,
)

from ray._private.state_api_test_utils import (
    StateAPICallSpec,
    run_workload,
    run_workload_with_state_api,
    STATE_LIST_LIMIT,
)

from ray._private.worker import RayContext


# Number of warm up iterations
NUM_WARM_UP_ITER = 3


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


def _warm_up_with_shuffle(num_partitions: int, partition_size: int):

    ctx = ray.init(log_to_driver=False)
    for _ in range(NUM_WARM_UP_ITER):
        run_workload(
            run_shuffle,
            ctx=ctx,
            num_partitions=num_partitions // 10,
            partition_size=partition_size,
        )
    ray.shutdown()


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
    "--call-interval-s", type=int, default=3, help="interval of state api calls"
)
def test(
    smoke_test,
    num_partitions,
    partition_size,
    call_interval_s,
):
    # Stage 0: Warm-up with a shuffle workload
    if not smoke_test:
        _warm_up_with_shuffle(
            num_partitions=num_partitions, partition_size=partition_size
        )
    else:
        num_partitions = 100

    # Set up state API calling methods
    def not_none(res):
        return res is not None

    apis = [
        StateAPICallSpec(list_nodes, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(list_objects, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(list_tasks, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(list_actors, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(summarize_tasks, not_none),
        StateAPICallSpec(summarize_actors, not_none),
        StateAPICallSpec(summarize_objects, not_none),
    ]

    results = run_workload_with_state_api(
        run_shuffle,
        {"num_partitions": num_partitions, "partition_size": partition_size},
        apis,
        call_interval_s=call_interval_s,
    )

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        json.dump(results, out_file)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    test()
