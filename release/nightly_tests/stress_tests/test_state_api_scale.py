import click
import json
import ray
from ray._private.ray_constants import LOG_PREFIX_ACTOR_NAME, LOG_PREFIX_JOB_ID
from ray._private.state_api_test_utils import (
    STATE_LIST_LIMIT,
    StateAPIMetric,
    aggregate_perf_results,
    invoke_state_api,
    GLOBAL_STATE_STATS,
)

import ray._private.test_utils as test_utils
import tqdm
import time
import os

from ray.util.placement_group import (
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


from ray.util.state import (
    get_log,
    list_actors,
    list_objects,
    list_tasks,
)

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__file__)

GiB = 1024 * 1024 * 1024
MiB = 1024 * 1024


def invoke_state_api_n(*args, **kwargs):
    def verify():
        NUM_API_CALL_SAMPLES = 10
        for _ in range(NUM_API_CALL_SAMPLES):
            invoke_state_api(*args, **kwargs)
        return True

    test_utils.wait_for_condition(verify, retry_interval_ms=2000, timeout=30)


def test_many_tasks(num_tasks: int):
    TASK_NAME_TEMPLATE = "pi4_sample_{num_tasks}"
    if num_tasks == 0:
        logger.info("Skipping test with no tasks")
        return

    # No running tasks
    invoke_state_api_n(
        lambda res: len(res) == 0,
        list_tasks,
        filters=[("name", "=", TASK_NAME_TEMPLATE.format(num_tasks=num_tasks))],
        key_suffix="0",
        limit=STATE_LIST_LIMIT,
        err_msg=(
            "Expect 0 running tasks for "
            f"{TASK_NAME_TEMPLATE.format(num_tasks=num_tasks)}"
        ),
    )

    # Task definition adopted from:
    # https://docs.ray.io/en/master/ray-core/examples/highly_parallel.html
    from random import random

    SAMPLES = 100

    @ray.remote
    def pi4_sample():
        in_count = 0
        for _ in range(SAMPLES):
            x, y = random(), random()
            if x * x + y * y <= 1:
                in_count += 1
        return in_count

    results = []
    for _ in tqdm.trange(num_tasks, desc="Launching tasks"):
        results.append(
            pi4_sample.options(
                name=TASK_NAME_TEMPLATE.format(num_tasks=num_tasks)
            ).remote()
        )

    invoke_state_api_n(
        lambda res: len(res) == num_tasks,
        list_tasks,
        filters=[("name", "=", TASK_NAME_TEMPLATE.format(num_tasks=num_tasks))],
        key_suffix=f"{num_tasks}",
        limit=STATE_LIST_LIMIT,
        err_msg=f"Expect {num_tasks} non finished tasks.",
    )

    ray.get(results)

    # Clean up
    # All compute tasks done other than the signal actor
    invoke_state_api_n(
        lambda res: len(res) == 0,
        list_tasks,
        filters=[
            ("name", "=", TASK_NAME_TEMPLATE.format(num_tasks=num_tasks)),
            ("state", "=", "RUNNING"),
        ],
        key_suffix="0",
        limit=STATE_LIST_LIMIT,
        err_msg="Expect 0 running tasks",
    )


def test_many_actors(num_actors: int):
    if num_actors == 0:
        logger.info("Skipping test with no actors")
        return

    @ray.remote
    class TestActor:
        def running(self):
            return True

        def exit(self):
            ray.actor.exit_actor()

    actor_class_name = TestActor.__ray_metadata__.class_name

    invoke_state_api(
        lambda res: len(res) == 0,
        list_actors,
        filters=[("state", "=", "ALIVE"), ("class_name", "=", actor_class_name)],
        key_suffix="0",
        limit=STATE_LIST_LIMIT,
    )

    actors = [
        TestActor.remote() for _ in tqdm.trange(num_actors, desc="Launching actors...")
    ]

    waiting_actors = [actor.running.remote() for actor in actors]
    logger.info("Waiting for actors to finish...")
    ray.get(waiting_actors)

    invoke_state_api_n(
        lambda res: len(res) == num_actors,
        list_actors,
        filters=[("state", "=", "ALIVE"), ("class_name", "=", actor_class_name)],
        key_suffix=f"{num_actors}",
        limit=STATE_LIST_LIMIT,
    )

    exiting_actors = [actor.exit.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Destroying actors..."):
        _exitted, exiting_actors = ray.wait(exiting_actors)

    invoke_state_api(
        lambda res: len(res) == 0,
        list_actors,
        filters=[("state", "=", "ALIVE"), ("class_name", "=", actor_class_name)],
        key_suffix="0",
        limit=STATE_LIST_LIMIT,
    )


def test_many_objects(num_objects, num_actors):
    if num_objects == 0:
        logger.info("Skipping test with no objects")
        return

    pg = placement_group([{"CPU": 1}] * num_actors, strategy="SPREAD")
    ray.get(pg.ready())

    # We will try to put actors on multiple nodes.
    @ray.remote
    class ObjectActor:
        def __init__(self):
            self.objs = []

        def create_objs(self, num_objects):
            import os

            for i in range(num_objects):
                # Object size shouldn't matter here.
                self.objs.append(ray.put(bytearray(os.urandom(1024))))
                if (i + 1) % 100 == 0:
                    logger.info(f"Created object {i+1}...")

            return self.objs

        def ready(self):
            pass

    actors = [
        ObjectActor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
            )
        ).remote()
        for _ in tqdm.trange(num_actors, desc="Creating actors...")
    ]

    waiting_actors = [actor.ready.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Waiting actors to be ready..."):
        _ready, waiting_actors = ray.wait(waiting_actors)

    # Splitting objects to multiple actors for creation,
    # credit: https://stackoverflow.com/a/2135920
    def _split(a, n):
        k, m = divmod(len(a), n)
        return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))

    num_objs_per_actor = [len(objs) for objs in _split(range(num_objects), num_actors)]

    waiting_actors = [
        actor.create_objs.remote(num_objs)
        for actor, num_objs in zip(actors, num_objs_per_actor)
    ]

    total_objs_created = 0
    for _ in tqdm.trange(num_actors, desc="Waiting actors to create objects..."):
        objs, waiting_actors = ray.wait(waiting_actors)
        total_objs_created += len(ray.get(*objs))

    assert (
        total_objs_created == num_objects
    ), "Expect correct number of objects created."

    invoke_state_api_n(
        lambda res: len(res) == num_objects,
        list_objects,
        filters=[
            ("reference_type", "=", "LOCAL_REFERENCE"),
            ("type", "=", "WORKER"),
        ],
        key_suffix=f"{num_objects}",
        limit=STATE_LIST_LIMIT,
    )

    del actors
    remove_placement_group(pg)


def test_large_log_file(log_file_size_byte: int):
    if log_file_size_byte == 0:
        logger.info("Skipping test with 0 log file size")
        return

    import sys
    import string
    import random
    import hashlib

    @ray.remote
    class LogActor:
        def write_log(self, log_file_size_byte: int):
            ctx = hashlib.sha1()
            job_id = ray.get_runtime_context().get_job_id()
            prefix = f"{LOG_PREFIX_JOB_ID}{job_id}\n{LOG_PREFIX_ACTOR_NAME}LogActor\n"
            ctx.update(prefix.encode())
            while log_file_size_byte > 0:
                n = min(log_file_size_byte, 4 * MiB)
                chunk = "".join(random.choices(string.ascii_letters, k=n))
                sys.stdout.writelines([chunk])
                ctx.update(chunk.encode())
                log_file_size_byte -= n

            sys.stdout.flush()
            return ctx.hexdigest(), ray.get_runtime_context().get_node_id()

    actor = LogActor.remote()

    task = actor.write_log.remote(log_file_size_byte=log_file_size_byte)
    expected_hash, node_id = ray.get(task)
    assert expected_hash is not None, "Empty checksum from the log actor"
    assert node_id is not None, "Empty node id from the log actor"

    # Retrieve the log and compare the checksum
    ctx = hashlib.sha1()

    time_taken = 0
    t_start = time.perf_counter()
    for s in get_log(actor_id=actor._actor_id.hex(), tail=1000000000):
        t_end = time.perf_counter()
        time_taken += t_end - t_start
        # Not including this time
        ctx.update(s.encode())
        # Only time the iterator's performance
        t_start = time.perf_counter()

    assert expected_hash == ctx.hexdigest(), "Mismatch log file"

    metric = StateAPIMetric(time_taken, log_file_size_byte)
    GLOBAL_STATE_STATS.calls["get_log"].append(metric)


def _parse_input(
    num_tasks_str: str, num_actors_str: str, num_objects_str: str, log_file_sizes: str
):
    def _split_to_int(s):
        tokens = s.split(",")
        return [int(token) for token in tokens]

    return (
        _split_to_int(num_tasks_str),
        _split_to_int(num_actors_str),
        _split_to_int(num_objects_str),
        _split_to_int(log_file_sizes),
    )


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


@click.command()
@click.option(
    "--num-tasks",
    required=False,
    default="1,100,1000,10000",
    type=str,
    help="Number of tasks to launch.",
)
@click.option(
    "--num-actors",
    required=False,
    default="1,100,1000,5000",
    type=str,
    help="Number of actors to launch.",
)
@click.option(
    "--num-objects",
    required=False,
    default="100,1000,10000,50000",
    type=str,
    help="Number of actors to launch.",
)
@click.option(
    "--num-actors-for-objects",
    required=False,
    default=16,
    type=int,
    help="Number of actors to use for object creation.",
)
@click.option(
    "--log-file-size-byte",
    required=False,
    default=f"{256*MiB},{1*GiB},{4*GiB}",
    type=str,
    help="Number of actors to launch.",
)
@click.option(
    "--smoke-test",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, it's a smoke test",
)
def test(
    num_tasks,
    num_actors,
    num_objects,
    num_actors_for_objects,
    log_file_size_byte,
    smoke_test,
):
    ray.init(address="auto", log_to_driver=False)

    if smoke_test:
        num_tasks = "1,100"
        num_actors = "1,10"
        num_objects = "1,100"
        num_actors_for_objects = 1
        log_file_size_byte = f"64,{16*MiB}"
        global STATE_LIST_LIMIT
        STATE_LIST_LIMIT = STATE_LIST_LIMIT // 1000

    # Parse the input
    num_tasks_arr, num_actors_arr, num_objects_arr, log_file_size_arr = _parse_input(
        num_tasks, num_actors, num_objects, log_file_size_byte
    )

    test_utils.wait_for_condition(no_resource_leaks)
    monitor_actor = test_utils.monitor_memory_usage()
    start_time = time.perf_counter()
    # Run some long-running tasks
    for n in num_tasks_arr:
        logger.info(f"Running with many tasks={n}")
        test_many_tasks(num_tasks=n)
        logger.info(f"test_many_tasks({n}) PASS")

    # Run many actors
    for n in num_actors_arr:
        logger.info(f"Running with many actors={n}")
        test_many_actors(num_actors=n)
        logger.info(f"test_many_actors({n}) PASS")

    # Create many objects
    for n in num_objects_arr:
        logger.info(f"Running with many objects={n}")
        test_many_objects(num_objects=n, num_actors=num_actors_for_objects)
        logger.info(f"test_many_objects({n}) PASS")

    # Create large logs
    for n in log_file_size_arr:
        logger.info(f"Running with large file={n} bytes")
        test_large_log_file(log_file_size_byte=n)
        logger.info(f"test_large_log_file({n} bytes) PASS")

    print("\n\nPASS")
    end_time = time.perf_counter()

    # Collect mem usage
    ray.get(monitor_actor.stop_run.remote())
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")
    del monitor_actor

    state_perf_result = aggregate_perf_results()
    results = {
        "time": end_time - start_time,
        "success": "1",
        "_peak_memory": round(used_gb, 2),
        "_peak_process_memory": usage,
    }

    if not smoke_test:
        results["perf_metrics"] = [
            {
                "perf_metric_name": "avg_state_api_latency_sec",
                "perf_metric_value": state_perf_result["avg_state_api_latency_sec"],
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "avg_state_api_get_log_latency_sec",
                "perf_metric_value": state_perf_result["avg_get_log_latency_sec"],
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "avg_state_api_list_tasks_10000_latency_sec",
                "perf_metric_value": state_perf_result[
                    "avg_list_tasks_10000_latency_sec"
                ],
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "avg_state_api_list_actors_5000_latency_sec",
                "perf_metric_value": state_perf_result[
                    "avg_list_actors_5000_latency_sec"
                ],
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "avg_state_api_list_objects_50000_latency_sec",
                "perf_metric_value": state_perf_result[
                    "avg_list_objects_50000_latency_sec"
                ],
                "perf_metric_type": "LATENCY",
            },
        ]

    if "TEST_OUTPUT_JSON" in os.environ:
        with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
            json.dump(results, out_file)

    results.update(state_perf_result)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    test()
