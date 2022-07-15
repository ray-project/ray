from collections import defaultdict, namedtuple
import click
import ray
from ray._private.ray_constants import LOG_PREFIX_ACTOR_NAME
import ray._private.test_utils as test_utils
import tqdm
import asyncio
import time
import os

from ray.experimental.state.api import (
    get_log,
    list_actors,
    list_objects,
    list_tasks,
)

TEST_MAX_ACTORS = int(1e4)  # 10k
TEST_MAX_TASKS = int(1e4)  # 10k
TEST_MAX_OBJECTS = int(1e5)  # 100k
TEST_LOG_FILE_SIZE = 4 * 1024 * 1024  # 4GB
STATE_LIST_LIMIT = int(1e6)  # 1m
STATE_LIST_TIMEOUT = 600  # 10min

StateAPIMetric = namedtuple("StateAPIMetric", ["latency_sec", "result_size"])
global_state_perf = defaultdict(list)


# We set num_cpus to zero because this actor will mostly just block on I/O.
@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()

    def send(self, clear=False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait=True):
        if should_wait:
            await self.ready_event.wait()


def test_many_tasks(num_tasks: int):
    # No tasks
    invoke_state_api(
        lambda res: len(res) == 0,
        list_tasks,
        limit=STATE_LIST_LIMIT,
        filters=[("name", "=", "pi4_sample()")],
    )

    # Task definition adopted from:
    # https://docs.ray.io/en/master/ray-core/examples/highly_parallel.html
    from random import random

    SAMPLES = 100

    @ray.remote
    def pi4_sample(signal):
        in_count = 0
        for _ in range(SAMPLES):
            x, y = random(), random()
            if x * x + y * y <= 1:
                in_count += 1
        # Block on signal
        ray.get(signal.wait.remote())
        return in_count

    results = []
    signal = SignalActor.remote()
    for _ in tqdm.trange(num_tasks, desc="Launching tasks"):
        results.append(pi4_sample.remote(signal))

    invoke_state_api(
        lambda res: len(res) == num_tasks,
        list_tasks,
        filters=[("name", "=", "pi4_sample()")],
        limit=STATE_LIST_LIMIT,
    )

    print("Waiting for tasks to finish...")
    ray.get(signal.send.remote())
    outputs = ray.get(results)

    pi = sum(outputs) * 4.0 / num_tasks / SAMPLES
    assert pi > 3 and pi < 4, f"Have a Pi={pi} from another universe."

    # Clean up
    # All compute tasks done other than the signal actor
    invoke_state_api(
        lambda res: len(res) == 0,
        list_tasks,
        filters=[("name", "=", "pi4_sample()")],
        limit=STATE_LIST_LIMIT,
    )

    del signal


def test_many_actors(num_actors: int):
    @ray.remote(num_cpus=0.00001)
    class TestActor:
        def running(self):
            return True

        def exit(self):
            ray.actor.exit_actor()

    actor_class_name = TestActor.__ray_metadata__.class_name
    invoke_state_api(
        lambda res: len(res) == 0,
        list_actors,
        filters=[("class_name", "=", actor_class_name)],
        limit=STATE_LIST_LIMIT,
    )

    actors = [
        TestActor.remote() for _ in tqdm.trange(num_actors, desc="Launching actors...")
    ]

    waiting_actors = [actor.running.remote() for actor in actors]
    print("Waiting for actors to finish...")
    ray.get(waiting_actors)

    invoke_state_api(
        lambda res: len(res) == num_actors,
        list_actors,
        filters=[("class_name", "=", actor_class_name)],
        limit=STATE_LIST_LIMIT,
    )

    exiting_actors = [actor.exit.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Destroying actors..."):
        _exitted, exiting_actors = ray.wait(exiting_actors)

    invoke_state_api(
        lambda res: len(res) == 0,
        list_actors,
        filters=[("state", "=", "ALIVE"), ("class_name", "=", actor_class_name)],
        limit=STATE_LIST_LIMIT,
    )
    invoke_state_api(
        lambda res: len(res) == num_actors,
        list_actors,
        filters=[("state", "=", "DEAD"), ("class_name", "=", actor_class_name)],
        limit=STATE_LIST_LIMIT,
    )


def test_many_objects(num_objects, num_actors):
    @ray.remote(num_cpus=0.1)
    class ObjectActor:
        def __init__(self):
            self.objs = []

        def create_objs(self, num_objects):
            import os

            for _ in range(num_objects):
                # Object size shouldn't matter here.
                self.objs.append(ray.put(bytearray(os.urandom(1024))))

            return self.objs

        def exit(self):
            ray.actor.exit_actor()

    actors = [
        ObjectActor.remote() for _ in tqdm.trange(num_actors, desc="Creating actors...")
    ]

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

    invoke_state_api(
        lambda res: len(res) == num_objects,
        list_objects,
        filters=[
            ("reference_type", "=", "LOCAL_REFERENCE"),
            ("type", "=", "Worker"),
        ],
        limit=STATE_LIST_LIMIT,
    )

    exiting_actors = [actor.exit.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Destroying actors..."):
        _exitted, exiting_actors = ray.wait(exiting_actors)


def test_large_log_file(log_file_size_byte: int):
    import sys
    import string
    import random
    import hashlib

    @ray.remote
    class LogActor:
        def write_log(self, log_file_size_byte: int):
            ctx = hashlib.md5()
            prefix = f"{LOG_PREFIX_ACTOR_NAME}LogActor\n"
            ctx.update(prefix.encode())
            while log_file_size_byte > 0:
                n = min(log_file_size_byte, 4096)
                chunk = "".join(random.choices(string.ascii_letters, k=n))
                sys.stdout.writelines([chunk])
                ctx.update(chunk.encode())
                log_file_size_byte -= n

            sys.stdout.flush()
            return ctx.hexdigest(), ray.get_runtime_context().node_id.hex()

    actor = LogActor.remote()
    expected_hash, node_id = ray.get(
        actor.write_log.remote(log_file_size_byte=log_file_size_byte)
    )
    assert expected_hash is not None, "Empty checksum from the log actor"
    assert node_id is not None, "Empty node id from the log actor"

    # Retrieve the log and compare the checksum
    ctx = hashlib.md5()
    # Assuming computing checksum doesn't dominate the time here.
    t_start = time.perf_counter()
    for s in get_log(actor_id=actor._actor_id.hex(), tail=-1, node_id=node_id):
        ctx.update(s.encode())
    t_end = time.perf_counter()
    assert expected_hash == ctx.hexdigest(), "Mismatch log file"

    time_taken = t_end - t_start
    metric = StateAPIMetric(time_taken, log_file_size_byte)
    global_state_perf["get_log"].append(metric)


def invoke_state_api(verify_cb, state_api_fn, **kwargs):
    global global_state_perf

    t_start = time.perf_counter()
    res = state_api_fn(**kwargs)
    t_end = time.perf_counter()

    time_taken = t_end - t_start
    metric = StateAPIMetric(time_taken, len(res))
    global_state_perf[state_api_fn.__name__] += [metric]
    print(f"Calling state api: {state_api_fn.__name__}, metric={metric}")
    assert verify_cb(res), f"Calling State API failed. len(res)=({len(res)})"


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


@click.command()
@click.option(
    "--num-tasks",
    required=False,
    default=TEST_MAX_TASKS,
    type=int,
    help="Number of tasks to launch.",
)
@click.option(
    "--num-actors",
    required=False,
    default=TEST_MAX_ACTORS,
    type=int,
    help="Number of actors to launch.",
)
@click.option(
    "--num-objects",
    required=False,
    default=TEST_MAX_OBJECTS,
    type=int,
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
    default=TEST_LOG_FILE_SIZE,
    type=int,
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
        num_tasks = 100
        num_actors = 10
        num_objects = 100
        log_file_size_byte = 1024

    test_utils.wait_for_condition(no_resource_leaks)
    monitor_actor = test_utils.monitor_memory_usage()
    start_time = time.perf_counter()
    # Run some long-running tasks
    print(f"\nRunning with many tasks={num_tasks}")
    test_many_tasks(num_tasks=num_tasks)
    print("\ntest_many_tasks() PASS")

    # Run many actors
    print(f"\nRunning with many actors={num_actors}")
    test_many_actors(num_actors=num_actors)
    print("\ntest_many_actors() PASS")

    # Create many objects
    print(f"\nRunning with many objects={num_objects}")
    test_many_objects(num_objects=num_objects, num_actors=num_actors_for_objects)
    print("\ntest_many_objects() PASS")

    # Create large logs
    print(f"\nRunning with large file={log_file_size_byte} bytes")
    test_large_log_file(log_file_size_byte=log_file_size_byte)
    print("\ntest_large_log_file() PASS")

    print("\n\nPASS")
    end_time = time.perf_counter()

    # Collect mem usage
    ray.get(monitor_actor.stop_run.remote())
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
    print(f"Peak memory usage: {round(used_gb, 2)}GB")
    print(f"Peak memory usage per processes:\n {usage}")
    del monitor_actor

    if "TEST_OUTPUT_JSON" in os.environ:
        import json

        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")

        def by_latency(metric):
            return metric.latency_sec

        slowest_task_metric = max(
            global_state_perf[list_tasks.__name__], key=by_latency
        )
        slowest_actors_metric = max(
            global_state_perf[list_actors.__name__], key=by_latency
        )
        slowest_objects_metric = max(
            global_state_perf[list_objects.__name__], key=by_latency
        )

        all_state_api_latency = sum(
            metric.latency_sec
            for metric_samples in global_state_perf.values()
            for metric in metric_samples
        )
        results = {
            "max_list_tasks_latency_sec": slowest_task_metric.latency_sec,
            "max_list_tasks_latency_result_size": slowest_task_metric.result_size,
            "max_list_actors_latency_sec": slowest_actors_metric.latency_sec,
            "max_list_actors_latency_result_size": slowest_actors_metric.result_size,
            "max_list_objects_latency_sec": slowest_objects_metric.latency_sec,
            "max_list_objects_latency_result_size": slowest_objects_metric.result_size,
            "get_log_latency_sec": global_state_perf[get_log.__name__][0].latency_sec,
            "get_log_latency_log_size_bytes": global_state_perf[get_log.__name__][
                0
            ].result_size,
            "time": end_time - start_time,
            "success": "1",
            "_peak_memory": round(used_gb, 2),
            "_peak_process_memory": usage,
            "perf_metrics": [
                {
                    "perf_metric_name": "all_state_api_latency",
                    "perf_metric_value": all_state_api_latency,
                    "perf_metric_type": "LATENCY",
                }
            ],
        }
        json.dump(results, out_file)
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    test()
