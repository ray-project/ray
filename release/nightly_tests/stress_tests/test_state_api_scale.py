import click
import ray
from ray._private.ray_constants import LOG_PREFIX_ACTOR_NAME
import ray._private.test_utils as test_utils
import tqdm
import asyncio

from ray.experimental.state.api import (
    get_log,
    list_actors,
    list_objects,
    list_tasks,
)

DEFAULT_RAY_TEST_MAX_ACTORS = int(1e3)
DEFAULT_RAY_TEST_MAX_TASKS = int(1e3)
DEFAULT_RAY_TEST_MAX_OBJECTS = int(1e3)

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
    invoke_state_api(lambda res: len(res) == 0, list_tasks)

    # Task definition adopted from:
    # https://docs.ray.io/en/master/ray-core/examples/highly_parallel.html
    from random import random

    SAMPLES = int(1e3)

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
    )

    ray.get(signal.send.remote())
    # Wait until all tasks finish
    outputs = []
    for _ in tqdm.trange(num_tasks, desc="Ensuring all tasks have finished"):
        done, results = ray.wait(results)
        v = ray.get(done[0])
        assert v >= 0
        outputs.append(v)

    pi = sum(outputs) * 4.0 / num_tasks / SAMPLES
    assert pi > 3 and pi < 4, f"Have a Pi={pi} from another universe."

    # Clean up
    # All compute tasks done other than the signal actor
    invoke_state_api(
        lambda res: len(res) == 0, list_tasks, filters=[("name", "=", "pi4_sample()")]
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
    )

    actors = [
        TestActor.remote() for _ in tqdm.trange(num_actors, desc="Launching actors...")
    ]

    waiting_actors = [actor.running.remote() for actor in actors]

    for _ in tqdm.trange(len(actors), desc="Waiting actors to be ready..."):
        ready, waiting_actors = ray.wait(waiting_actors)
        assert ray.get(*ready)

    invoke_state_api(
        lambda res: len(res) == num_actors,
        list_actors,
        filters=[("class_name", "=", actor_class_name)],
    )

    exiting_actors = [actor.exit.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Destroying actors..."):
        _exitted, exiting_actors = ray.wait(exiting_actors)

    invoke_state_api(
        lambda res: len(res) == 0, list_actors, filters=[("state", "=", "ALIVE")]
    )
    invoke_state_api(
        lambda res: len(res) == num_actors,
        list_actors,
        filters=[("state", "=", "DEAD"), ("class_name", "=", actor_class_name)],
    )


def test_many_objects(num_objects, num_actors):
    @ray.remote(num_cpus=0.1)
    class ObjectActor:
        def __init__(self):
            self.objs = []

        def create_objs(self, num_objects, size):
            import os

            for _ in range(num_objects):
                self.objs.append(ray.put(bytearray(os.urandom(size))))

            return self.objs

        def exit(self):
            ray.actor.exit_actor()

    assert num_objects % num_actors == 0, "num_objects must be multiple of num_actors"

    actors = [
        ObjectActor.remote() for _ in tqdm.trange(num_actors, desc="Creating actors...")
    ]

    waiting_actors = [
        actor.create_objs.remote(num_objects // num_actors, 100000) for actor in actors
    ]
    for _ in tqdm.trange(len(actors), desc="Waiting actors to be create objects..."):
        objs, waiting_actors = ray.wait(waiting_actors)
        assert len(ray.get(*objs)) == num_objects // num_actors

    invoke_state_api(
        lambda res: len(res) == num_objects,
        list_objects,
        filters=[
            ("reference_type", "=", "LOCAL_REFERENCE"),
            ("type", "=", "Worker"),
        ],
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
    for s in get_log(actor_id=actor._actor_id.hex(), tail=-1, node_id=node_id):
        ctx.update(s.encode())

    assert expected_hash == ctx.hexdigest(), "Mismatch log file"


def invoke_state_api(verify_cb, state_api_fn, **kwargs):
    def _verify():
        res = state_api_fn(**kwargs)
        assert verify_cb(res), f"Calling State API failed. len(res)=({len(res)})"
        return True

    test_utils.wait_for_condition(_verify)


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


@click.command()
@click.option(
    "--num-tasks",
    required=False,
    default=DEFAULT_RAY_TEST_MAX_TASKS,
    type=int,
    help="Number of tasks to launch.",
)
@click.option(
    "--num-actors",
    required=False,
    default=DEFAULT_RAY_TEST_MAX_ACTORS,
    type=int,
    help="Number of actors to launch.",
)
@click.option(
    "--num-objects",
    required=False,
    default=DEFAULT_RAY_TEST_MAX_OBJECTS,
    type=int,
    help="Number of actors to launch.",
)
@click.option(
    "--log-file-size-byte",
    required=False,
    default=DEFAULT_RAY_TEST_MAX_OBJECTS,
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
def test(num_tasks, num_actors, num_objects, log_file_size_byte, smoke_test):
    ray.init(address="auto")

    num_cpus = int(
        sum(node["Resources"]["CPU"] for node in ray.nodes() if node["alive"])
    )

    test_utils.wait_for_condition(no_resource_leaks)
    # monitor_actor = test_utils.monitor_memory_usage()
    # start_time = time.time()

    # Run some long-running tasks
    test_many_tasks(num_tasks=num_tasks)

    # Run many actors
    test_many_actors(num_actors=num_actors)

    # Create many objects
    test_many_objects(num_objects=num_objects, num_actors=num_cpus)

    # Create large logs
    test_large_log_file(log_file_size_byte=log_file_size_byte)

    # TODO: dumping test results
    print("PASS")


if __name__ == "__main__":
    test()
