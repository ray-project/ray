import asyncio
import os
import argparse

import ray
from ray._private.state_api_test_utils import verify_failed_task
from ray.util.state import list_workers
from ray._private.test_utils import wait_for_condition
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()


if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image}}

ray.init(num_cpus=1)


def get_worker_by_pid(pid, detail=True):
    for w in list_workers(detail=detail):
        if w["pid"] == pid:
            return w
    assert False


@ray.remote(runtime_env=runtime_env)
def f():
    return ray.get(g.remote())


@ray.remote(runtime_env=runtime_env)
def g():
    return os.getpid()


# Start a task that has a blocking call ray.get with g.remote.
# g.remote will borrow the CPU and start a new worker.
# The worker started for g.remote will exit by IDLE timeout.
pid = ray.get(f.remote())


def verify_exit_by_idle_timeout():
    worker = get_worker_by_pid(pid)
    type = worker["exit_type"]
    detail = worker["exit_detail"]
    return type == "INTENDED_SYSTEM_EXIT" and "it was idle" in detail


wait_for_condition(verify_exit_by_idle_timeout)

ray.shutdown()


@ray.remote(
    num_cpus=1,
    runtime_env=runtime_env,
)
class A:
    def __init__(self):
        self.sleeping = False

    async def getpid(self):
        while not self.sleeping:
            await asyncio.sleep(0.1)
        return os.getpid()

    async def sleep(self):
        self.sleeping = True
        await asyncio.sleep(9999)


pg = ray.util.placement_group(bundles=[{"CPU": 1}])
a = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
).remote()
a.sleep.options(name="sleep").remote()
pid = ray.get(a.getpid.remote())
ray.util.remove_placement_group(pg)


def verify_exit_by_pg_removed():
    worker = get_worker_by_pid(pid)
    type = worker["exit_type"]
    detail = worker["exit_detail"]
    assert verify_failed_task(
        name="sleep",
        error_type="ACTOR_DIED",
        error_message=["INTENDED_SYSTEM_EXIT", "placement group was removed"],
    )
    return type == "INTENDED_SYSTEM_EXIT" and "placement group was removed" in detail


wait_for_condition(verify_exit_by_pg_removed)


@ray.remote(runtime_env=runtime_env)
class PidDB:
    def __init__(self):
        self.pid = None

    def record_pid(self, pid):
        self.pid = pid

    def get_pid(self):
        return self.pid


p = PidDB.remote()


@ray.remote(runtime_env=runtime_env)
class FaultyActor:
    def __init__(self):
        p.record_pid.remote(os.getpid())
        raise Exception("exception in the initialization method")

    def ready(self):
        pass


a = FaultyActor.remote()
wait_for_condition(lambda: ray.get(p.get_pid.remote()) is not None)
pid = ray.get(p.get_pid.remote())


def verify_exit_by_actor_init_failure():
    worker = get_worker_by_pid(pid)
    type = worker["exit_type"]
    detail = worker["exit_detail"]
    assert type == "USER_ERROR" and "exception in the initialization method" in detail
    return verify_failed_task(
        name="FaultyActor.__init__",
        error_type="TASK_EXECUTION_EXCEPTION",
        error_message="exception in the initialization method",
    )


wait_for_condition(verify_exit_by_actor_init_failure)
