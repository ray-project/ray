import ray
import ray.autoscaler.sdk
from ray.test_utils import Semaphore
from ray.util.placement_group import placement_group

from time import sleep
from tqdm import tqdm, trange

MAX_NUM_NODES = 250
MAX_ACTORS_IN_CLUSTER = 500
MAX_RUNNING_TASKS_IN_CLUSTER = 10000
MAX_PLACEMENT_GROUPS = 32

def scale_to(target):
    ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
    while len(ray.nodes()) != target:
        print("Current num_nodes: ", len(ray.nodes()))
        print("Waiting ...")
        sleep(5)

def test_nodes():
    scale_to(MAX_NUM_NODES)
    assert len(ray.nodes()) == MAX_NUM_NODES

def test_max_actors():
    @ray.remote
    class Actor:
        def foo(self):
            pass

    print("Launching actors")
    actors = [Actor.remote() for _ in trange(MAX_ACTORS_IN_CLUSTER)]

    print("Ensuring actors have started")
    for actor in tqdm(actors):
        assert ray.get(actor.foo.remote()) is None

def test_max_running_tasks():
    counter = Semaphore.remote(0)
    blocker = Semaphore.remote(0)
    total_resource = ray.cluster_resources()["node"]

    # Use this custom node resource instead of cpus for scheduling to properly loadbalance. CPU borrowing could lead to unintuitive load balancing if we used num_cpus.
    resource_per_task = total_resource / MAX_RUNNING_TASKS_IN_CLUSTER

    @ray.remote(num_cpus=0, resources={"node": resource_per_task})
    def task(counter, blocker):
        sleep(30)
        # TODO (Alex): We should guarantee that all these tasks are running concurrently. Unfortunately these semaphores currently don't work due to too many simultaneous connections.
        # ray.get(counter.release.remote())
        # ray.get(blocker.acquire.remote())
        pass

    print("Launching tasks")
    refs = [task.remote(counter, blocker) for _ in trange(MAX_RUNNING_TASKS_IN_CLUSTER)]

    # print("Ensuring all tasks are running")
    # for _ in trange(MAX_RUNNING_TASKS_IN_CLUSTER):
    #     ray.get(counter.acquire.remote())

    # print("Unblocking all tasks")
    # for _ in trange(MAX_RUNNING_TASKS_IN_CLUSTER):
    #     ray.get(blocker.release.remote())
    print("Waiting ...")
    for _ in trange(30):
        sleep(1)

    print("Ensuring all tasks have finished")
    for _ in trange(MAX_RUNNING_TASKS_IN_CLUSTER):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None

def test_many_placement_groups():
    print("Creating placement groups")
    placement_groups = []
    for _ in trange(MAX_PLACEMENT_GROUPS):
        bundle1 = {"CPU": 0.5, "node": 0.125}
        bundle2 = {"CPU": 0.5}
        bundle3 = {"node": 0.125}
        pg = placement_group([bundle1, bundle2, bundle3], strategy="STRICT_SPREAD")
        placement_groups.append(pg)

    print("Waiting for all placement groups to be ready")
    for pg in tqdm(placement_groups):
        ray.get(pg.ready())


    @ray.remote(num_cpus=0.5, resources={"node": 0.125})
    def f1():
        sleep(30)

    @ray.remote(num_cpus=0.5)
    def f2():
        sleep(30)

    @ray.remote(resources={"node": 0.125})
    def f3():
        sleep(30)

    print("Running tasks on placement groups")
    refs = []
    for pg in tqdm(placement_groups):
        ref1 = f1.options(placement_group=pg).remote()
        ref2 = f2.options(placement_group=pg).remote()
        ref3 = f3.options(placement_group=pg).remote()
        refs.extend([ref1, ref2, ref3])

    print("Waiting...")
    for _ in trange(30):
        sleep(1)

    print("Getting results")
    for _ in trange(MAX_PLACEMENT_GROUPS):
        results = ray.get(refs[-3:])
        assert results == [None, None, None], results
        refs = refs[:-3]



ray.init(address="auto")

test_nodes()
print("Done launching nodes")
# test_max_actors()
# print("Done testing actors")
# test_max_running_tasks()
# print("Done testing tasks")
test_many_placement_groups()
print("Done")

