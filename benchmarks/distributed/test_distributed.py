import ray
import ray.autoscaler.sdk
from ray.test_utils import Semaphore
from ray.util.placement_group import placement_group, remove_placement_group

from time import sleep, perf_counter
from tqdm import tqdm, trange

TEST_NUM_NODES = 64
MAX_ACTORS_IN_CLUSTER = 10000
MAX_RUNNING_TASKS_IN_CLUSTER = 10000
MAX_PLACEMENT_GROUPS = 1000
MAX_NUM_NODES = 250


def num_alive_nodes():
    n = 0
    for node in ray.nodes():
        if node["Alive"]:
            n += 1
    return n


def scale_to(target):
    while num_alive_nodes() != target:
        ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
        print(f"Current # nodes: {num_alive_nodes()}, target: {target}")
        print("Waiting ...")
        sleep(5)


def test_nodes():
    scale_to(MAX_NUM_NODES)
    assert num_alive_nodes() == MAX_NUM_NODES
    # Treat this as a trivial task to ensure the nodes are all functioning
    test_max_running_tasks()


def test_max_actors():
    # TODO (Alex): Dynamically set this based on number of cores
    cpus_per_actor = 0.25

    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

    actors = [
        Actor.remote()
        for _ in trange(MAX_ACTORS_IN_CLUSTER, desc="Launching actors")
    ]

    for actor in tqdm(actors, desc="Ensuring actors have started"):
        assert ray.get(actor.foo.remote()) is None


def test_max_running_tasks():
    counter = Semaphore.remote(0)
    blocker = Semaphore.remote(0)

    @ray.remote(num_cpus=0.25)
    def task(counter, blocker):
        sleep(300)

    refs = [
        task.remote(counter, blocker)
        for _ in trange(MAX_RUNNING_TASKS_IN_CLUSTER, desc="Launching tasks")
    ]

    max_cpus = ray.cluster_resources()["CPU"]
    min_cpus_available = max_cpus
    for _ in trange(int(300 / 0.1), desc="Waiting"):
        try:
            cur_cpus = ray.available_resources().get("CPU", 0)
            min_cpus_available = min(min_cpus_available, cur_cpus)
        except Exception:
            # There are race conditions `.get` can fail if a new heartbeat
            # comes at the same time.
            pass
        sleep(0.1)

    # There are some relevant magic numbers in this check. 10k tasks each
    # require 1/4 cpus. Therefore, ideally 2.5k cpus will be used.
    err_str = f"Only {max_cpus - min_cpus_available}/{max_cpus} cpus used."
    assert max_cpus - min_cpus_available > 2000, err_str

    for _ in trange(
            MAX_RUNNING_TASKS_IN_CLUSTER,
            desc="Ensuring all tasks have finished"):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None


def test_many_placement_groups():
    @ray.remote(num_cpus=1, resources={"node": 0.02})
    def f1():
        sleep(10)
        pass

    @ray.remote(num_cpus=1)
    def f2():
        sleep(10)
        pass

    @ray.remote(resources={"node": 0.02})
    def f3():
        sleep(10)
        pass

    bundle1 = {"node": 0.02, "CPU": 1}
    bundle2 = {"CPU": 1}
    bundle3 = {"node": 0.02}

    pgs = []
    for _ in trange(MAX_PLACEMENT_GROUPS, desc="Creating pgs"):
        pg = placement_group(bundles=[bundle1, bundle2, bundle3])
        pgs.append(pg)

    for pg in tqdm(pgs, desc="Waiting for pgs to be ready"):
        ray.get(pg.ready())

    refs = []
    for pg in tqdm(pgs, desc="Scheduling tasks"):
        ref1 = f1.options(placement_group=pg).remote()
        ref2 = f2.options(placement_group=pg).remote()
        ref3 = f3.options(placement_group=pg).remote()
        refs.extend([ref1, ref2, ref3])

    for _ in trange(10, desc="Waiting"):
        sleep(1)

    with tqdm() as p_bar:
        while refs:
            done, refs = ray.wait(refs)
            p_bar.update()

    for pg in tqdm(pgs, desc="Cleaning up pgs"):
        remove_placement_group(pg)


ray.init(address="auto")

scale_to(TEST_NUM_NODES)
assert num_alive_nodes(
) == TEST_NUM_NODES, "Wrong number of nodes in cluster " + len(ray.nodes())

cluster_resources = ray.cluster_resources()

available_resources = ray.available_resources()
assert available_resources == cluster_resources, (
    str(available_resources) + " != " + str(cluster_resources))
print("Done launching nodes")

actor_start = perf_counter()
test_max_actors()
actor_end = perf_counter()

sleep(1)
assert num_alive_nodes(
) == TEST_NUM_NODES, "Wrong number of nodes in cluster " + len(ray.nodes())
assert available_resources == cluster_resources, (
    str(available_resources) + " != " + str(cluster_resources))
print("Done testing actors")

task_start = perf_counter()
test_max_running_tasks()
task_end = perf_counter()

sleep(1)
assert num_alive_nodes(
) == TEST_NUM_NODES, "Wrong number of nodes in cluster " + len(ray.nodes())
assert available_resources == cluster_resources, (
    str(available_resources) + " != " + str(cluster_resources))
print("Done testing tasks")

pg_start = perf_counter()
test_many_placement_groups()
pg_end = perf_counter()

sleep(1)
assert num_alive_nodes(
) == TEST_NUM_NODES, "Wrong number of nodes in cluster " + len(ray.nodes())
assert available_resources == cluster_resources, (
    str(available_resources) + " != " + str(cluster_resources))
print("Done testing placement groups")

launch_start = perf_counter()
test_nodes()
launch_end = perf_counter()

sleep(1)
assert num_alive_nodes(
) == MAX_NUM_NODES, "Wrong number of nodes in cluster " + len(ray.nodes())
print("Done.")

actor_time = actor_end - actor_start
task_time = task_end - task_start
pg_time = pg_end - pg_start
launch_time = launch_end - launch_start

print(f"Actor time: {actor_time} ({MAX_ACTORS_IN_CLUSTER} actors)")
print(f"Task time: {task_time} ({MAX_RUNNING_TASKS_IN_CLUSTER} tasks)")
print(f"PG time: {pg_time} ({MAX_PLACEMENT_GROUPS} placement groups)")
print(f"Node launch time: {launch_time} ({MAX_NUM_NODES} nodes)")
