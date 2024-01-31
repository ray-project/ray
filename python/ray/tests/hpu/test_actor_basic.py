# coding: utf-8
import os
import sys
import ray

import pytest

def test_decorator_args(ray_start_regular):
    
    # This is a valid way of using the decorator.
    @ray.remote(resources={'HPU': 1})  # noqa: F811
    class Actor:  # noqa: F811
        def __init__(self):
            pass
    
    # This is a valid way of using the decorator.
    @ray.remote(num_cpus=1, resources={'HPU': 1})  # noqa: F811
    class Actor:  # noqa: F811
        def __init__(self):
            pass


def test_actor_deletion_with_hpus(shutdown_only):
    ray.init(num_cpus=1, resources={'HPU': 1}, object_store_memory=int(150 * 1024 * 1024))

    # When an actor that uses a HPU exits, make sure that the HPU resources
    # are released.

    @ray.remote(resources={'HPU': 1})
    class Actor:
        def getpid(self):
            return os.getpid()

    for _ in range(5):
        # If we can successfully create an actor, that means that enough
        # HPU resources are available.
        a = Actor.remote()
        ray.get(a.getpid.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_actor_hpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 2
    num_hpus_per_raylet = 2
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=10 * 2, resources={"HPU": num_hpus_per_raylet}
        )
    ray.init(address=cluster.address)

    @ray.remote(resources={'HPU': 1})
    class Actor1:
        def __init__(self):
            resource_ids = ray.get_runtime_context().get_resource_ids()
            self.hpu_ids = resource_ids.get('HPU')

        def get_location_and_ids(self):
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.hpu_ids),
            )

    # Create one actor per HPU.
    actors = [Actor1.remote() for _ in range(num_nodes * num_hpus_per_raylet)]
    # Make sure that no two actors are assigned to the same HPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors]
    )
    node_names = {location for location, hpu_id in locations_and_ids}
    assert len(node_names) == num_nodes
    location_actor_combinations = []
    for node_name in node_names:
        for hpu_id in range(num_hpus_per_raylet):
            location_actor_combinations.append((node_name, (f'{hpu_id}',)))

    assert set(locations_and_ids) == set(location_actor_combinations)

    # Creating a new actor should fail because all of the HPUs are being
    # used.
    a = Actor1.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_blocking_actor_task(shutdown_only):
    ray.init(num_cpus=1, resources={"HPU": 1}, object_store_memory=int(150 * 1024 * 1024))

    @ray.remote(resources={"HPU": 1})
    def f():
        return 1

    @ray.remote
    class Foo:
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure we can execute a blocking actor method even if there is
    # only one CPU.
    actor = Foo.remote()
    ray.get(actor.blocking_method.remote())

    @ray.remote(num_cpus=1)
    class CPUFoo:
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure that lifetime CPU resources are not released when actors
    # block.
    actor = CPUFoo.remote()
    x_id = actor.blocking_method.remote()
    ready_ids, remaining_ids = ray.wait([x_id], timeout=1.0)
    assert ready_ids == []
    assert remaining_ids == [x_id]

    @ray.remote(resources={"HPU": 1})
    class HPUFoo:
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure that HPU resources are not released when actors block.
    actor = HPUFoo.remote()
    x_id = actor.blocking_method.remote()
    ready_ids, remaining_ids = ray.wait([x_id], timeout=1.0)
    assert ready_ids == []
    assert remaining_ids == [x_id]


def test_actor_habana_visible_devices(shutdown_only):
    """Test user can overwrite HABANA_VISIBLE_MODULES
    after the actor is created."""
    ray.init(resources={'HPU': 1})

    @ray.remote(resources={'HPU': 1})
    class Actor:
        def set_habana_visible_devices(self, habana_visible_devices):
            os.environ["HABANA_VISIBLE_MODULES"] = habana_visible_devices

        def get_habana_visible_devices(self):
            return os.environ["HABANA_VISIBLE_MODULES"]

    actor = Actor.remote()
    assert ray.get(actor.get_habana_visible_devices.remote()) == "0"
    ray.get(actor.set_habana_visible_devices.remote("0,1"))
    assert ray.get(actor.get_habana_visible_devices.remote()) == "0,1"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
