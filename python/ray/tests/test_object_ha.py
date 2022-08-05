import pytest
import ray
import time
import numpy as np
import json
import os
import sys
from ray.tests.conftest import (
    mock_distributed_fs_object_spilling_config,
)


def test_owner_failed(ray_start_cluster):
    NODE_NUMBER = 2
    cluster_node_config = [
        {"num_cpus": 1, "resources": {f"node{i+1}": 10}} for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    OBJECT_NUMBER = 10

    @ray.remote(resources={"node1": 1}, num_cpus=0)
    class Creator:
        def gen_object_refs(self):
            refs = []
            for _ in range(OBJECT_NUMBER):
                refs.append(ray.put(np.random.rand(2, 2), _ha=True))
            return refs

    @ray.remote(resources={"node2": 1}, num_cpus=0)
    class Borrower:
        def __init__(self):
            self.refs = None

        def set_object_refs(self, refs):
            self.refs = refs

        def get_objects(self):
            ray.get(self.refs)
            return True

    creator = Creator.remote()
    borrower = Borrower.remote()

    refs = ray.get(creator.gen_object_refs.remote())
    ray.get(borrower.set_object_refs.remote(refs))
    owner = ray.get_actor("_ray_global_owner_")
    ray.kill(owner, no_restart=True)

    assert ray.get(borrower.get_objects.remote(), timeout=120)


@pytest.mark.parametrize(
    "actor_resources",
    [
        dict(zip(["creator", "borrower"], [{f"node{i}": 1} for i in _]))
        for _ in [
            [1, 2],  # None of them is on the same node.
            [1, 1],  # borrower and creator are on the same node.
        ]
    ],
)
def test_object_location(ray_start_cluster, actor_resources):
    NODE_NUMBER = 2
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    class Worker:
        def __init__(self):
            self.refs = None

        def create_obj(self):
            self.refs = [
                ray.put(np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8), _ha=True)
            ]
            print("global owner: ", ray.ActorID(self.refs[0].global_owner_id()))
            return self.refs

        def send_refs(self, refs):
            self.refs = refs

        def try_get(self):
            print("global owner in get: ", ray.ActorID(self.refs[0].global_owner_id()))
            [data] = ray.get(self.refs)
            return len(data)

        def warmup(self):
            return True

        def evict_all_object(self):
            ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))

        def exit(self):
            sys.exit(0)

    worker_1 = (
        ray.remote(Worker)
        .options(resources=actor_resources["creator"], num_cpus=0)
        .remote()
    )
    ray.get(worker_1.warmup.remote())
    worker_2 = (
        ray.remote(Worker)
        .options(resources=actor_resources["borrower"], num_cpus=0)
        .remote()
    )
    ray.get(worker_2.warmup.remote())

    refs = ray.get(worker_1.create_obj.remote())
    owner = ray.get_actor("_ray_global_owner_")
    print(
        "test object ref: ",
        refs[0],
        "global owner:",
        ray.ActorID(refs[0].global_owner_id()),
        "checkout url:",
        refs[0].spilled_url().decode("utf-8"),
    )
    ray.get(worker_2.send_refs.remote(refs))
    print("try get obj before kill:", ray.get(worker_2.try_get.remote()))
    print("old owner pid:", ray.get(owner.getpid.remote()))
    ray.get(worker_2.evict_all_object.remote())
    print("try get obj after kill:", ray.get(worker_2.try_get.remote()))
    ray.get(worker_2.evict_all_object.remote())
    try:
        ray.get(owner.exit.remote())
    except ray.exceptions.RayActorError:
        pass
    else:
        raise RuntimeError
    print("wait 5 seconds for owner died.")
    checkout_url = refs[0].spilled_url().decode("utf-8").split("?")[0]
    os.remove(checkout_url)
    time.sleep(5)
    print("new owner pid:", ray.get(owner.getpid.remote()))
    if actor_resources["creator"] != actor_resources["borrower"]:
        print("try get obj after kill:", ray.get(worker_2.try_get.remote()))
    else:
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.get(worker_2.try_get.remote(), timeout=10)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
