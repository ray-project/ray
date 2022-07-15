import resource
import pytest
import ray
import time
import numpy as np


def test_owner_failed(ray_start_cluster):
    cluster_node_config = [
        {"num_cpus": 1, "resources": {f"node{i+1}": 10}} for i in range(3)
    ]
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    OBJECT_NUMBER = 10

    @ray.remote(resources={"node1": 1}, num_cpus=0)
    class Creator:
        def gen_object_refs(self, owner):
            refs = []
            for _ in range(OBJECT_NUMBER):
                refs.append(ray.put(np.random.rand(2, 2), _owner=owner))
            ray.get(owner.set_object_refs.remote(refs))
            return refs

    @ray.remote(resources={"node2": 1}, num_cpus=0)
    class Owner:
        def __init__(self):
            self.refs = None

        def set_object_refs(self, refs):
            self.refs = refs

        def warmup(self):
            return 0

    @ray.remote(resources={"node3": 1}, num_cpus=0)
    class Borrower:
        def __init__(self):
            self.refs = None

        def set_object_refs(self, refs):
            self.refs = refs

        def get_objects(self):
            for ref in self.refs:
                ray.get(ref)
            return True

    owner = Owner.remote()
    creator = Creator.remote()
    borrower = Borrower.remote()

    # Make sure the owner actor is alive.
    ray.get(owner.warmup.remote())

    refs = ray.get(creator.gen_object_refs.remote(owner))
    ray.get(borrower.set_object_refs.remote(refs))

    ray.kill(owner)

    assert ray.get(borrower.get_objects.remote(), timeout=60)


@pytest.mark.parametrize(
    "actor_resources",
    [
        dict(zip(["owner", "creator", "borrower"], [{f"node{i}": 1} for i in _]))
        for _ in [
            [1, 2, 3],  # None of them is on the same node.
            # [1, 1, 3],  # Owner and creator are on the same node.
            # [3, 2, 3],  # Owner and borrower are on the same node.
            # [1, 3, 3],  # Creator and borrower are on the same node.
            # [3, 3, 3],  # All of them are on the same node.
        ]
    ],
)
def test_checkpoint(ray_start_cluster, actor_resources):
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(3)
    ]
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    @ray.remote(resources=actor_resources["owner"], num_cpus=0, max_restarts=100)
    class Owner:
        def __init__(self):
            self.refs = None

        def set_object_refs(self, refs):
            self.refs = refs

        def warmup(self):
            return 0

        def exit(self):
            raise RuntimeError

    owner = Owner.remote()
    ray.get(owner.warmup.remote())

    ref = ray.put("test_data", _owner=owner)
    spilled_url = ref.spilled_url()
    print("spilled_url:", spilled_url, "len:", len(spilled_url))
    print("ref:", ref)
    print("data:", ray.get(ref))
    try:
        ray.get(owner.exit.remote())
    except RuntimeError:
        pass
    else:
        raise RuntimeError
    print("wait 5 seconds for owner died.")
    time.sleep(5)
    print("data:", ray.get(ref))

    class Worker:
        def __init__(self):
            self.refs = None

        def create_obj(self, owner):
            self.refs = [
                ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8), _owner=owner)
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
            raise RuntimeError

    owner = Owner.remote()
    ray.get(owner.warmup.remote())
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

    refs = ray.get(worker_1.create_obj.remote(owner))
    print("test object ref: ", refs[0], "global owner:", ray.ActorID(refs[0].global_owner_id()))
    ray.get(worker_2.send_refs.remote(refs))
    print("try get obj before kill:", ray.get(worker_2.try_get.remote()))
    ray.get(worker_2.evict_all_object.remote())
    try:
        ray.get(owner.exit.remote())
    except RuntimeError:
        pass
    else:
        raise RuntimeError
    print("wait 5 seconds for owner died.")
    time.sleep(5)
    print("try get obj after kill:", ray.get(worker_2.try_get.remote()))
    ray.get(worker_2.evict_all_object.remote())
    try:
        ray.get(worker_1.exit.remote())
    except RuntimeError:
        pass
    else:
        raise RuntimeError
    print("wait 5 seconds for creater died.")
    time.sleep(5)
    ray.get(owner.warmup.remote())
    print("try get obj after kill:", ray.get(worker_2.try_get.remote()))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
