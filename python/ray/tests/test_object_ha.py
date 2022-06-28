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


def test_checkpoint(ray_start_cluster):
    cluster_node_config = [
        {"num_cpus": 10, "resources": {f"node{i+1}": 10}} for i in range(3)
    ]
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    @ray.remote(resources={"node2": 1}, num_cpus=0)
    class Owner:
        def __init__(self):
            self.refs = None

        def set_object_refs(self, refs):
            self.refs = refs

        def warmup(self):
            return 0

    owner = Owner.remote()
    ray.get(owner.warmup.remote())

    ref = ray.put("test_data", _owner=owner)
    checkpoint_url = ref.checkpoint_url()
    print("checkpoint_url:", checkpoint_url, "len:", len(checkpoint_url))
    print("ref:", ref)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
