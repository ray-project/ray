import ray
import ray.autoscaler.sdk

from time import sleep

MAX_NUM_NODES = 1000

def scale_to(target):
    ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
    while len(ray.nodes()) != target:
        sleep(5)

def test_nodes():
    scale_to(MAX_NUM_NODES)
    assert len(ray.nodes()) == MAX_NUM_NODES

ray.init(address="auto")

test_nodes()

