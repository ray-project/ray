import numpy as np

import ray
import ray.autoscaler.sdk

from time import sleep, perf_counter
from tqdm import tqdm, trange

NUM_NODES = 50
OBJECT_SIZE = 2 ** 30


def scale_to(target):
    ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
    while len(ray.nodes()) != target:
        print(f"Current # nodes: {len(ray.nodes())}, target: {target}")
        print("Waiting ...")
        sleep(5)


def test_object_broadcast():
    scale_to(NUM_NODES)

    @ray.remote(num_cpus=1, resources={"node": 1})
    class Actor:
        def foo(self):
            pass

        def sum(self, arr):
            return np.sum(arr)

    actors = [Actor.remote() for _ in range(NUM_NODES)]

    arr = np.ones(OBJECT_SIZE, dtype=np.uint8)
    ref = ray.put(arr)

    for actor in tqdm(actors, desc="Ensure all actors have started."):
        ray.get(actor.foo.remote())

    result_refs = []
    for actor in tqdm(actors, desc="Broadcasting objects"):
        result_refs.append(actor.sum.remote(ref))

    results = ray.get(result_refs)
    for result in results:
        assert result == OBJECT_SIZE


ray.init(address="auto")
start = perf_counter()
test_object_broadcast()
end = perf_counter()
print(f"Broadcast time: {end - start} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")
