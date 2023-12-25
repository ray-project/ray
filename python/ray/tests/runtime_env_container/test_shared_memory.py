import ray
import numpy as np
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()

worker_pth = "/home/ray/anaconda3/lib/python3.9/site-packages/ray/_private/workers/default_worker.py"  # noqa


@ray.remote(runtime_env={"container": {"image": args.image, "worker_path": worker_pth}})
def f():
    array = np.random.rand(5000, 5000)
    return ray.put(array)


ray.init()
ref = ray.get(f.remote())
val = ray.get(ref)
size = sys.getsizeof(val)
assert size < sys.getsizeof(np.random.rand(5000, 5000))
print(f"Size of result fetched from ray.put: {size}")
assert val.shape == (5000, 5000)
