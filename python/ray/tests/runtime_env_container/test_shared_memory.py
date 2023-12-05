import ray
import numpy as np
import sys
import argparse

parser = argparse.ArgumentParser(
    description="Example Python script taking command line arguments."
)
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--worker-path",
    type=str,
    help="The path to `default_worker.py` inside the container.",
)
args = parser.parse_args()


@ray.remote(
    runtime_env={"container": {"image": args.image, "worker_path": args.worker_path}}
)
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
