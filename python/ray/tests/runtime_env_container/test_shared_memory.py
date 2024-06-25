import ray
import numpy as np
import sys
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()

if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image}}


@ray.remote(runtime_env=runtime_env)
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
