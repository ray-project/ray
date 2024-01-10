import ray
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()


@ray.remote(runtime_env={"container": {"image": args.image}})
def create_ref():
    with open("file.txt") as f:
        assert f.read().strip() == "helloworldalice"

    ref = ray.put(np.zeros(100_000_000))
    return ref


wrapped_ref = create_ref.remote()
assert (ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)).all()
