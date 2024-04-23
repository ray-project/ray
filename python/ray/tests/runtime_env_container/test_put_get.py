import ray
import numpy as np
import argparse
from ray._private.test_utils import get_ray_default_worker_file_path

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()

worker_pth = get_ray_default_worker_file_path()


@ray.remote(runtime_env={"container": {"image": args.image, "worker_path": worker_pth}})
def create_ref():
    with open("file.txt") as f:
        assert f.read().strip() == "helloworldalice"

    ref = ray.put(np.zeros(100_000_000))
    return ref


wrapped_ref = create_ref.remote()
assert (ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)).all()
