import ray
import numpy as np
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
def create_ref():
    with open("file.txt") as f:
        assert f.read().strip() == "helloworldalice"

    ref = ray.put(np.zeros(100_000_000))
    return ref


wrapped_ref = create_ref.remote()
assert (ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)).all()
