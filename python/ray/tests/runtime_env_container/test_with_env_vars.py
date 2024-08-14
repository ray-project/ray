import argparse
import os

import ray

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()

if args.use_image_uri_api:
    runtime_env = {
        "image_uri": args.image,
        "env_vars": {"TEST_DEF": "1"},
    }
else:
    runtime_env = {
        "container": {"image": args.image},
        "env_vars": {"TEST_DEF": "hi world"},
    }


@ray.remote(runtime_env=runtime_env)
def f():
    return os.environ.get("TEST_DEF")


assert ray.get(f.remote()) == "hi world"
