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
def f(env_var_name: str):
    return os.environ.get(env_var_name)


os.environ["TEST_SCRIPT_ENV_VAR"] = "hi from driver"

# Set in runtime environment `env_vars`, should be picked up
assert ray.get(f.remote("TEST_DEF")) == "hi world"
# Environment variables that start with prefix "RAY_" should be
# inherited from host environment
assert ray.get(f.remote("RAY_TEST_ABC")) == "1"
# Environment variable from driver should not be inherited
assert not ray.get(f.remote("TEST_SCRIPT_ENV_VAR"))
