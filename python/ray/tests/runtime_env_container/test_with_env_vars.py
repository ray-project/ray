import argparse
import os

import ray

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
args = parser.parse_args()

worker_pth = "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py"  # noqa


@ray.remote(
    runtime_env={
        "container": {"image": args.image, "worker_path": worker_pth},
        "env_vars": {"TEST_DEF": "1"},
    }
)
def f():
    return os.environ.get("TEST_DEF")


assert ray.get(f.remote()) == "1"
