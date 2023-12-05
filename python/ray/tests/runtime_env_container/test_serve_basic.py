import argparse
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.handle import DeploymentHandle

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


@serve.deployment(
    ray_actor_options={
        "runtime_env": {
            "container": {"image": args.image, "worker_path": args.worker_path}
        }
    }
)
class Model:
    def __call__(self):
        with open("file.txt") as f:
            return f.read().strip()


def check_application(app_handle: DeploymentHandle, expected: str):
    ref = app_handle.remote()
    assert ref.result() == expected
    return True


h = serve.run(Model.bind())
wait_for_condition(
    check_application,
    app_handle=h,
    expected="helloworldalice",
    timeout=300,
)
