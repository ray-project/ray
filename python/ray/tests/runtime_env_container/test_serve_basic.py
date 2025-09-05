import argparse

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.handle import DeploymentHandle

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


@serve.deployment(ray_actor_options={"runtime_env": runtime_env})
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
