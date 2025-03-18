import ray
from pathlib import Path
import re
from ray.util.state import list_tasks
from ray._private.test_utils import wait_for_condition
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()

ray.init(num_cpus=1)

session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
session_path = Path(session_dir)
log_dir_path = session_path / "logs"


def task_finished():
    tasks = list_tasks()
    assert len(tasks) > 0
    assert tasks[0].worker_id
    assert tasks[0].worker_pid
    assert tasks[0].state == "FINISHED"
    return True


if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image}}


# Run a basic workload.
@ray.remote(runtime_env=runtime_env)
def f():
    for i in range(10):
        print(f"test {i}")


f.remote()
wait_for_condition(task_finished)

task_state = list_tasks()[0]
worker_id = task_state.worker_id
worker_pid = task_state.worker_pid
print(f"Worker ID: {worker_id}")
print(f"Worker PID: {worker_pid}")

paths = [path.name for path in log_dir_path.iterdir()]
assert f"python-core-worker-{worker_id}_{worker_pid}.log" in paths
assert any(re.search(f"^worker-{worker_id}-.*-{worker_pid}.err$", p) for p in paths)
assert any(re.search(f"^worker-{worker_id}-.*-{worker_pid}.out$", p) for p in paths)
