import argparse
import os
import tempfile

import ray

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()

working_dir = tempfile.mkdtemp()
with open(os.path.join(working_dir, "job_module.py"), "w") as f:
    f.write('VALUE = "from-working-dir"\n')

# The job-level working_dir is inherited by the containerized task below.
ray.init(num_cpus=1, runtime_env={"working_dir": working_dir})
runtime_env_dir = ray._private.worker.global_worker.node.get_runtime_env_dir_path()

# Stands in for a resource cached by another job on this node.
other_job_file = os.path.join(
    runtime_env_dir, "working_dir_files", "other_job", "other_job.py"
)
os.makedirs(os.path.dirname(other_job_file), exist_ok=True)
with open(other_job_file, "w") as f:
    f.write("ORIGINAL\n")

if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image}}


@ray.remote(runtime_env=runtime_env)
def probe():
    import job_module

    try:
        with open(job_module.__file__, "a") as f:
            f.write("# modified\n")
        own_working_dir_writable = True
    except OSError:
        own_working_dir_writable = False
    return {
        "job_module_value": job_module.VALUE,
        "other_job_file_visible": os.path.exists(other_job_file),
        "own_working_dir_writable": own_working_dir_writable,
    }


result = ray.get(probe.remote())
print(result)
assert result == {
    "job_module_value": "from-working-dir",
    "other_job_file_visible": False,
    "own_working_dir_writable": False,
}, result
with open(other_job_file) as f:
    assert f.read() == "ORIGINAL\n"
