import argparse
import glob
import os
import subprocess
import sys

import ray

parser = argparse.ArgumentParser()
parser.add_argument("--image", required=True)
args = parser.parse_args()

runtime_env = {
    "image_uri": f"podman://{args.image}",
    "pip": ["/tmp/pip_install_test-0.5-py3-none-any.whl"],
}


@ray.remote(runtime_env=runtime_env)
def dependency_location():
    import pip_install_test

    try:
        with open(os.path.join(sys.prefix, "write-test"), "w", encoding="utf-8"):
            pass
        cache_is_read_only = False
    except OSError:
        cache_is_read_only = True
    return sys.executable, pip_install_test.__file__, cache_is_read_only


results = ray.get([dependency_location.remote() for _ in range(20)])
executables = {executable for executable, _, _ in results}
module_paths = {module_path for _, module_path, _ in results}

assert len(executables) == 1, executables
assert len(module_paths) == 1, module_paths
assert "/runtime_resources/image_uri/" in next(iter(executables))
assert "/runtime_resources/image_uri/" in next(iter(module_paths))
assert all(cache_is_read_only for _, _, cache_is_read_only in results)

manifests = glob.glob(
    os.path.join(
        "/tmp/ray/session_latest/runtime_resources/image_uri", "*", "manifest.json"
    )
)
assert len(manifests) == 1, manifests

base_image_import = subprocess.run(
    [
        "podman",
        "run",
        "--rm",
        "--entrypoint",
        "python",
        args.image,
        "-c",
        "import pip_install_test",
    ],
    capture_output=True,
    text=True,
)
assert base_image_import.returncode != 0, base_image_import.stdout
