import argparse
import asyncio
import glob
import hashlib
import json
import logging
import os
import subprocess
import sys
import tempfile

import ray
from ray._private.runtime_env.image_uri import (
    ImageMetadata,
    ImageURIPlugin,
    _write_install_env,
)

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


async def check_install_env_transport():
    # Exercise the real podman transport with values that --env-file truncated
    # or rejected. Compare hashes so values never need to enter logged argv.
    with open(manifests[0], encoding="utf-8") as file:
        metadata = ImageMetadata(**json.load(file)["image"])
    install_env = {
        "MULTILINE": "first\nsecond\r\n",
        "LONG": "x" * 70000,
        "QUOTED": "\"'$VALUE`command`中文",
        "EMPTY": "",
        "# NAME\n": "valid OS name",
        "PATH": "/synthetic/job/bin",
    }
    expected = hashlib.sha256(
        json.dumps(install_env, sort_keys=True).encode()
    ).hexdigest()
    script = (
        "import hashlib,json,os,sys; "
        "env={name:os.environ[name] for name in sys.argv[1:]}; "
        "print('ENV_DIGEST=' + hashlib.sha256("
        "json.dumps(env,sort_keys=True).encode()).hexdigest())"
    )
    plugin = ImageURIPlugin("/tmp/ray")
    with tempfile.TemporaryDirectory(prefix="ray-install-env-test-") as directory:
        env_file = os.path.join(directory, "env.json")
        _write_install_env(env_file, install_env)
        staging = os.path.join(directory, "staging")
        os.mkdir(staging)
        output = await plugin._run_in_image(
            metadata,
            staging,
            staging,
            metadata.python_executable,
            ["-c", script, *install_env],
            env_file,
            logging.getLogger(__name__),
        )
        assert f"ENV_DIGEST={expected}" in output


asyncio.run(check_install_env_transport())

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
