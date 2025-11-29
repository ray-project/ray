import os
import subprocess
import sys

import ray
import ray._private.ray_constants as ray_constants
from ray._common.network_utils import find_free_port, parse_address
from ray._private.utils import read_ray_address


def _run_python(code: str, env: dict):
    subprocess.check_call([sys.executable, "-c", code], env=env)


def test_ray_init_dynamic_gcs_port():
    # Case 1: ray.init starts a head node and exposes a dynamic GCS port.
    env = os.environ.copy()

    ray.init()
    gcs_address = ray._private.worker._global_node.gcs_address
    _, port = parse_address(gcs_address)
    assert int(port) > 0

    # Case 2: Connect a driver via address="auto" using the address file.
    code = (
        "import ray\n"
        "ray.init(address='auto')\n"
        "ray.get(ray.remote(lambda: 1).remote())\n"
        "ray.shutdown()\n"
    )
    _run_python(code, env)

    # Case 3: Connect a driver via an explicit address.
    code = (
        "import ray\n"
        f"ray.init(address='{gcs_address}')\n"
        "ray.get(ray.remote(lambda: 1).remote())\n"
        "ray.shutdown()\n"
    )
    _run_python(code, env)
    ray.shutdown()


def test_ray_start_dynamic_gcs_port():
    # Case 4: CLI starts head with dynamic GCS port.
    env = os.environ.copy()

    head_cmd = [
        "ray",
        "start",
        "--head",
        "--port",
        "0",
    ]

    subprocess.check_call(head_cmd, env=env)
    gcs_address = read_ray_address()
    _, gcs_port = parse_address(gcs_address)
    assert int(gcs_port) > 0

    # Case 5: CLI starts worker connecting to the head via GCS address.
    worker_cmd = [
        "ray",
        "start",
        "--address",
        gcs_address,
    ]
    subprocess.check_call(worker_cmd, env=env)

    code = (
        "import ray\n"
        f"ray.init(address='{gcs_address}')\n"
        "ray.get(ray.remote(lambda: 1).remote())\n"
        "ray.shutdown()\n"
    )
    _run_python(code, env)

    subprocess.check_call(
        ["ray", "stop", "--force"],
        env=env,
    )


def test_ray_start_fixed_gcs_port():
    # Case 6: CLI starts head with an explicit GCS port.
    env = os.environ.copy()
    fixed_port = find_free_port()

    subprocess.check_call(
        [
            "ray",
            "start",
            "--head",
            "--port",
            str(fixed_port),
        ],
        env=env,
    )
    gcs_address = read_ray_address()
    _, gcs_port = parse_address(gcs_address)
    assert int(gcs_port) == fixed_port

    _run_python(
        (
            "import ray\n"
            f"ray.init(address='{gcs_address}')\n"
            "ray.get(ray.remote(lambda: 1).remote())\n"
            "ray.shutdown()\n"
        ),
        env,
    )

    subprocess.check_call(["ray", "stop", "--force"], env=env)


def test_ray_start_default_gcs_port():
    # Case 7: CLI starts head with default GCS port.
    env = os.environ.copy()
    env.pop(ray_constants.GCS_PORT_ENVIRONMENT_VARIABLE, None)

    subprocess.check_call(
        [
            "ray",
            "start",
            "--head",
        ],
        env=env,
    )
    gcs_address = read_ray_address()
    _, gcs_port = parse_address(gcs_address)
    assert int(gcs_port) == ray_constants.DEFAULT_PORT

    _run_python(
        (
            "import ray\n"
            f"ray.init(address='{gcs_address}')\n"
            "ray.get(ray.remote(lambda: 1).remote())\n"
            "ray.shutdown()\n"
        ),
        env,
    )

    subprocess.check_call(["ray", "stop", "--force"], env=env)
