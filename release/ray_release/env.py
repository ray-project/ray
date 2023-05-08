import os
from typing import Dict

from ray_release.exception import ReleaseTestConfigError

DEFAULT_ENVIRONMENT = "aws"


def load_environment(environment_name: str) -> Dict[str, str]:
    this_dir = os.path.dirname(__file__)
    env_file = os.path.join(this_dir, "environments", f"{environment_name}.env")

    if not os.path.exists(env_file):
        raise ReleaseTestConfigError(
            f"Unknown environment with name: {environment_name}"
        )

    env = {}
    with open(env_file, "r") as f:
        for line in f.readlines():
            if not line:
                continue
            key, val = line.strip().split("=", maxsplit=1)
            env[key] = val.strip('"')

    return env


def populate_os_env(env: Dict[str, str]) -> None:
    for k, v in env.items():
        os.environ[k] = v
