import os
from typing import Dict
import pytest
import sys
from ray.exceptions import RuntimeEnvSetupError
from ray._private.test_utils import wait_for_condition, generate_runtime_env_dict
import ray

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("specify_env_in_init", [False, True])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_install_failure_logging(
    start_cluster, specify_env_in_init, field, spec_format, tmp_path, capsys
):
    cluster, address = start_cluster
    using_ray_client = address.startswith("ray://")

    bad_envs: Dict[str, Dict] = {}
    bad_packages: Dict[str, str] = {}
    for scope in "init", "actor", "task":
        bad_packages[scope] = "doesnotexist" + scope
        bad_envs[scope] = generate_runtime_env_dict(
            field, spec_format, tmp_path, pip_list=[bad_packages[scope]]
        )

    if specify_env_in_init:
        if using_ray_client:
            with pytest.raises(ConnectionAbortedError) as excinfo:
                ray.init(address, runtime_env=bad_envs["init"])
            assert bad_packages["init"] in str(excinfo.value)
        else:
            ray.init(address, runtime_env=bad_envs["init"])
            wait_for_condition(
                lambda: bad_packages["init"] in capsys.readouterr().out, timeout=30
            )
        return

    ray.init(address)

    @ray.remote(runtime_env=bad_envs["actor"])
    class A:
        pass

    a = A.remote()  # noqa

    wait_for_condition(
        lambda: bad_packages["actor"] in capsys.readouterr().out, timeout=30
    )

    @ray.remote(runtime_env=bad_envs["task"])
    def f():
        pass

    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.remote())

    wait_for_condition(
        lambda: bad_packages["task"] in capsys.readouterr().out, timeout=30
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
