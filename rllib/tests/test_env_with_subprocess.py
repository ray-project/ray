"""Tests that envs clean up after themselves on agent exit."""
import os
import subprocess
import tempfile

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.examples.env.env_with_subprocess import EnvWithSubprocess
from ray._private.test_utils import wait_for_condition


def leaked_processes():
    """Returns whether any subprocesses were leaked."""
    result = subprocess.check_output(
        "ps aux | grep '{}' | grep -v grep || true".format(
            EnvWithSubprocess.UNIQUE_CMD
        ),
        shell=True,
    )
    return result


if __name__ == "__main__":

    # Create 4 temp files, which the Env has to clean up after it's done.
    _, tmp1 = tempfile.mkstemp("test_env_with_subprocess")
    _, tmp2 = tempfile.mkstemp("test_env_with_subprocess")
    _, tmp3 = tempfile.mkstemp("test_env_with_subprocess")
    _, tmp4 = tempfile.mkstemp("test_env_with_subprocess")
    register_env("subproc", lambda c: EnvWithSubprocess(c))

    ray.init()
    # Check whether everything is ok.
    assert os.path.exists(tmp1)
    assert os.path.exists(tmp2)
    assert os.path.exists(tmp3)
    assert os.path.exists(tmp4)
    assert not leaked_processes()

    run_experiments(
        {
            "demo": {
                "run": "PG",
                "env": "subproc",
                "num_samples": 1,
                "config": {
                    "num_workers": 1,
                    "env_config": {
                        "tmp_file1": tmp1,
                        "tmp_file2": tmp2,
                        "tmp_file3": tmp3,
                        "tmp_file4": tmp4,
                    },
                    "framework": "tf",
                },
                "stop": {"training_iteration": 1},
            },
        }
    )

    ray.shutdown()

    # Check whether processes are still running.
    wait_for_condition(lambda: not leaked_processes(), timeout=30)

    print("OK")
