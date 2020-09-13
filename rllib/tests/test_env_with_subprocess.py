"""Tests that envs clean up after themselves on agent exit."""
import os
import subprocess
import tempfile
import time

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.examples.env.env_with_subprocess import EnvWithSubprocess


def leaked_processes():
    """Returns whether any subprocesses were leaked."""
    result = subprocess.check_output(
        "ps aux | grep '{}' | grep -v grep || true".format(
            EnvWithSubprocess.UNIQUE_CMD),
        shell=True)
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

    run_experiments({
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
            "stop": {
                "training_iteration": 1
            },
        },
    })
    time.sleep(10.0)
    # Check whether processes are still running or Env has not cleaned up
    # the given tmp files.
    leaked = leaked_processes()
    assert not leaked, "LEAKED PROCESSES: {}".format(leaked)
    assert not os.path.exists(tmp1), "atexit handler not called"
    assert not os.path.exists(tmp2), "atexit handler not called"
    assert not os.path.exists(tmp3), "close not called"
    assert not os.path.exists(tmp4), "close not called"
    print("OK")
