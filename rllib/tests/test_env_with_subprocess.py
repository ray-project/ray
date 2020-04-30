"""Tests that envs clean up after themselves on agent exit."""
import os
import subprocess
import time

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.examples.env.env_with_subprocess import EnvWithSubprocess


def leaked_processes():
    """Returns whether any subprocesses were leaked."""
    result = subprocess.check_output(
        "ps aux | grep '{}' | grep -v grep || true".format(
            EnvWithSubprocess.UNIQUE_CMD), shell=True)
    return result


if __name__ == "__main__":
    register_env("subproc", lambda config: EnvWithSubprocess(config))
    ray.init()
    assert os.path.exists(EnvWithSubprocess.UNIQUE_FILE_0)
    assert os.path.exists(EnvWithSubprocess.UNIQUE_FILE_1)
    assert not leaked_processes()
    run_experiments({
        "demo": {
            "run": "PG",
            "env": "subproc",
            "num_samples": 1,
            "config": {
                "num_workers": 1,
            },
            "stop": {
                "training_iteration": 1
            },
        },
    })
    time.sleep(10.0)
    leaked = leaked_processes()
    assert not leaked, "LEAKED PROCESSES: {}".format(leaked)
    assert not os.path.exists(EnvWithSubprocess.UNIQUE_FILE_0), \
        "atexit handler not called"
    assert not os.path.exists(EnvWithSubprocess.UNIQUE_FILE_1), \
        "atexit handler not called"
    assert not os.path.exists(EnvWithSubprocess.UNIQUE_FILE_2), \
        "close not called"
    assert not os.path.exists(EnvWithSubprocess.UNIQUE_FILE_3), \
        "close not called"
    print("OK")
