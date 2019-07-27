"""Tests that envs clean up after themselves on agent exit."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import atexit
import gym
import os
import subprocess
import tempfile
import time

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env

# Dummy command to run as a subprocess with a unique name
UNIQUE_CMD = "sleep {}".format(str(time.time()))
_, UNIQUE_FILE_0 = tempfile.mkstemp("test_env_with_subprocess")
_, UNIQUE_FILE_1 = tempfile.mkstemp("test_env_with_subprocess")
_, UNIQUE_FILE_2 = tempfile.mkstemp("test_env_with_subprocess")
_, UNIQUE_FILE_3 = tempfile.mkstemp("test_env_with_subprocess")


class EnvWithSubprocess(gym.Env):
    """Our env that spawns a subprocess."""

    def __init__(self, config):
        self.action_space = Discrete(2)
        self.observation_space = Discrete(2)
        # Subprocess that should be cleaned up
        self.subproc = subprocess.Popen(UNIQUE_CMD.split(" "), shell=False)
        self.config = config
        # Exit handler should be called
        if config.worker_index == 0:
            atexit.register(lambda: os.unlink(UNIQUE_FILE_0))
        else:
            atexit.register(lambda: os.unlink(UNIQUE_FILE_1))
        atexit.register(lambda: self.subproc.kill())

    def close(self):
        if self.config.worker_index == 0:
            os.unlink(UNIQUE_FILE_2)
        else:
            os.unlink(UNIQUE_FILE_3)

    def reset(self):
        return 0

    def step(self, action):
        return 0, 0, True, {}


def leaked_processes():
    """Returns whether any subprocesses were leaked."""
    result = subprocess.check_output(
        "ps aux | grep '{}' | grep -v grep || true".format(UNIQUE_CMD),
        shell=True)
    return result


if __name__ == "__main__":
    register_env("subproc", lambda config: EnvWithSubprocess(config))
    ray.init()
    assert os.path.exists(UNIQUE_FILE_0)
    assert os.path.exists(UNIQUE_FILE_1)
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
    time.sleep(5.0)
    leaked = leaked_processes()
    assert not leaked, "LEAKED PROCESSES: {}".format(leaked)
    assert not os.path.exists(UNIQUE_FILE_0), "atexit handler not called"
    assert not os.path.exists(UNIQUE_FILE_1), "atexit handler not called"
    assert not os.path.exists(UNIQUE_FILE_2), "close not called"
    assert not os.path.exists(UNIQUE_FILE_3), "close not called"
    print("OK")
