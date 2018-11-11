"""Tests that envs clean up after themselves on agent exit."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import gym
import subprocess
import time

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env

# Dummy command to run as a subprocess with a unique name
UNIQUE_CMD = "sleep {}".format(str(time.time()))


class EnvWithSubprocess(gym.Env):
    """Our env that spawns a subprocess."""

    def __init__(self):
        self.action_space = Discrete(2)
        self.observation_space = Discrete(2)
        self.subproc = subprocess.Popen(UNIQUE_CMD, shell=True)

    def reset(self):
        return [0]

    def step(self, action):
        return [0], 0, True, {}


def leaked_processes():
    """Returns whether any subprocesses were leaked."""
    result = subprocess.check_output(
        "ps aux | grep '{}' | grep -v grep || true".format(UNIQUE_CMD),
        shell=True)
    return result


if __name__ == "__main__":
    register_env("subproc", lambda config: EnvWithSubprocess())
    ray.init()
    assert not leaked_processes(), \
        "Some leaked processes from before, try running 'killall sleep'?"
    run_experiments({
        "demo": {
            "run": "PG",
            "env": "subproc",
            "num_samples": 1,
            "stop": {
                "training_iteration": 1
            },
        },
    })
    leaked = leaked_processes()
    assert not leaked, "LEAKED PROCESSES: {}".format(leaked)
    print("OK")
