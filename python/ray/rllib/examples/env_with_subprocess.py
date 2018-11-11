"""Example of how to write an env that launches a subprocess.

When the process exists, the subprocess's entire process group is killed via
a hook to avoid leaking resources.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import atexit
import gym
import os
import signal
import subprocess

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env

# Dummy command to run as a subprocess
SLEEP_CMD = "sleep 12345"


class EnvWithSubprocess(gym.Env):
    """Our env that spawns a subprocess."""

    def __init__(self):
        self.action_space = Discrete(2)
        self.observation_space = Discrete(2)

        # We use os.setsid so that the process becomes a group leader. This
        # allows us to easily kill its children as well via os.killpg().
        self.subproc = subprocess.Popen(
            SLEEP_CMD, preexec_fn=os.setsid, shell=True)

        # Cleanup function to run on exit
        def cleanup():
            print("Killing child process group")
            os.killpg(os.getpgid(self.subproc.pid), signal.SIGKILL)
            # Note that this isn't sufficient to kill nested child processes:
            # self.subproc.kill()

        atexit.register(cleanup)

        # Patch _exit to handle normal exits as well
        real_os_exit = os._exit
        os._exit = lambda a: [cleanup(), real_os_exit(a)]

    def reset(self):
        return [0]

    def step(self, action):
        return [0], 0, True, {}


def leaked_processes():
    """Returns whether any subprocesses were leaked."""
    result = subprocess.check_output(
        "ps aux | grep '{}' | grep -v grep || true".format(SLEEP_CMD),
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
