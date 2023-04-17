import atexit
import gymnasium as gym
from gymnasium.spaces import Discrete
import os
import subprocess


class EnvWithSubprocess(gym.Env):
    """An env that spawns a subprocess."""

    # Dummy command to run as a subprocess with a unique name
    UNIQUE_CMD = "sleep 20"

    def __init__(self, config):
        self.UNIQUE_FILE_0 = config["tmp_file1"]
        self.UNIQUE_FILE_1 = config["tmp_file2"]
        self.UNIQUE_FILE_2 = config["tmp_file3"]
        self.UNIQUE_FILE_3 = config["tmp_file4"]

        self.action_space = Discrete(2)
        self.observation_space = Discrete(2)
        # Subprocess that should be cleaned up.
        self.subproc = subprocess.Popen(self.UNIQUE_CMD.split(" "), shell=False)
        self.config = config
        # Exit handler should be called.
        atexit.register(lambda: self.subproc.kill())
        if config.worker_index == 0:
            atexit.register(lambda: os.unlink(self.UNIQUE_FILE_0))
        else:
            atexit.register(lambda: os.unlink(self.UNIQUE_FILE_1))

    def close(self):
        if self.config.worker_index == 0:
            os.unlink(self.UNIQUE_FILE_2)
        else:
            os.unlink(self.UNIQUE_FILE_3)

    def reset(self, *, seed=None, options=None):
        return 0, {}

    def step(self, action):
        return 0, 0, True, False, {}
