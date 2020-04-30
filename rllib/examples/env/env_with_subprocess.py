from gym.spaces import Discrete
import atexit
import gym
import os
import subprocess
import tempfile
import time


class EnvWithSubprocess(gym.Env):
    """Our env that spawns a subprocess."""

    # Dummy command to run as a subprocess with a unique name
    UNIQUE_CMD = "sleep {}".format(str(time.time()))

    _, UNIQUE_FILE_0 = tempfile.mkstemp("test_env_with_subprocess")
    _, UNIQUE_FILE_1 = tempfile.mkstemp("test_env_with_subprocess")
    _, UNIQUE_FILE_2 = tempfile.mkstemp("test_env_with_subprocess")
    _, UNIQUE_FILE_3 = tempfile.mkstemp("test_env_with_subprocess")

    def __init__(self, config):
        self.action_space = Discrete(2)
        self.observation_space = Discrete(2)
        # Subprocess that should be cleaned up
        self.subproc = subprocess.Popen(
            self.UNIQUE_CMD.split(" "), shell=False)
        self.config = config
        # Exit handler should be called
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

    def reset(self):
        return 0

    def step(self, action):
        return 0, 0, True, {}
