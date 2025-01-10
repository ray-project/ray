import json
import os
import time

import numpy as np

from ray.tune import Trainable

MOCK_TRAINABLE_NAME = "mock_trainable"
MOCK_ERROR_KEY = "mock_error"


class MyTrainableClass(Trainable):
    """Example agent whose learning curve is a random sigmoid.

    The dummy hyperparameters "width" and "height" determine the slope and
    maximum reward value reached.
    """

    def setup(self, config):
        self._sleep_time = config.get("sleep", 0)
        self._mock_error = config.get(MOCK_ERROR_KEY, False)
        self._persistent_error = config.get("persistent_error", False)

        self.timestep = 0
        self.restored = False

    def step(self):
        if (
            self._mock_error
            and self.timestep > 0  # allow at least 1 successful checkpoint.
            and (self._persistent_error or not self.restored)
        ):
            raise RuntimeError(f"Failing on purpose! {self.timestep=}")

        if self._sleep_time > 0:
            time.sleep(self._sleep_time)

        self.timestep += 1
        v = np.tanh(float(self.timestep) / self.config.get("width", 1))
        v *= self.config.get("height", 1)

        # Here we use `episode_reward_mean`, but you can also report other
        # objectives such as loss or accuracy.
        return {"episode_reward_mean": v}

    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps({"timestep": self.timestep}))

    def load_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "r") as f:
            self.timestep = json.loads(f.read())["timestep"]

        self.restored = True


def register_mock_trainable():
    from ray.tune import register_trainable

    register_trainable(MOCK_TRAINABLE_NAME, MyTrainableClass)
