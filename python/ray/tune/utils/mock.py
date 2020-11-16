import os
import numpy as np
import json

import ray.utils

from ray.rllib.agents.mock import _MockTrainer
from ray.tune import DurableTrainable, Trainable
from ray.tune.sync_client import get_sync_client
from ray.tune.syncer import NodeSyncer

MOCK_REMOTE_DIR = os.path.join(ray.utils.get_user_temp_dir(),
                               "mock-tune-remote") + os.sep
# Sync and delete templates that operate on local directories.
LOCAL_SYNC_TEMPLATE = "mkdir -p {target} && rsync -avz {source}/ {target}/"
LOCAL_DELETE_TEMPLATE = "rm -rf {target}"


def mock_storage_client():
    """Mocks storage client that treats a local dir as durable storage."""
    return get_sync_client(LOCAL_SYNC_TEMPLATE, LOCAL_DELETE_TEMPLATE)


class MockNodeSyncer(NodeSyncer):
    """Mock NodeSyncer that syncs to and from /tmp"""

    def has_remote_target(self):
        return True

    @property
    def _remote_path(self):
        if self._remote_dir.startswith("/"):
            self._remote_dir = self._remote_dir[1:]
        return os.path.join(MOCK_REMOTE_DIR, self._remote_dir)


class MockRemoteTrainer(_MockTrainer):
    """Mock Trainable that saves at tmp for simulated clusters."""

    def __init__(self, *args, **kwargs):
        super(MockRemoteTrainer, self).__init__(*args, **kwargs)
        if self._logdir.startswith("/"):
            self._logdir = self._logdir[1:]
        self._logdir = os.path.join(MOCK_REMOTE_DIR, self._logdir)
        if not os.path.exists(self._logdir):
            os.makedirs(self._logdir)


class MockDurableTrainer(DurableTrainable, _MockTrainer):
    """Mock DurableTrainable that saves at tmp for simulated clusters."""

    # TODO(ujvl): This class uses multiple inheritance; it should be cleaned
    #  up once the durable training API converges.

    def __init__(self, remote_checkpoint_dir, *args, **kwargs):
        _MockTrainer.__init__(self, *args, **kwargs)
        DurableTrainable.__init__(self, remote_checkpoint_dir, *args, **kwargs)

    def _create_storage_client(self):
        return mock_storage_client()


class MyTrainableClass(Trainable):
    """Example agent whose learning curve is a random sigmoid.

    The dummy hyperparameters "width" and "height" determine the slope and
    maximum reward value reached.
    """

    def setup(self, config):
        self.timestep = 0

    def step(self):
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
        return path

    def load_checkpoint(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.timestep = json.loads(f.read())["timestep"]
