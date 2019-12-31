from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pickle
import sys

from ray.tune import DurableTrainable
from ray.tune.sync_client import get_sync_client

if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


def mock_storage_client():
    """Mock storage client that treats a local dir as durable storage."""
    sync = "mkdir -p {target} && rsync -avz {source}/ {target}/"
    delete = "rm -rf {target}"
    return get_sync_client(sync, delete)


class MockDurableTrainable(DurableTrainable):
    """Mock Trainable used for tests."""

    def __init__(self, remote_checkpoint_dir, *args, **kwargs):
        mock_module = "ray.tune.durable_trainable.get_cloud_sync_client"
        with patch(mock_module) as mock_get_cloud_client:
            mock_get_cloud_client.return_value = mock_storage_client()
            super(MockDurableTrainable, self).__init__(remote_checkpoint_dir,
                                                       *args, **kwargs)

    def _setup(self, config):
        self.info = config.get("info", 0)

    def _train(self):
        result = dict(mean_accuracy=0.9, timesteps_this_iter=10)
        return result

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "mock_agent.pkl")
        with open(path, "wb") as f:
            pickle.dump(self.info, f)
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path, "rb") as f:
            info = pickle.load(f)
        self.info = info
