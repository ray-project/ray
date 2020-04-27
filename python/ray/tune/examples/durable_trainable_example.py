import argparse
import numpy as np
import time
import logging
import os
import ray
from ray import tune
from ray.tune import DurableTrainable
from ray.tune.sync_client import get_sync_client

from ray import cloudpickle

logger = logging.getLogger(__name__)


class MockDurableTrainable(DurableTrainable):
    """Mocks the storage client on initialization to store data locally."""

    def __init__(self, remote_checkpoint_dir, *args, **kwargs):
        # Mock the path as a local path.
        local_dir_suffix = remote_checkpoint_dir.split("://")[1]
        remote_checkpoint_dir = os.path.join(ray.utils.get_user_temp_dir(),
                                             local_dir_suffix)
        # Disallow malformed relative paths for delete safety.
        assert os.path.abspath(remote_checkpoint_dir).startswith(
            ray.utils.get_user_temp_dir())
        logger.info("Using %s as the mocked remote checkpoint directory.",
                    self.remote_checkpoint_dir)
        super(MockDurableTrainable, self).__init__(remote_checkpoint_dir,
                                                   *args, **kwargs)

    def _create_storage_client(self):
        sync = "mkdir -p {target} && rsync -avz {source} {target}"
        delete = "rm -rf {target}"
        return get_sync_client(sync, delete)


class OptimusFn(object):
    def __init__(self, params, max_t=10000):
        self.params = params
        self.noise = np.random.normal(size=max_t) * 0.005

    def eval(self, k, add_noise=True):
        b0, b1, b2 = self.params
        score = (b0 * k / 100 + 0.1 * b1 + 0.5)**(-1) + b2 * 0.01
        if add_noise:
            return score + abs(self.noise[k])
        else:
            return score


def get_optimus_trainable(parent_cls):
    class OptimusTrainable(parent_cls):
        def _setup(self, config):
            self.iter = 0
            if config.get("seed"):
                np.random.seed(config["seed"])
            time.sleep(config.get("startup_delay", 0))
            params = [config["param1"], config["param2"], config["param3"]]
            self.func = OptimusFn(params=params)
            self.initial_samples_per_step = 500
            self.mock_data = open("/dev/urandom", "rb").read(1024)

        def _train(self):
            self.iter += 1
            new_loss = self.func.eval(self.iter)
            time.sleep(0.5)
            return {
                "mean_loss": float(new_loss),
                "mean_accuracy": (2 - new_loss) / 2,
                "samples": self.initial_samples_per_step
            }

        def _save(self, checkpoint_dir):
            time.sleep(0.5)
            return {
                "func": cloudpickle.dumps(self.func),
                "seed": np.random.get_state(),
                "data": self.mock_data,
                "iter": self.iter
            }

        def _restore(self, checkpoint):
            self.func = cloudpickle.loads(checkpoint["func"])
            self.data = checkpoint["data"]
            self.iter = checkpoint["iter"]
            np.random.set_state(checkpoint["seed"])

    return OptimusTrainable


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", default=False)
    parser.add_argument("--mock-storage", action="store_true", default=False)
    parser.add_argument("--remote-dir", type=str)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse()
    address = None if args.local else "auto"
    ray.init(address=address)

    config = {
        "seed": None,
        "startup_delay": 0.001,
        "param1": tune.sample_from(lambda spec: np.random.exponential(0.1)),
        "param2": tune.sample_from(lambda _: np.random.rand()),
        "param3": tune.sample_from(lambda _: np.random.rand()),
    }

    parent = MockDurableTrainable if args.mock_storage else DurableTrainable
    analysis = tune.run(
        get_optimus_trainable(parent),
        name="durableTrainable" + str(time.time()),
        config=config,
        num_samples=4,
        verbose=1,
        queue_trials=True,
        # fault tolerance parameters
        max_failures=-1,
        checkpoint_freq=20,
        sync_to_driver=False,
        sync_on_checkpoint=False,
        upload_dir="s3://ray-tune-test/exps/",
        checkpoint_score_attr="training_iteration",
    )
