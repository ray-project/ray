import argparse
import cloudpickle
import numpy as np
import os
import time
import logging

import ray
from ray import tune
from ray.tune import track

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class OptimusFn(object):
    def __init__(self, params, max_t=10000):
        self.params = params
        self.noise = np.random.normal(size=max_t) * 0.005

    def __call__(self, k, add_noise=True):
        time.sleep(0.5)
        b0, b1, b2 = self.params
        score = (b0 * k / 100 + 0.1 * b1 + 0.5)**(-1) + b2 * 0.01
        if add_noise:
            return score + abs(self.noise[k])
        else:
            return score


CHECKPOINT_FREQ = 10
TRAINING_ITERS = 1000


def train(config):
    if track.is_pending_restore():
        # Handle restoration.
        checkpoint = track.restore()
        func = cloudpickle.loads(checkpoint["func"])
        mock_data = checkpoint["data"]
        cur_i = checkpoint["iter"]
        np.random.set_state(checkpoint["seed"])
    else:
        # Handle initialization.
        params = [config["param1"], config["param2"], config["param3"]]
        func = OptimusFn(params=params)
        mock_data = open("/dev/urandom", "rb").read(1024)
        cur_i = 0

    for i in range(cur_i, TRAINING_ITERS):
        new_loss = func(i)

        if i % CHECKPOINT_FREQ == 0:
            checkpoint = {
                "func": cloudpickle.dumps(func),
                "seed": np.random.get_state(),
                "data": mock_data,
                "iter": i,
            }
        else:
            checkpoint = None

        track.log(
            mean_loss=float(new_loss),
            mean_accuracy=(2 - new_loss) / 2,
            **{track.SAVE: checkpoint},
        )


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", default=False)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse()
    address = None if args.local else "auto"
    ray.init(address=address)

    config = {
        "param1": tune.sample_from(lambda spec: np.random.exponential(0.1)),
        "param2": tune.sample_from(lambda _: np.random.rand()),
        "param3": tune.sample_from(lambda _: np.random.rand()),
    }

    analysis = tune.run(
        train,
        name="durableTrainable" + str(time.time()),
        config=config,
        num_samples=1,
        verbose=2,
        queue_trials=True,
        max_failures=-1,
    )
