from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import register_trainable
from ray.tune import run_experiments


def easy_objective(args, reporter):
    import time
    # val = args["height"]
    time.sleep(0.2)
    loss = (args["height"] - 14) ** 2 + abs(args["width"] - 3)
    reporter(
        mean_loss=loss,
        timesteps_total=1)
    time.sleep(0.1)


ray.init(redirect_output=True)
register_trainable("exp", easy_objective)


if __name__ == '__main__':
    from hyperopt import hp
    from ray.tune.hpopt_experiment import HyperOptExperiment

    space = {
        'width': hp.uniform('width', 0, 20),
        'height': hp.uniform('height', -100, 100),
    }

    config = {"repeat": 1000,
              "stop": {"training_iteration": 1},
              "config": {"space": space}}
    exp = HyperOptExperiment("my_exp", "exp", **config)

    run_experiments(exp, verbose=False)
