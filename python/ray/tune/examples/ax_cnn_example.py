import torch
import numpy as np

import ray
from ray.tune import run
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.ax import AxSearch

from ax.utils.tutorials.cnn_utils import load_mnist, train, evaluate

dtype = torch.float
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

train_loader, valid_loader, test_loader = load_mnist()

def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        net = train(train_loader=train_loader, parameters=parameterization, dtype=dtype, device=device)
        reporter(
            timesteps_total=i,
            accuracy=evaluate(net=net,data_loader=valid_loader,dtype=dtype,device=device))
        time.sleep(0.02)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init()

    config = {
        "num_samples": 10 if args.smoke_test else 30,
        "config": {
            "iterations": 100,
        },
        "stop": {
            "timesteps_total": 100
        }
    }
    parameters=[
        {"name": "lr", "type": "range", "bounds": [1e-6, 0.4], "log_scale": True},
        {"name": "momentum", "type": "range", "bounds": [0.0, 1.0]},
    ]
    algo = AxSearch(
        parameters=parameters,
        objective_name="accuracy",
        max_concurrent=4,
    )
    scheduler = AsyncHyperBandScheduler(reward_attr="accuracy")
    run(easy_objective, name="ax", search_alg=algo, **config)
