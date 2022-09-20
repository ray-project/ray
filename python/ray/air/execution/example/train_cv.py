from typing import Dict, Any, Callable, Optional

import ray
from ray.air.execution.impl.split.split_controller import SplitController
from ray.air.execution.impl.train.progress_loop import train_run
from ray.train.torch import TorchConfig
from train_torch import train_func


def fold_func(config):
    result = train_run(
        train_func,
        config=config,
        backend_config=TorchConfig(),
        num_workers=config["num_workers_per_fold"],
    )
    return result


def run_cv(
    fold_fn: Callable,
    num_folds: int = 4,
    num_workers_per_fold: int = 2,
    resources_per_fold: Optional[Dict[str, Any]] = None,
):
    resources_per_fold = resources_per_fold or {"CPU": num_workers_per_fold}

    controller = SplitController()

    for i in range(num_folds):
        controller.add_split(
            fold_fn,
            resources=resources_per_fold,
            config={
                "num_workers_per_fold": num_workers_per_fold,
                "lr": 1e-2,
                "hidden_size": 1,
                "batch_size": 4,
                "epochs": 8,
            },
        )

    while not controller.is_finished():
        controller.step()

    print("Got results", [result["loss"] for result in controller.results])


if __name__ == "__main__":
    ray.init()
    run_cv(
        fold_fn=fold_func,
        resources_per_fold={"CPU": 2},
        num_folds=4,
        num_workers_per_fold=2,
    )
