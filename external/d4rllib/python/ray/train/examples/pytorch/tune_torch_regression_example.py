import argparse

import ray
from ray import tune
from ray.train import DataConfig, ScalingConfig
from ray.train.examples.pytorch.torch_regression_example import get_datasets, train_func
from ray.train.torch import TorchTrainer
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


def tune_linear(num_workers, num_samples, use_gpu):
    train_dataset, val_dataset = get_datasets()

    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": train_dataset, "validation": val_dataset},
        dataset_config=DataConfig(datasets_to_split=["train"]),
    )

    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.loguniform(1e-4, 1e-1),
                "batch_size": tune.choice([4, 16, 32]),
                "epochs": 3,
            }
        },
        tune_config=TuneConfig(num_samples=num_samples, metric="loss", mode="min"),
    )
    result_grid = tuner.fit()
    best_result = result_grid.get_best_result()
    print(best_result)
    return best_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--num-samples",
        type=int,
        default=2,
        help="Sets number of samples for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Use GPU for training."
    )

    args = parser.parse_args()

    if args.smoke_test:
        # 2 workers, 1 for trainer, 1 for datasets
        ray.init(num_cpus=4)
        tune_linear(num_workers=2, num_samples=1, use_gpu=False)
    else:
        ray.init(address=args.address)
        tune_linear(
            num_workers=args.num_workers,
            use_gpu=args.use_gpu,
            num_samples=args.num_samples,
        )
