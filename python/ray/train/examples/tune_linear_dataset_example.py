import argparse

import ray
from ray import tune
from ray.train import Trainer

from train_linear_dataset_example import train_func, get_datasets


def tune_linear(num_workers, num_samples, use_gpu):
    datasets = get_datasets()

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=use_gpu)
    Trainable = trainer.to_tune_trainable(train_func, dataset=datasets)
    analysis = tune.run(
        Trainable,
        num_samples=num_samples,
        config={
            "lr": tune.loguniform(1e-4, 1e-1),
            "batch_size": tune.choice([4, 16, 32]),
            "epochs": 3,
        },
    )
    results = analysis.get_best_config(metric="loss", mode="min")
    print(results)
    return results


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
        # 1 for driver, 1 for datasets
        num_cpus = args.num_workers + 2
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=args.num_workers + 2, num_gpus=num_gpus)
    else:
        ray.init(address=args.address)
    tune_linear(
        num_workers=args.num_workers, use_gpu=args.use_gpu, num_samples=args.num_samples
    )
