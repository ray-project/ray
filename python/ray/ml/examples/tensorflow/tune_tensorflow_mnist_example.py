import argparse

import ray
from ray import tune
from ray.ml.train.integrations.tensorflow import TensorflowTrainer

from ray.ml.examples.tensorflow.tensorflow_mnist_example import train_func


def tune_tensorflow_mnist(
    num_workers: int = 2, num_samples: int = 2, use_gpu: bool = False
):
    scaling_config = dict(num_workers=num_workers, use_gpu=use_gpu)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
    )
    Trainable = trainer.as_trainable()
    analysis = tune.run(
        Trainable,
        num_samples=num_samples,
        config={
            "train_loop_config": {
                "lr": tune.loguniform(1e-4, 1e-1),
                "batch_size": tune.choice([32, 64, 128]),
                "epochs": 3,
            }
        },
    )
    best_loss = analysis.get_best_config(metric="loss", mode="min")
    best_accuracy = analysis.get_best_config(metric="accuracy", mode="max")
    print(f"Best loss config: {best_loss}")
    print(f"Best accuracy config: {best_accuracy}")
    return analysis


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
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )

    args = parser.parse_args()

    if args.smoke_test:
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=8, num_gpus=num_gpus)
        tune_tensorflow_mnist(num_workers=2, num_samples=2, use_gpu=args.use_gpu)
    else:
        ray.init(address=args.address)
        tune_tensorflow_mnist(
            num_workers=args.num_workers,
            num_samples=args.num_samples,
            use_gpu=args.use_gpu,
        )
