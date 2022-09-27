import argparse

from ray.air.config import RunConfig, ScalingConfig
from ray.train.examples.torch_fashion_mnist_example import train_func
from ray.train.torch import TorchTrainer
from ray.air.callbacks.mlflow import MLflowLoggerCallback


def main(num_workers=2, use_gpu=False):
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"lr": 1e-3, "batch_size": 64, "epochs": 4},
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        run_config=RunConfig(
            callbacks=[MLflowLoggerCallback(experiment_name="train_fashion_mnist")]
        ),
    )
    final_results = trainer.fit()

    print("Final metrics: ", final_results.metrics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )

    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=4)
        args.num_workers = 2
        args.use_gpu = False
    else:
        ray.init(address=args.address)
    main(num_workers=args.num_workers, use_gpu=args.use_gpu)
