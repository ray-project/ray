import argparse
import sys

import ray
from ray import train
from ray.data.preprocessors import Concatenator
from ray.train import Result, ScalingConfig

if sys.version_info >= (3, 12):
    # Skip this test in Python 3.12+ because TensorFlow is not supported.
    sys.exit(0)
else:
    import tensorflow as tf

    from ray.air.integrations.keras import ReportCheckpointCallback
    from ray.train.tensorflow import TensorflowTrainer


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(100,)),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


def train_func(config: dict):
    batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_model()
        multi_worker_model.compile(
            optimizer=tf.keras.optimizers.SGD(learning_rate=config.get("lr", 1e-3)),
            loss=tf.keras.losses.mean_absolute_error,
            metrics=[tf.keras.metrics.mean_squared_error],
        )

    dataset = train.get_dataset_shard("train")

    results = []
    for _ in range(epochs):
        tf_dataset = dataset.to_tf(
            feature_columns="x", label_columns="y", batch_size=batch_size
        )
        history = multi_worker_model.fit(
            tf_dataset, callbacks=[ReportCheckpointCallback()]
        )
        results.append(history.history)
    return results


def train_tensorflow_regression(num_workers: int = 2, use_gpu: bool = False) -> Result:
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/regression.csv")
    columns_to_concatenate = [f"x{i:03}" for i in range(100)]
    preprocessor = Concatenator(columns=columns_to_concatenate, output_column_name="x")
    dataset = preprocessor.fit_transform(dataset)

    config = {"lr": 1e-3, "batch_size": 32, "epochs": 4}
    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
        datasets={"train": dataset},
    )
    results = trainer.fit()
    print(results.metrics)
    return results


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

    if args.smoke_test:
        # 2 workers, 1 for trainer, 1 for datasets
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=4, num_gpus=num_gpus)
        result = train_tensorflow_regression(num_workers=2, use_gpu=args.use_gpu)
    else:
        ray.init(address=args.address)
        result = train_tensorflow_regression(
            num_workers=args.num_workers, use_gpu=args.use_gpu
        )
    print(result)
