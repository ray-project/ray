import argparse
from typing import Dict, Tuple

import tensorflow as tf
from ray.air.callbacks.keras import Callback as TrainReportCallback

import ray
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig
from ray.data import Dataset
from ray.train.tensorflow import TensorflowTrainer, prepare_dataset_shard


def get_datasets_and_configs(
    a=5, b=10, size=1000
) -> Tuple[Dict[str, Dataset], Dict[str, DatasetConfig]]:
    def get_dataset(a, b, size) -> Dataset:
        items = [i / size for i in range(size)]
        dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
        return dataset

    datasets = {"train": get_dataset(a, b, size)}

    # Use dataset pipelining
    dataset_configs = {
        "train": DatasetConfig(use_stream_api=True),
    }

    return datasets, dataset_configs


def build_and_compile_model(config):
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )

    model.compile(
        optimizer=tf.keras.optimizers.SGD(learning_rate=config.get("lr", 1e-3)),
        loss=tf.keras.losses.mean_squared_error,
        metrics=[tf.keras.metrics.mean_squared_error],
    )
    return model


def train_func(config):
    batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_and_compile_model(config)

    dataset_pipeline = session.get_dataset_shard("train")
    dataset_iterator = dataset_pipeline.iter_epochs()

    for _ in range(epochs):
        dataset = next(dataset_iterator)
        tf_dataset = prepare_dataset_shard(
            dataset.to_tf(
                label_column="y",
                output_signature=(
                    tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                    tf.TensorSpec(shape=(None), dtype=tf.float32),
                ),
                batch_size=batch_size,
            )
        )
        multi_worker_model.fit(tf_dataset, callbacks=[TrainReportCallback()])


def train_tensorflow_linear(num_workers=2, use_gpu=False):
    datasets, dataset_configs = get_datasets_and_configs()
    trainer = TensorflowTrainer(
        train_func,
        train_loop_config={"lr": 1e-3, "batch_size": 32, "epochs": 4},
        datasets=datasets,
        dataset_config=dataset_configs,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    results = trainer.fit()
    print(f"Results: {results.metrics}")
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
        # 1 for datasets, 1 for Trainable actor
        num_cpus = args.num_workers + 2
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=num_cpus, num_gpus=num_gpus)
    else:
        ray.init(address=args.address)
    train_tensorflow_linear(num_workers=args.num_workers, use_gpu=args.use_gpu)
