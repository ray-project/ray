import argparse

import numpy as np
import pandas as pd
import tensorflow as tf

import ray
from ray.air import session
from ray.air.callbacks.keras import Callback as TrainCheckpointReportCallback
from ray.air.result import Result
from ray.data import Dataset
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    TensorflowPredictor,
    TensorflowTrainer,
    prepare_dataset_shard,
)
from ray.air.config import ScalingConfig


def get_dataset(a=5, b=10, size=1000) -> Dataset:
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/regression.csv")

    def combine_x(batch):
        return pd.DataFrame(
            {
                "x": batch[[f"x{i:03d}" for i in range(100)]].values.tolist(),
                "y": batch["y"],
            }
        )

    dataset = dataset.map_batches(combine_x)
    return dataset


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(100,)),
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            tf.keras.layers.Flatten(),
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

    dataset = session.get_dataset_shard("train")

    def to_tf_dataset(dataset, batch_size):
        def to_tensor_iterator():
            for batch in dataset.iter_tf_batches(
                batch_size=batch_size, dtypes=tf.float32
            ):
                yield batch["x"], batch["y"]

        output_signature = (
            tf.TensorSpec(shape=(None, 100), dtype=tf.float32),
            tf.TensorSpec(shape=(None), dtype=tf.float32),
        )
        tf_dataset = tf.data.Dataset.from_generator(
            to_tensor_iterator, output_signature=output_signature
        )
        return prepare_dataset_shard(tf_dataset)

    results = []
    for _ in range(epochs):
        tf_dataset = to_tf_dataset(dataset=dataset, batch_size=batch_size)
        history = multi_worker_model.fit(
            tf_dataset, callbacks=[TrainCheckpointReportCallback()]
        )
        results.append(history.history)
    return results


def train_tensorflow_regression(num_workers: int = 2, use_gpu: bool = False) -> Result:
    dataset_pipeline = get_dataset()
    config = {"lr": 1e-3, "batch_size": 32, "epochs": 4}
    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
        datasets={"train": dataset_pipeline},
    )
    results = trainer.fit()
    print(results.metrics)
    return results


def predict_regression(result: Result) -> Dataset:
    batch_predictor = BatchPredictor.from_checkpoint(
        result.checkpoint, TensorflowPredictor, model_definition=build_model
    )

    df = pd.DataFrame(
        [[np.random.uniform(0, 1, size=100)] for i in range(100)], columns=["x"]
    )
    prediction_dataset = ray.data.from_pandas(df)

    predictions = batch_predictor.predict(prediction_dataset, dtype=tf.float32)

    print("PREDICTIONS")
    predictions.show()

    return predictions


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
    predict_regression(result)
