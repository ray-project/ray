# This example showcases how to use Tensorflow with Ray Train.
# Original code:
# https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras
# https://blog.keras.io/building-autoencoders-in-keras.html
import argparse

import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_datasets as tfds

import ray
from ray import train
from ray.air.integrations.keras import ReportCheckpointCallback
from ray.data.datasource import SimpleTensorFlowDatasource
from ray.data.extensions import TensorArray
from ray.train import Result, ScalingConfig
from ray.train.tensorflow import TensorflowTrainer, prepare_dataset_shard


def get_dataset(split_type="train"):
    def dataset_factory():
        return tfds.load("mnist", split=[split_type], as_supervised=True)[0].take(128)

    dataset = ray.data.read_datasource(
        SimpleTensorFlowDatasource(), dataset_factory=dataset_factory
    )

    def normalize_images(x):
        x = np.float32(x.numpy()) / 255.0
        x = np.reshape(x, (-1,))
        return x

    def preprocess_dataset(batch):
        return [
            (normalize_images(image), normalize_images(image)) for image, _ in batch
        ]

    dataset = dataset.map_batches(preprocess_dataset)

    def convert_batch_to_pandas(batch):

        images = [TensorArray(image) for image, _ in batch]
        # because we did autoencoder here
        df = pd.DataFrame({"image": images, "label": images})
        return df

    dataset = dataset.map_batches(convert_batch_to_pandas)
    return dataset


def build_autoencoder_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.Input(shape=(784,)),
            # encoder
            tf.keras.layers.Dense(128, activation="relu"),
            tf.keras.layers.Dense(64, activation="relu"),
            tf.keras.layers.Dense(32, activation="relu"),
            # decoder
            tf.keras.layers.Dense(64, activation="relu"),
            tf.keras.layers.Dense(128, activation="relu"),
            tf.keras.layers.Dense(784, activation="sigmoid"),
        ]
    )
    return model


def train_func(config: dict):

    per_worker_batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)

    dataset_shard = train.get_dataset_shard("train")

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_autoencoder_model()
        learning_rate = config.get("lr", 0.001)
        multi_worker_model.compile(
            loss=tf.keras.losses.BinaryCrossentropy(),
            optimizer=tf.keras.optimizers.Adam(learning_rate=learning_rate),
            metrics=[
                "binary_crossentropy",
            ],
        )

    def to_tf_dataset(dataset, batch_size):
        def to_tensor_iterator():
            for batch in dataset.iter_tf_batches(
                batch_size=batch_size, dtypes=tf.float32
            ):
                yield batch["image"], batch["label"]

        output_signature = (
            tf.TensorSpec(shape=(None, 784), dtype=tf.float32),
            tf.TensorSpec(shape=(None, 784), dtype=tf.float32),
        )
        tf_dataset = tf.data.Dataset.from_generator(
            to_tensor_iterator, output_signature=output_signature
        )
        return prepare_dataset_shard(tf_dataset)

    results = []
    for epoch in range(epochs):
        tf_dataset = to_tf_dataset(
            dataset=dataset_shard,
            batch_size=per_worker_batch_size,
        )
        history = multi_worker_model.fit(
            tf_dataset, callbacks=[ReportCheckpointCallback()]
        )
        results.append(history.history)
    return results


def train_tensorflow_mnist(
    num_workers: int = 2, use_gpu: bool = False, epochs: int = 4
) -> Result:
    train_dataset = get_dataset(split_type="train")
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        datasets={"train": train_dataset},
        scaling_config=scaling_config,
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
        "--epochs", type=int, default=3, help="Number of epochs to train for."
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
        result = train_tensorflow_mnist(num_workers=2, use_gpu=args.use_gpu)
    else:
        ray.init(address=args.address)
        result = train_tensorflow_mnist(
            num_workers=args.num_workers, use_gpu=args.use_gpu, epochs=args.epochs
        )
    print(result)
