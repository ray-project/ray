# This example showcases how to use Tensorflow with Ray Train.
# Original code:
# https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras
import argparse
import json
import os

import numpy as np
import pandas as pd
import ray.train as train
import tensorflow as tf
import tensorflow_datasets as tfds
from ray.data.datasource import SimpleTensorFlowDatasource
from ray.ml.result import Result
from ray.ml.train.integrations.tensorflow import TensorflowTrainer
from ray.train.tensorflow import prepare_dataset_shard
from tensorflow.keras.callbacks import Callback
from tqdm import trange

import ray


class TrainCheckpointReportCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        train.save_checkpoint(**{"model": self.model.get_weights()})
        train.report(**logs)


def train_dataset_factory():
    return tfds.load("mnist", split=["train"], as_supervised=True)[0]


def test_dataset_factory():
    return tfds.load("mnist", split=["test"], as_supervised=True)[0]


train_dataset = ray.data.read_datasource(
    SimpleTensorFlowDatasource(), dataset_factory=train_dataset_factory
)
test_dataset = ray.data.read_datasource(
    SimpleTensorFlowDatasource(), dataset_factory=test_dataset_factory
)


def normalize_images(x):
    x = tf.cast(x, tf.float32) / 255.0
    x = tf.reshape(x, (-1,))
    return x


def preprocess_dataset(batch):
    return [(normalize_images(image), normalize_images(image)) for image, _ in batch]


train_dataset = train_dataset.map_batches(preprocess_dataset)
test_dataset = test_dataset.map_batches(preprocess_dataset)


def convert_batch_to_pandas(batch):

    images = [image for image, _ in batch]

    # because we did autoencoder here
    df = pd.DataFrame({"image": images, "label": images})
    return df


train_dataset = train_dataset.map_batches(convert_batch_to_pandas)
test_dataset = test_dataset.map_batches(convert_batch_to_pandas)


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
    steps_per_epoch = config.get("steps_per_epoch", 70)

    tf_config = json.loads(os.environ["TF_CONFIG"])
    num_workers = len(tf_config["cluster"]["worker"])

    dataset_shard = train.get_dataset_shard("train")

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_autoencoder_model()
        learning_rate = config.get("lr", 0.001)
        multi_worker_model.compile(
            loss=tf.keras.losses.BinaryCrossentropy(),
            optimizer=tf.keras.optimizers.Adam(learning_rate=learning_rate),
            metrics=["binary_crossentropy",],
        )

    for epoch in range(epochs):
        tf_dataset = prepare_dataset_shard(
            dataset_shard.to_tf(
                feature_columns=["image"],
                label_column="label",
                output_signature=(
                    tf.TensorSpec(shape=(None, 784), dtype=tf.float32),
                    tf.TensorSpec(shape=(None, 784), dtype=tf.float32),
                ),
                batch_size=per_worker_batch_size,
            )
        )
        multi_worker_model.fit(tf_dataset)
        train.save_checkpoint(
            epoch=epoch, model_weights=multi_worker_model.get_weights()
        )


def train_tensorflow_mnist(
    num_workers: int = 2, use_gpu: bool = False, epochs: int = 4
) -> Result:
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    scaling_config = dict(num_workers=num_workers, use_gpu=use_gpu)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        datasets={"train": train_dataset},
        scaling_config=scaling_config,
    )

    results = trainer.fit()
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

    import ray

    if args.smoke_test:
        # 2 workers, 1 for trainer, 1 for datasets
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=4, num_gpus=num_gpus)
        train_tensorflow_mnist(num_workers=2, use_gpu=args.use_gpu)
    else:
        ray.init(address=args.address)
        train_tensorflow_mnist(
            num_workers=args.num_workers, use_gpu=args.use_gpu, epochs=args.epochs
        )
