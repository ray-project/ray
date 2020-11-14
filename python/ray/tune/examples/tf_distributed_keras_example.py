"""
Adapted from
https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras
"""
import argparse
import tensorflow as tf
import numpy as np
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.integration.keras import TuneReportCheckpointCallback
from ray.tune.integration.tensorflow import (DistributedTrainableCreator,
                                             get_num_workers)


def mnist_dataset(batch_size):
    (x_train, y_train), _ = tf.keras.datasets.mnist.load_data()
    # The `x` arrays are in uint8 and have values in the range [0, 255].
    # You need to convert them to float32 with values in the range [0, 1]
    x_train = x_train / np.float32(255)
    y_train = y_train.astype(np.int64)
    train_dataset = tf.data.Dataset.from_tensor_slices(
        (x_train, y_train)).shuffle(60000).repeat().batch(batch_size)
    return train_dataset


def build_and_compile_cnn_model(config):
    model = tf.keras.Sequential([
        tf.keras.Input(shape=(28, 28)),
        tf.keras.layers.Reshape(target_shape=(28, 28, 1)),
        tf.keras.layers.Conv2D(32, 3, activation="relu"),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(config.get("hidden", 128), activation="relu"),
        tf.keras.layers.Dense(10)
    ])
    model.compile(
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        optimizer=tf.keras.optimizers.SGD(
            learning_rate=config.get("lr", 0.05),
            momentum=config.get("momentum", 0.5)),
        metrics=["accuracy"])
    return model


def train_mnist(config, checkpoint_dir=None):
    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    per_worker_batch_size = 64
    num_workers = get_num_workers()
    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = mnist_dataset(global_batch_size)
    with strategy.scope():
        multi_worker_model = build_and_compile_cnn_model(config)

    multi_worker_model.fit(
        multi_worker_dataset,
        epochs=2,
        steps_per_epoch=70,
        callbacks=[
            TuneReportCheckpointCallback(
                {
                    "mean_accuracy": "accuracy"
                }, filename="checkpoint")
        ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="enables CUDA training")
    parser.add_argument(
        "--cluster",
        action="store_true",
        default=False,
        help="enables multi-node tuning")
    args = parser.parse_args()
    tf_trainable = DistributedTrainableCreator(
        train_mnist,
        use_gpu=args.use_gpu,
        num_workers=2,
    )
    sched = AsyncHyperBandScheduler(
        time_attr="training_iteration",
        metric="mean_accuracy",
        mode="max",
        max_t=400,
        grace_period=20)
    tune.run(
        tf_trainable,
        name="exp",
        scheduler=sched,
        stop={
            "mean_accuracy": 0.99,
            "training_iteration": 10
        },
        num_samples=1,
        config={
            "lr": tune.sample_from(lambda spec: np.random.uniform(0.001, 0.1)),
            "momentum": tune.sample_from(
                lambda spec: np.random.uniform(0.1, 0.9)),
            "hidden": tune.sample_from(
                lambda spec: np.random.randint(32, 512)),
        })
