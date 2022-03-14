import argparse
import os

from filelock import FileLock
from tensorflow.keras.datasets import mnist

import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.integration.keras import TuneReportCallback


def train_mnist(config):
    # https://github.com/tensorflow/tensorflow/issues/32159
    import tensorflow as tf

    batch_size = 128
    num_classes = 10
    epochs = 12

    with FileLock(os.path.expanduser("~/.data.lock")):
        (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train, x_test = x_train / 255.0, x_test / 255.0
    model = tf.keras.models.Sequential(
        [
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(config["hidden"], activation="relu"),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(num_classes, activation="softmax"),
        ]
    )

    model.compile(
        loss="sparse_categorical_crossentropy",
        optimizer=tf.keras.optimizers.SGD(lr=config["lr"], momentum=config["momentum"]),
        metrics=["accuracy"],
    )

    model.fit(
        x_train,
        y_train,
        batch_size=batch_size,
        epochs=epochs,
        verbose=0,
        validation_data=(x_test, y_test),
        callbacks=[TuneReportCallback({"mean_accuracy": "accuracy"})],
    )


def tune_mnist(num_training_iterations):
    sched = AsyncHyperBandScheduler(
        time_attr="training_iteration", max_t=400, grace_period=20
    )

    analysis = tune.run(
        train_mnist,
        name="exp",
        scheduler=sched,
        metric="mean_accuracy",
        mode="max",
        stop={"mean_accuracy": 0.99, "training_iteration": num_training_iterations},
        num_samples=10,
        resources_per_trial={"cpu": 2, "gpu": 0},
        config={
            "threads": 2,
            "lr": tune.uniform(0.001, 0.1),
            "momentum": tune.uniform(0.1, 0.9),
            "hidden": tune.randint(32, 512),
        },
    )
    print("Best hyperparameters found were: ", analysis.best_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using " "Ray Client.",
    )
    args, _ = parser.parse_known_args()
    if args.smoke_test:
        ray.init(num_cpus=4)
    elif args.server_address:
        ray.init(f"ray://{args.server_address}")

    tune_mnist(num_training_iterations=5 if args.smoke_test else 300)
