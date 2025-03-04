#!/usr/bin/env python
# coding: utf-8
#
# This example showcases how to use TF2.0 APIs with Tune.
# Original code: https://www.tensorflow.org/tutorials/quickstart/advanced
#
# As of 10/12/2019: One caveat of using TF2.0 is that TF AutoGraph
# functionality does not interact nicely with Ray actors. One way to get around
# this is to `import tensorflow` inside the Tune Trainable.
#

import argparse
import os
import sys

from filelock import FileLock

from ray import tune

MAX_TRAIN_BATCH = 10

if sys.version_info >= (3, 12):
    # Tensorflow is not installed for Python 3.12 because of keras compatibility.
    sys.exit(0)
else:
    from tensorflow.keras import Model
    from tensorflow.keras.datasets.mnist import load_data
    from tensorflow.keras.layers import Conv2D, Dense, Flatten


class MyModel(Model):
    def __init__(self, hiddens=128):
        super(MyModel, self).__init__()
        self.conv1 = Conv2D(32, 3, activation="relu")
        self.flatten = Flatten()
        self.d1 = Dense(hiddens, activation="relu")
        self.d2 = Dense(10, activation="softmax")

    def call(self, x):
        x = self.conv1(x)
        x = self.flatten(x)
        x = self.d1(x)
        return self.d2(x)


class MNISTTrainable(tune.Trainable):
    def setup(self, config):
        # IMPORTANT: See the above note.
        import tensorflow as tf

        # Use FileLock to avoid race conditions.
        with FileLock(os.path.expanduser("~/.tune.lock")):
            (x_train, y_train), (x_test, y_test) = load_data()
        x_train, x_test = x_train / 255.0, x_test / 255.0

        # Add a channels dimension
        x_train = x_train[..., tf.newaxis]
        x_test = x_test[..., tf.newaxis]
        self.train_ds = tf.data.Dataset.from_tensor_slices((x_train, y_train))
        self.train_ds = self.train_ds.shuffle(10000).batch(config.get("batch", 32))

        self.test_ds = tf.data.Dataset.from_tensor_slices((x_test, y_test)).batch(32)

        self.model = MyModel(hiddens=config.get("hiddens", 128))
        self.loss_object = tf.keras.losses.SparseCategoricalCrossentropy()
        self.optimizer = tf.keras.optimizers.Adam()
        self.train_loss = tf.keras.metrics.Mean(name="train_loss")
        self.train_accuracy = tf.keras.metrics.SparseCategoricalAccuracy(
            name="train_accuracy"
        )

        self.test_loss = tf.keras.metrics.Mean(name="test_loss")
        self.test_accuracy = tf.keras.metrics.SparseCategoricalAccuracy(
            name="test_accuracy"
        )

        @tf.function
        def train_step(images, labels):
            with tf.GradientTape() as tape:
                predictions = self.model(images)
                loss = self.loss_object(labels, predictions)
            gradients = tape.gradient(loss, self.model.trainable_variables)
            self.optimizer.apply_gradients(
                zip(gradients, self.model.trainable_variables)
            )

            self.train_loss(loss)
            self.train_accuracy(labels, predictions)

        @tf.function
        def test_step(images, labels):
            predictions = self.model(images)
            t_loss = self.loss_object(labels, predictions)

            self.test_loss(t_loss)
            self.test_accuracy(labels, predictions)

        self.tf_train_step = train_step
        self.tf_test_step = test_step

    def save_checkpoint(self, checkpoint_dir: str):
        return None

    def load_checkpoint(self, checkpoint):
        return None

    def step(self):
        self.train_loss.reset_states()
        self.train_accuracy.reset_states()
        self.test_loss.reset_states()
        self.test_accuracy.reset_states()

        for idx, (images, labels) in enumerate(self.train_ds):
            if idx > MAX_TRAIN_BATCH:  # This is optional and can be removed.
                break
            self.tf_train_step(images, labels)

        for test_images, test_labels in self.test_ds:
            self.tf_test_step(test_images, test_labels)

        # It is important to return tf.Tensors as numpy objects.
        return {
            "epoch": self.iteration,
            "loss": self.train_loss.result().numpy(),
            "accuracy": self.train_accuracy.result().numpy() * 100,
            "test_loss": self.test_loss.result().numpy(),
            "mean_accuracy": self.test_accuracy.result().numpy() * 100,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    tuner = tune.Tuner(
        MNISTTrainable,
        tune_config=tune.TuneConfig(
            metric="test_loss",
            mode="min",
        ),
        run_config=tune.RunConfig(
            stop={"training_iteration": 5 if args.smoke_test else 50},
            verbose=1,
        ),
        param_space={"hiddens": tune.grid_search([32, 64, 128])},
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
