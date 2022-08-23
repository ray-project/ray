import argparse
import time
from typing import Dict

import numpy as np

import alpa

import ray.train as train


import ray
from ray.data.datasource import SimpleTensorFlowDatasource
import pandas as pd
from ray.data.extensions import TensorArray


def get_dataset(dataset_name="mnist"):
    """Load MNIST train and test datasets using Ray dataset."""

    # Hide any GPUs from TensorFlow. Otherwise TF might reserve memory and make
    # it unavailable to JAX.
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf.config.experimental.set_visible_devices([], "GPU")

    def train_dataset_factory():
        return tfds.load(dataset_name, split=["train"], as_supervised=True)[0]

    def test_dataset_factory():
        return tfds.load(dataset_name, split=["test"], as_supervised=True)[0]

    train_dataset = ray.data.read_datasource(
        SimpleTensorFlowDatasource(), dataset_factory=train_dataset_factory
    )
    test_dataset = ray.data.read_datasource(
        SimpleTensorFlowDatasource(), dataset_factory=test_dataset_factory
    )

    def normalize_images(batch):
        return [(tf.cast(image, tf.float32) / 255.0, label) for image, label in batch]

    train_dataset = train_dataset.map_batches(normalize_images)
    test_dataset = test_dataset.map_batches(normalize_images)

    def convert_batch_to_pandas(batch):
        images = [TensorArray(image.numpy()) for image, _ in batch]
        labels = [label.numpy() for _, label in batch]

        df = pd.DataFrame({"image": images, "label": labels})
        return df

    train_dataset = train_dataset.map_batches(convert_batch_to_pandas)
    test_dataset = test_dataset.map_batches(convert_batch_to_pandas)

    return train_dataset, test_dataset


def train_func(datasets: ray.data.Dataset, config: Dict):
    import jax
    import jax.numpy as jnp
    from flax import linen as nn
    from flax.training import train_state
    import optax

    # NOTE: the flax nn module has to be defined inside
    # otherwise, the error message `ValueError: parent must be None, Module or Scope`
    # see: https://github.com/google/flax/discussions/1390
    class MLP(nn.Module):
        """A simple mlp model."""

        @nn.compact
        def __call__(self, x):
            x = x.reshape((x.shape[0], -1))  # flatten
            x = nn.Dense(features=512)(x)
            x = nn.relu(x)
            x = nn.Dense(features=256)(x)
            x = nn.relu(x)
            x = nn.Dense(features=10)(x)
            return x

    def create_train_state(rng, learning_rate, momentum):
        """Creates initial `TrainState`."""
        mlp = MLP()
        params = mlp.init(rng, jnp.ones([1, 28, 28, 1]))["params"]
        tx = optax.sgd(learning_rate, momentum)
        return train_state.TrainState.create(apply_fn=MLP().apply, params=params, tx=tx)

    @alpa.parallelize
    def train_step(state, images, labels):
        """Computes gradients, loss and accuracy for a single batch."""

        def loss_fn(params):
            logits = MLP().apply({"params": params}, images)
            one_hot = jax.nn.one_hot(labels, 10)
            loss = jnp.mean(optax.softmax_cross_entropy(logits=logits, labels=one_hot))
            return loss, logits

        grad_fn = jax.value_and_grad(loss_fn, has_aux=True)
        (loss, logits), grads = grad_fn(state.params)
        accuracy = jnp.mean(jnp.argmax(logits, -1) == labels)
        state = state.apply_gradients(grads=grads)
        return state, loss, accuracy

    def train_epoch(state, train_ds):
        """Train for a single epoch."""
        epoch_loss = []
        epoch_accuracy = []

        for batch_images, batch_labels in train_ds:
            state, loss, accuracy = train_step(state, batch_images, batch_labels)
            epoch_loss.append(loss)
            epoch_accuracy.append(accuracy)
        train_loss = np.mean(epoch_loss)
        train_accuracy = np.mean(epoch_accuracy)
        return state, train_loss, train_accuracy

    """Execute model training"""
    # get the configuration
    learning_rate = config["learning_rate"]
    momentum = config["momentum"]
    batch_size = config["batch_size"]
    num_epochs = config["num_epochs"]

    # Create datasets
    train_ds = datasets.pop("train")

    rng = jax.random.PRNGKey(0)
    rng, init_rng = jax.random.split(rng)

    # Create model & optimizer.
    state = create_train_state(init_rng, learning_rate, momentum)

    # this can be numpy dataset
    jax_dataset = train_ds.to_jax(
        feature_columns=["image"],
        label_column="label",
        batch_size=batch_size,
        unsqueeze_feature_tensors=False,
        unsqueeze_label_tensor=False,
    )
    jax_dataset = list(jax_dataset)

    acc_results = []
    for epoch in range(1, num_epochs + 1):
        rng, input_rng = jax.random.split(rng)
        tic = time.time()
        state, train_loss, train_accuracy = train_epoch(state, jax_dataset)
        epoch_time = time.time() - tic
        print(
            "epoch:% 3d, train_loss: %.4f, train_accuracy: %.2f, epoch_time: %.3f"
            % (epoch, train_loss, train_accuracy * 100, epoch_time)
        )

        train.report(train_loss=train_loss, train_accuracy=train_accuracy)
        acc_results.append(train_accuracy)

    return acc_results


def train_mnist():
    train_dataset, test_dataset = get_dataset("mnist")

    config = {
        "learning_rate": 0.1,
        "momentum": 0.9,
        "batch_size": 5000,
        "num_epochs": 100,
    }

    from ray.train.alpa import AlpaTrainer

    trainer = AlpaTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        datasets={"train": train_dataset},
    )

    results = trainer.fit()
    print()
    print(f"Loss results: {results}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )

    args, _ = parser.parse_known_args()

    import ray

    ray.init(address=args.address)
    train_mnist()
