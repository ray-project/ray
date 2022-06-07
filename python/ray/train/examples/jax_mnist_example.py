import argparse
import functools
import time
from typing import Dict

import einops
import numpy as np
import optax
import ray.train as train
import tensorflow_datasets as tfds
from ray.train.callbacks import JsonLoggerCallback
from ray.train.trainer import Trainer


def get_datasets():
    import jax

    """Load MNIST train and test datasets into memory."""
    # shard the dataset
    def shard_fn(x):
        return einops.rearrange(x, "(d l) ... -> d l ...", d=jax.process_count())[
            jax.process_index()
        ]

    # Hide any GPUs from TensorFlow. Otherwise TF might reserve memory and make
    # it unavailable to JAX.
    import tensorflow as tf

    tf.config.experimental.set_visible_devices([], "GPU")

    ds_builder = tfds.builder("mnist")
    ds_builder.download_and_prepare()
    test_ds = tfds.as_numpy(ds_builder.as_dataset(split="test", batch_size=-1))
    train_ds = tfds.as_numpy(ds_builder.as_dataset(split="train", batch_size=-1))

    train_ds["image"] = train_ds["image"][
        : len(train_ds["image"]) // jax.device_count() * jax.device_count()
    ]
    train_ds["label"] = train_ds["label"][
        : len(train_ds["label"]) // jax.device_count() * jax.device_count()
    ]
    test_ds["image"] = test_ds["image"][
        : len(test_ds["image"]) // jax.device_count() * jax.device_count()
    ]
    test_ds["label"] = test_ds["label"][
        : len(test_ds["label"]) // jax.device_count() * jax.device_count()
    ]

    train_ds["image"] = np.float32(shard_fn(train_ds["image"])) / 255.0
    test_ds["image"] = np.float32(shard_fn(test_ds["image"])) / 255.0
    train_ds["label"] = np.int32(shard_fn(train_ds["label"]))
    test_ds["label"] = np.int32(shard_fn(test_ds["label"]))
    return train_ds, test_ds


def train_func(config: Dict):
    import jax
    import jax.numpy as jnp
    from flax import jax_utils
    from flax import linen as nn
    from flax.training import train_state
    from flax.training.common_utils import shard
    from jax import lax

    # NOTE: the flax nn module has to define inside
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

    @functools.partial(jax.pmap, static_broadcasted_argnums=(1, 2))
    def create_train_state(rng, learning_rate, momentum):
        """Creates initial `TrainState`."""
        mlp = MLP()
        params = mlp.init(rng, jnp.ones([1, 28, 28, 1]))["params"]
        tx = optax.sgd(learning_rate, momentum)
        return train_state.TrainState.create(apply_fn=MLP().apply, params=params, tx=tx)

    @functools.partial(jax.pmap, axis_name="ensemble")
    def apply_model(state, images, labels):
        """Computes gradients, loss and accuracy for a single batch."""

        def loss_fn(params):
            logits = MLP().apply({"params": params}, images)
            one_hot = jax.nn.one_hot(labels, 10)
            loss = jnp.mean(optax.softmax_cross_entropy(logits=logits, labels=one_hot))
            return loss, logits

        grad_fn = jax.value_and_grad(loss_fn, has_aux=True)
        (loss, logits), grads = grad_fn(state.params)
        accuracy = jnp.mean(jnp.argmax(logits, -1) == labels)
        return grads, loss, accuracy

    @jax.pmap
    def update_model(state, grads):
        return state.apply_gradients(grads=grads)

    @functools.partial(jax.pmap, axis_name="ensemble")
    def train_step(state, images, labels):
        """Computes gradients, loss and accuracy for a single batch."""

        def loss_fn(params):
            logits = MLP().apply({"params": params}, images)
            one_hot = jax.nn.one_hot(labels, 10)
            loss = jnp.mean(optax.softmax_cross_entropy(logits=logits, labels=one_hot))
            return loss, logits

        grad_fn = jax.value_and_grad(loss_fn, has_aux=True)
        (loss, logits), grads = grad_fn(state.params)
        grads = lax.pmean(grads, axis_name="ensemble")
        accuracy = jnp.mean(jnp.argmax(logits, -1) == labels)
        state = state.apply_gradients(grads=grads)
        loss = lax.pmean(loss, axis_name="ensemble")
        accuracy = lax.pmean(accuracy, axis_name="ensemble")
        return state, loss, accuracy

    def train_epoch(state, train_ds, batch_size, rng):
        """Train for a single epoch."""
        train_ds_size = len(train_ds["image"])
        steps_per_epoch = train_ds_size // batch_size

        epoch_loss = []
        epoch_accuracy = []

        for i in range(steps_per_epoch):
            batch_images = train_ds["image"][i * batch_size : (i + 1) * batch_size]
            batch_labels = train_ds["label"][i * batch_size : (i + 1) * batch_size]
            batch_images = shard(batch_images)
            batch_labels = shard(batch_labels)
            state, loss, accuracy = train_step(state, batch_images, batch_labels)
            epoch_loss.append(jax_utils.unreplicate(loss))
            epoch_accuracy.append(jax_utils.unreplicate(accuracy))
        train_loss = np.mean(epoch_loss)
        train_accuracy = np.mean(epoch_accuracy)
        return state, train_loss, train_accuracy

    """Execute model training"""
    # get the configuration
    learning_rate = config["learning_rate"]
    momentum = config["momentum"]
    batch_size = config["batch_size"]
    num_epochs = config["num_epochs"]
    worker_batch_size = batch_size // train.world_size()

    # Create datasets
    train_ds, test_ds = get_datasets()

    rng = jax.random.PRNGKey(0)
    rng, init_rng = jax.random.split(rng)
    init_rng = jax_utils.replicate(rng)

    # Create model & optimizer.
    state = create_train_state(init_rng, learning_rate, momentum)

    acc_results = []
    for epoch in range(1, num_epochs + 1):
        rng, input_rng = jax.random.split(rng)
        tic = time.time()
        state, train_loss, train_accuracy = train_epoch(
            state, train_ds, worker_batch_size, input_rng
        )
        epoch_time = time.time() - tic
        print(
            "epoch:% 3d, train_loss: %.4f, train_accuracy: %.2f, epoch_time: %.3f"
            % (epoch, train_loss, train_accuracy * 100, epoch_time)
        )

        train.report(train_loss=train_loss, train_accuracy=train_accuracy)
        acc_results.append(train_accuracy)

    return acc_results


def train_mnist(num_workers=4, use_gpu=True, num_gpu_per_worker=1):
    trainer = Trainer(
        backend="jax",
        num_workers=num_workers,
        use_gpu=use_gpu,
        resources_per_worker={"GPU": num_gpu_per_worker},
    )

    trainer.start()
    result = trainer.run(
        train_func=train_func,
        config={
            "learning_rate": 0.1,
            "momentum": 0.9,
            "batch_size": 8192,
            "num_epochs": 10,
        },
        callbacks=[JsonLoggerCallback()],
    )
    trainer.shutdown()
    print()
    print(f"Loss results: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=4,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=True, help="Enables GPU training"
    )
    parser.add_argument(
        "--num-gpu-per-worker",
        "-ngpu",
        type=int,
        default=1,
        help="Sets the number of gpus on each node for training.",
    )

    args, _ = parser.parse_known_args()

    import ray

    ray.init(address=args.address)
    train_mnist(
        num_workers=args.num_workers,
        use_gpu=args.use_gpu,
        num_gpu_per_worker=args.num_gpu_per_worker,
    )
