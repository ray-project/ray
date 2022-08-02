import argparse
import time
from typing import Dict

import numpy as np

import alpa

from ray.air import session
import os


def get_datasets():
    """Load MNIST train and test datasets into memory."""

    # Hide any GPUs from TensorFlow. Otherwise TF might reserve memory and make
    # it unavailable to JAX.
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf.config.experimental.set_visible_devices([], "GPU")

    ds_builder = tfds.builder("mnist")
    ds_builder.download_and_prepare()
    train_ds = tfds.as_numpy(ds_builder.as_dataset(split="train", batch_size=-1))
    test_ds = tfds.as_numpy(ds_builder.as_dataset(split="test", batch_size=-1))
    train_ds["image"] = np.float32(train_ds["image"]) / 255.0
    test_ds["image"] = np.float32(test_ds["image"]) / 255.0
    train_ds["label"] = np.int32(train_ds["label"])
    test_ds["label"] = np.int32(test_ds["label"])
    return train_ds, test_ds


def train_func(config: Dict):
    import jax
    import jax.numpy as jnp
    from flax import linen as nn
    from flax.training import train_state
    import optax

    # TODO: is this able to adding somewhere else?
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"

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

    def train_epoch(state, train_ds, batch_size):
        """Train for a single epoch."""
        train_ds_size = len(train_ds["image"])
        steps_per_epoch = train_ds_size // batch_size

        epoch_loss = []
        epoch_accuracy = []

        for i in range(steps_per_epoch):
            batch_images = train_ds["image"][i * batch_size : (i + 1) * batch_size]
            batch_labels = train_ds["label"][i * batch_size : (i + 1) * batch_size]
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
    train_ds, test_ds = get_datasets()

    rng = jax.random.PRNGKey(0)
    rng, init_rng = jax.random.split(rng)

    # Create model & optimizer.
    state = create_train_state(init_rng, learning_rate, momentum)

    for epoch in range(1, num_epochs + 1):
        tic = time.time()
        state, train_loss, train_accuracy = train_epoch(state, train_ds, batch_size)
        epoch_time = time.time() - tic
        print(
            "epoch:% 3d, train_loss: %.4f, train_accuracy: %.2f, epoch_time: %.3f"
            % (epoch, train_loss, train_accuracy * 100, epoch_time)
        )

        session.report({"train_loss": train_loss, "train_accuracy": train_accuracy})


def train_mnist(num_workers, use_gpu, num_gpu_per_worker):
    config = {
        "learning_rate": 0.1,
        "momentum": 0.9,
        "batch_size": 5000,
        "num_epochs": 100,
    }
    from ray.train.alpa import AlpaTrainer
    from ray.air.config import ScalingConfig

    trainer = AlpaTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker={"CPU": 1, "GPU": num_gpu_per_worker},
        ),
    )

    results = trainer.fit()
    print()
    print(f"Results: {results.metrics}")


def tune_mnist(num_samples):
    from ray.tune.tune_config import TuneConfig
    from ray import tune
    from ray.tune.tuner import Tuner
    from ray.train.alpa import AlpaTrainer
    from ray.air.config import ScalingConfig

    trainer = AlpaTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            num_workers=2, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 2}
        ),
    )

    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "learning_rate": tune.loguniform(1e-4, 1e-1),
                "batch_size": 5000,
                "momentum": 0.9,
                "num_epochs": tune.choice([100, 200, 300]),
            },
        },
        tune_config=TuneConfig(num_samples=num_samples),
    )
    analysis = tuner.fit()
    result = analysis.get_best_result(metric="train_loss", mode="min")
    print(result)
    return result


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
    ray.shutdown()

    # tune_mnist(num_samples=8)
