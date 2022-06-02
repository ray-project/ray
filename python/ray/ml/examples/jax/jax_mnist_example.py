import ray.train as train

from typing import Dict
import argparse
import time
from flax import linen as nn
from flax.training import train_state
import jax
import jax.numpy as jnp
import numpy as np
import optax
import functools
from jax import lax
from flax import jax_utils
from flax.training.common_utils import shard
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

def train_func(config: Dict):

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
        # TODO: consider the input shape here!
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


    def train_epoch(state, train_ds):
        """Train for a single epoch."""
        epoch_loss = []
        epoch_accuracy = []
        for batch_images, batch_labels in train_ds: 
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
    learning_rate = config['learning_rate']
    momentum = config['momentum']
    batch_size = config['batch_size']
    num_epochs = config['num_epochs']
    worker_batch_size = batch_size // train.world_size()

    print(train.world_size(), jax.local_device_count(), jax.device_count(), jax.process_index(), jax.process_count())

    worker_batch_size = worker_batch_size // jax.local_device_count() * jax.local_device_count()

    # Create datasets
    train_ds = train.get_dataset_shard("train")

    rng = jax.random.PRNGKey(0)
    rng, init_rng = jax.random.split(rng)
    init_rng = jax_utils.replicate(rng)

    # Create model & optimizer.
    state = create_train_state(init_rng, learning_rate, momentum)

    jax_dataset = train_ds.to_jax(
                feature_columns=["image"],
                label_column="label",
                batch_size=batch_size,
                unsqueeze_feature_tensors=False,
                unsqueeze_label_tensor=False)
    jax_dataset = list(jax_dataset)
    acc_results = []
    for epoch in range(1, num_epochs + 1):
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

def train_mnist(num_workers=4, use_gpu=True, num_gpu_per_worker=1):
    train_dataset, test_dataset = get_dataset("mnist")
    
    config={
            "learning_rate": 0.1,
            "momentum": 0.9,
            "batch_size": 5000,
            "num_epochs": 100,
        }
    scaling_config = dict(num_workers=num_workers, use_gpu=use_gpu, resources_per_worker={'GPU': num_gpu_per_worker})
    from ray.ml.train.integrations.jax import JaxTrainer

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        datasets={"train": train_dataset},
        scaling_config=scaling_config
    )

    results = trainer.fit()
    print()
    print(f"Loss results: {results}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-nodes",
        "-n",
        type=int,
        default=1,
        help="Sets number of node for training.",
    )
    parser.add_argument(
        "--num-gpu-per-worker",
        "-ngpu",
        type=int,
        default=1,
        help="Sets the number of gpus on each node for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=True, help="Enables GPU training"
    )

    args, _ = parser.parse_known_args()

    import ray
    ray.init(address=args.address)
    train_mnist(num_workers=args.num_nodes, use_gpu=args.use_gpu, args.num_gpu_per_worker)
