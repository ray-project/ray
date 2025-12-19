# This example showcases how to use Tensorflow with Ray Train.
# Original code:
# https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras
import argparse
import json
import os

import numpy as np
import tensorflow as tf
from filelock import FileLock

from ray.air.integrations.keras import ReportCheckpointCallback
from ray.train import Result, RunConfig, ScalingConfig
from ray.train.tensorflow import TensorflowTrainer


def mnist_dataset(batch_size: int) -> tf.data.Dataset:
    # Load once per process (FileLock prevents races when running locally)
    with FileLock(os.path.expanduser("~/.mnist_lock")):
        (x_train, y_train), _ = tf.keras.datasets.mnist.load_data()

    # Normalize and ensure float32 numpy arrays, then add channel dim
    x_train = x_train.astype(np.float32) / 255.0
    x_train = x_train[..., np.newaxis]  # shape -> (N, 28, 28, 1)
    y_train = y_train.astype(np.int64)

    # Create tf.data.Dataset and convert elements to Tensors, with stable shapes.
    ds = tf.data.Dataset.from_tensor_slices((x_train, y_train))
    ds = ds.shuffle(60000)
    # Important: drop_remainder=True to keep fixed batch shapes across replicas
    ds = ds.repeat().batch(batch_size, drop_remainder=True)

    # Ensure TF knows how to shard the dataset in a multi-worker setup.
    options = tf.data.Options()
    try:
        # Explicitly set the auto-shard policy to the default AUTO behavior.
        options.experimental_distribute.auto_shard_policy = (
            tf.data.experimental.AutoShardPolicy.AUTO
        )
    except Exception:
        # Older TF versions may expose API differently; ignore if not present.
        pass
    ds = ds.with_options(options)

    ds = ds.prefetch(tf.data.AUTOTUNE)

    return ds


def build_cnn_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.Input(shape=(28, 28, 1)),
            tf.keras.layers.Conv2D(32, 3, activation="relu"),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(128, activation="relu"),
            tf.keras.layers.Dense(10),
        ]
    )
    return model


def train_func(config: dict):
    per_worker_batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)
    steps_per_epoch = config.get("steps_per_epoch", 70)

    tf_config = json.loads(os.environ["TF_CONFIG"])
    num_workers = len(tf_config["cluster"]["worker"])

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    # Use global batch size (per worker batch * num workers)
    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = mnist_dataset(global_batch_size)

    # debug: show element spec so we can detect PerReplica issues early
    # (This will print in each worker's logs)
    tf.print("train_ds.element_spec:", multi_worker_dataset.element_spec)

    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_cnn_model()
        learning_rate = config.get("lr", 0.001)
        multi_worker_model.compile(
            loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
            optimizer=tf.keras.optimizers.SGD(learning_rate=learning_rate),
            metrics=["accuracy"],
        )

    # Pass the plain tf.data.Dataset to fit. Do NOT call
    # strategy.experimental_distribute_dataset(...) when using model.fit.
    history = multi_worker_model.fit(
        multi_worker_dataset,
        epochs=epochs,
        steps_per_epoch=steps_per_epoch,
        callbacks=[ReportCheckpointCallback()],
    )
    results = history.history
    return results


def train_tensorflow_mnist(
    num_workers: int = 2,
    use_gpu: bool = False,
    epochs: int = 4,
    storage_path: str = None,
) -> Result:
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        run_config=RunConfig(storage_path=storage_path),
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
