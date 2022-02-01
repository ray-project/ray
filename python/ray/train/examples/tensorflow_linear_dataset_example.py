import argparse

import tensorflow as tf
from tensorflow.keras.callbacks import Callback

import ray
import ray.train as train
from ray.data import Dataset
from ray.data.dataset_pipeline import DatasetPipeline
from ray.train import Trainer
from ray.train.tensorflow import prepare_dataset_shard


class TrainReportCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        train.report(**logs)


def get_dataset_pipeline(a=5, b=10, size=1000) -> DatasetPipeline:
    def get_dataset(a, b, size) -> Dataset:
        items = [i / size for i in range(size)]
        dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
        return dataset

    dataset = get_dataset(a, b, size)

    dataset_pipeline = dataset.repeat().random_shuffle_each_window()

    return dataset_pipeline


def build_and_compile_model(config):
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )

    model.compile(
        optimizer=tf.keras.optimizers.SGD(learning_rate=config.get("lr", 1e-3)),
        loss=tf.keras.losses.mean_squared_error,
        metrics=[tf.keras.metrics.mean_squared_error],
    )
    return model


def train_func(config):
    batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_and_compile_model(config)

    dataset_pipeline = train.get_dataset_shard()
    dataset_iterator = dataset_pipeline.iter_epochs()

    results = []
    for _ in range(epochs):
        dataset = next(dataset_iterator)
        tf_dataset = prepare_dataset_shard(
            dataset.to_tf(
                label_column="y",
                output_signature=(
                    tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                    tf.TensorSpec(shape=(None), dtype=tf.float32),
                ),
                batch_size=batch_size,
            )
        )
        history = multi_worker_model.fit(tf_dataset, callbacks=[TrainReportCallback()])
        results.append(history.history)
    return results


def train_tensorflow_linear(num_workers=2, use_gpu=False):
    dataset_pipeline = get_dataset_pipeline()
    trainer = Trainer(backend="tensorflow", num_workers=num_workers, use_gpu=use_gpu)
    trainer.start()
    results = trainer.run(
        train_func=train_func,
        dataset=dataset_pipeline,
        config={"lr": 1e-3, "batch_size": 32, "epochs": 4},
    )
    trainer.shutdown()
    print(f"Results: {results[0]}")
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
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    if args.smoke_test:
        # 1 for datasets
        num_cpus = args.num_workers + 1
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=num_cpus, num_gpus=num_gpus)
    else:
        ray.init(address=args.address)
    train_tensorflow_linear(num_workers=args.num_workers, use_gpu=args.use_gpu)
