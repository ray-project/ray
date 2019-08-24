from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import tensorflow as tf
import numpy as np

from ray import tune
from ray.experimental.sgd.tf.tf_trainer import TFTrainer, TFTrainable


def linear_dataset(a=2, b=5, size=1000):
    x = np.arange(0, 10, 10 / size, dtype=np.float32)
    y = a * x + b

    x = x.reshape((-1, 1))
    y = y.reshape((-1, 1))

    return x, y


def simple_dataset(batch_size=20):
    NUM_TRAIN_SAMPLES = 1000

    x_train, y_train = linear_dataset(size=NUM_TRAIN_SAMPLES)
    x_test, y_test = linear_dataset(size=400)

    train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
    test_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))

    tf.random.set_seed(22)
    train_dataset = train_dataset.shuffle(NUM_TRAIN_SAMPLES).batch(
        batch_size, drop_remainder=True)
    test_dataset = test_dataset.batch(batch_size, drop_remainder=True)

    return train_dataset, test_dataset


def simple_model():
    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(10, input_shape=(1, )),
        tf.keras.layers.Dense(1, activation="softmax")])

    model.compile(
        optimizer="sgd",
        loss="mean_squared_error",
        metrics=["mean_squared_error"])

    return model


def train_example(num_replicas=1, use_gpu=False):
    trainer = TFTrainer(
        model_creator=simple_model,
        data_creator=simple_dataset,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        batch_size=128)

    train_stats1 = trainer.train()
    train_stats1.update(trainer.validate())
    print(train_stats1)

    train_stats2 = trainer.train()
    train_stats2.update(trainer.validate())
    print(train_stats2)

    val_stats = trainer.validate()
    print(val_stats)
    print("success!")


def tune_example(num_replicas=1, use_gpu=False):
    config = {
        "model_creator": tune.function(simple_model),
        "data_creator": tune.function(simple_dataset),
        "num_replicas": num_replicas,
        "use_gpu": use_gpu,
        "batch_size": 128
    }

    analysis = tune.run(
        TFTrainable,
        num_samples=2,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)

    return analysis.get_best_config(metric="validation_loss", mode="min")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    import ray

    ray.init(redis_address=args.redis_address)

    if args.tune:
        tune_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
    else:
        train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
