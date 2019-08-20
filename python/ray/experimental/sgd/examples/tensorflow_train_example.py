from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import tensorflow as tf

from ray import tune
from ray.experimental.sgd.tensorflow.tensorflow_trainer import (
    TensorFlowTrainer, TensorFlowTrainable)

def get_dataset(batch_size):
    NUM_TRAIN_SAMPLES = 60000
    mnist = tf.keras.datasets.mnist

    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train, x_test = x_train / 255.0, x_test / 255.0

    train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
    test_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))

    tf.random.set_seed(22)
    train_dataset = train_dataset.shuffle(NUM_TRAIN_SAMPLES).batch(
        batch_size, drop_remainder=True)
    test_dataset = test_dataset.batch(batch_size, drop_remainder=True)

    return train_dataset, test_dataset


def get_model(config=None):

    model = tf.keras.models.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(10, activation='softmax')
    ])

    model.compile(
        optimizer='adam',
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy'])

    return model


def train_example(num_replicas=1, use_gpu=False):
    trainer = TensorFlowTrainer(
        model_creator=get_model,
        data_creator=get_dataset,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        batch_size=512)

    train_stats1 = trainer.train()
    train_stats1.update(trainer.validate())
    print(train_stats1)

    train_stats2 = trainer.train()
    train_stats2.update(trainer.validate())
    print(train_stats2)

    assert train_stats1["validation_loss"] > train_stats2["validation_loss"]

    val_stats = trainer.validate()
    print(val_stats)
    print("success!")


def tune_example(num_replicas=1, use_gpu=False):
    config = {
        "model_creator": tune.function(get_model),
        "data_creator": tune.function(get_dataset),
        "num_replicas": num_replicas,
        "use_gpu": use_gpu,
        "batch_size": 512
    }

    analysis = tune.run(
        TensorFlowTrainable,
        num_samples=12,
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
