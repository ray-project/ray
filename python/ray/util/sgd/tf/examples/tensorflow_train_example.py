import argparse
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import numpy as np

import ray
from ray import tune
from ray.util.sgd.tf.tf_trainer import TFTrainer, TFTrainable

NUM_TRAIN_SAMPLES = 1000
NUM_TEST_SAMPLES = 400


def create_config(batch_size):
    return {
        # todo: batch size needs to scale with # of workers
        "batch_size": batch_size,
        "fit_config": {
            "steps_per_epoch": NUM_TRAIN_SAMPLES // batch_size
        },
        "evaluate_config": {
            "steps": NUM_TEST_SAMPLES // batch_size,
        }
    }


def linear_dataset(a=2, size=1000):
    x = np.random.rand(size)
    y = x / 2

    x = x.reshape((-1, 1))
    y = y.reshape((-1, 1))

    return x, y


def simple_dataset(config):
    batch_size = config["batch_size"]
    x_train, y_train = linear_dataset(size=NUM_TRAIN_SAMPLES)
    x_test, y_test = linear_dataset(size=NUM_TEST_SAMPLES)

    train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
    test_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))
    train_dataset = train_dataset.shuffle(NUM_TRAIN_SAMPLES).repeat().batch(
        batch_size)
    test_dataset = test_dataset.repeat().batch(batch_size)

    return train_dataset, test_dataset


def simple_model(config):
    model = Sequential([Dense(10, input_shape=(1, )), Dense(1)])

    model.compile(
        optimizer="sgd",
        loss="mean_squared_error",
        metrics=["mean_squared_error"])

    return model


def train_example(num_replicas=1, batch_size=128, use_gpu=False):
    trainer = TFTrainer(
        model_creator=simple_model,
        data_creator=simple_dataset,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        verbose=True,
        config=create_config(batch_size))

    # model baseline performance
    start_stats = trainer.validate()
    print(start_stats)

    # train for 2 epochs
    trainer.train()
    trainer.train()

    # model performance after training (should improve)
    end_stats = trainer.validate()
    print(end_stats)

    # sanity check that training worked
    dloss = end_stats["validation_loss"] - start_stats["validation_loss"]
    dmse = (end_stats["validation_mean_squared_error"] -
            start_stats["validation_mean_squared_error"])
    print(f"dLoss: {dloss}, dMSE: {dmse}")

    if dloss > 0 or dmse > 0:
        print("training sanity check failed. loss increased!")
    else:
        print("success!")


def tune_example(num_replicas=1, use_gpu=False):
    config = {
        "model_creator": simple_model,
        "data_creator": simple_dataset,
        "num_replicas": num_replicas,
        "use_gpu": use_gpu,
        "trainer_config": create_config(batch_size=128)
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
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for Ray")
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

    if args.smoke_test:
        ray.init(num_cpus=2)
    else:
        ray.init(address=args.address)

    if args.tune:
        tune_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
    else:
        train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
