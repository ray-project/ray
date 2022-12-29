import ray
from ray import tune
from ray.air import ScalingConfig, session
from ray.data.preprocessors import Concatenator, Chain, StandardScaler
from ray.train.horovod import HorovodTrainer
from ray.tune import Tuner, TuneConfig
import numpy as np


# TF/Keras-specific
import horovod.keras as hvd

from ray.air.integrations.keras import Callback as KerasCallback
import tensorflow as tf


def create_keras_model(input_features):
    return tf.keras.Sequential(
        [
            tf.keras.Input(shape=(input_features,)),
            tf.keras.layers.Dense(16, activation="relu"),
            tf.keras.layers.Dense(16, activation="relu"),
            tf.keras.layers.Dense(1),
        ]
    )


def keras_train_loop(config):
    lr = config["lr"]
    epochs = config["epochs"]
    batch_size = config["batch_size"]
    num_features = config["num_features"]

    hvd.init()

    dataset = session.get_dataset_shard("train")

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        optimizer = tf.keras.optimizers.SGD(learning_rate=lr * hvd.size())
        optimizer = hvd.DistributedOptimizer(optimizer)

        multi_worker_model = create_keras_model(num_features)
        multi_worker_model.compile(
            optimizer=optimizer,
            loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
            metrics=[
                tf.keras.metrics.BinaryCrossentropy(
                    name="loss",
                )
            ],
        )

    for _ in range(epochs):
        tf_dataset = dataset.to_tf(
            feature_columns="concat_out", label_columns="target", batch_size=batch_size
        )
        multi_worker_model.fit(
            tf_dataset,
            callbacks=[
                hvd.callbacks.BroadcastGlobalVariablesCallback(0),
                KerasCallback(),
            ],
            verbose=0,
        )


def tune_horovod_keras(num_workers, num_samples, use_gpu):
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
    num_features = len(dataset.schema().names) - 1

    preprocessor = Chain(
        StandardScaler(columns=["mean radius", "mean texture"]),
        Concatenator(exclude=["target"], dtype=np.float32),
    )

    horovod_trainer = HorovodTrainer(
        train_loop_per_worker=keras_train_loop,
        train_loop_config={"epochs": 10, "num_features": num_features},
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": dataset},
        preprocessor=preprocessor,
    )

    tuner = Tuner(
        horovod_trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.uniform(0.1, 1),
                "batch_size": tune.choice([32, 64]),
            }
        },
        tune_config=TuneConfig(mode="min", metric="loss", num_samples=num_samples),
        _tuner_kwargs={"fail_fast": True},
    )

    result_grid = tuner.fit()

    print("Best hyperparameters found were: ", result_grid.get_best_result().config)


if __name__ == "__main__":
    ray.init(num_cpus=16)
    tune_horovod_keras(num_workers=2, num_samples=4, use_gpu=False)
