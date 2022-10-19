import tensorflow as tf

import ray
from ray.air import Checkpoint, session
from ray.air.config import ScalingConfig
from ray.train.tensorflow import TensorflowTrainer

input_size = 1


def build_model():
    # toy neural network : 1-layer
    return tf.keras.Sequential(
        [tf.keras.layers.Dense(1, activation="linear", input_shape=(input_size,))]
    )


def train_loop_for_worker(config):
    dataset_shard = session.get_dataset_shard("train")
    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    with strategy.scope():
        model = build_model()
        model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

    for epoch in range(config["num_epochs"]):
        tf_dataset = dataset_shard.to_tf(
            feature_columns="x", label_columns="y", batch_size=1
        )
        model.fit(tf_dataset)
        # You can also use ray.air.callbacks.keras.Callback
        # for reporting and checkpointing instead of reporting manually.
        session.report(
            {},
            checkpoint=Checkpoint.from_dict(
                dict(epoch=epoch, model=model.get_weights())
            ),
        )


train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_for_worker,
    scaling_config=ScalingConfig(num_workers=3),
    datasets={"train": train_dataset},
    train_loop_config={"num_epochs": 2},
)
result = trainer.fit()
