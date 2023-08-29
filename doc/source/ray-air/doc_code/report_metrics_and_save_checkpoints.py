# flake8: noqa
# isort: skip_file

# __air_session_start__
import os
import tempfile

import tensorflow as tf
from ray import train
from ray.train import Checkpoint, ScalingConfig
from ray.train.tensorflow import TensorflowTrainer


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


def train_func():
    ckpt = train.get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as loaded_checkpoint_dir:
            import tensorflow as tf

            model = tf.keras.models.load_model(loaded_checkpoint_dir)
    else:
        model = build_model()

    with tempfile.TemporaryDirectory() as tmpdir:
        model.save(tmpdir, overwrite=True)
        train.report(metrics={"iter": 1}, checkpoint=Checkpoint.from_directory(tmpdir))


scaling_config = ScalingConfig(num_workers=2)
trainer = TensorflowTrainer(
    train_loop_per_worker=train_func, scaling_config=scaling_config
)
result = trainer.fit()

# trainer2 will pick up from the checkpoint saved by trainer1.
trainer2 = TensorflowTrainer(
    train_loop_per_worker=train_func,
    scaling_config=scaling_config,
    # this is ultimately what is accessed through
    # ``train.get_checkpoint()``
    resume_from_checkpoint=result.checkpoint,
)
result2 = trainer2.fit()

# __air_session_end__
