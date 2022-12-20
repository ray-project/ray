# flake8: noqa
# isort: skip_file

# __air_session_start__

import tensorflow as tf
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig
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
    ckpt = session.get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as loaded_checkpoint_dir:
            import tensorflow as tf

            model = tf.keras.models.load_model(loaded_checkpoint_dir)
    else:
        model = build_model()

    model.save("my_model", overwrite=True)
    session.report(
        metrics={"iter": 1}, checkpoint=Checkpoint.from_directory("my_model")
    )


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
    # ``Session.get_checkpoint()``
    resume_from_checkpoint=result.checkpoint,
)
result2 = trainer2.fit()

# __air_session_end__
