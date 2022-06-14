# flake8: noqa

import tensorflow as tf

# __use_pretrained_model_start__
import ray
from ray.air.batch_predictor import BatchPredictor
from ray.train.tensorflow import TensorflowPredictor, to_air_checkpoint


# to simulate having a pretrained model.
def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


model = build_model()
checkpoint = to_air_checkpoint(model)
batch_predictor = BatchPredictor(
    checkpoint, TensorflowPredictor, model_definition=build_model
)
predict_dataset = ray.data.range(3)
predictions = batch_predictor.predict(predict_dataset)

# __use_pretrained_model_end__
