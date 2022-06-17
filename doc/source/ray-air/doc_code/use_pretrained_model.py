# flake8: noqa
# isort: skip_file

# __use_predictor_start__
import ray
import tensorflow as tf
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    to_air_checkpoint,
    TensorflowPredictor,
)

# This can be a trained model.
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
predictor = TensorflowPredictor.from_checkpoint(
    checkpoint, model_definition=build_model)

data = np.array([[1, 2], [3, 4]])
predictions = predictor.predict(data)
# __use_predictor_end__

# __use_pretrained_model_start__
import ray
import tensorflow as tf
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    to_air_checkpoint,
    TensorflowPredictor,
)


# This can be a trained model
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
