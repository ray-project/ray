import numpy as np
from tensorflow import keras
from tensorflow.keras import layers

import ray
from ray.data.preprocessors import Concatenator
from ray.train.tensorflow import to_air_checkpoint, TensorflowPredictor
from ray.train.batch_predictor import BatchPredictor


def create_model(input_features):
    return keras.Sequential(
        [
            keras.Input(shape=(input_features,)),
            layers.Dense(16, activation="relu"),
            layers.Dense(16, activation="relu"),
            layers.Dense(1, activation="sigmoid"),
        ]
    )


dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
num_features = len(dataset.schema().names) - 1
dataset = dataset.drop_columns(["target"])

prep = Concatenator(dtype=np.float32)

checkpoint = to_air_checkpoint(model=create_model(num_features), preprocessor=prep)
# You can also fetch a checkpoint from a Trainer
# checkpoint = trainer.fit().checkpoint

batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint, TensorflowPredictor, model_definition=lambda: create_model(num_features)
)

predicted_probabilities = batch_predictor.predict(dataset)
predicted_probabilities.show()
# {'predictions': array([1.], dtype=float32)}
# {'predictions': array([0.], dtype=float32)}
