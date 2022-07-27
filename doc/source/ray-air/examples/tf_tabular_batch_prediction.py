from typing import List
import numpy as np

import ray
from ray.data.preprocessors import Concatenator
from ray.train.tensorflow import TensorflowCheckpoint, TensorflowPredictor
from ray.train.batch_predictor import BatchPredictor


def create_model(input_features):
    from tensorflow import keras  # this is needed for tf<2.9
    from tensorflow.keras import layers

    return keras.Sequential(
        [
            keras.Input(shape=(input_features,)),
            layers.Dense(16, activation="relu"),
            layers.Dense(16, activation="relu"),
            layers.Dense(1, activation="sigmoid"),
        ]
    )


dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
all_features: List[str] = dataset.schema().names
all_features.remove("target")
num_features = len(all_features)

prep = Concatenator(dtype=np.float32)

checkpoint = TensorflowCheckpoint.from_model(
    model=create_model(num_features), preprocessor=prep
)
# You can also fetch a checkpoint from a Trainer
# checkpoint = trainer.fit().checkpoint

batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint, TensorflowPredictor, model_definition=lambda: create_model(num_features)
)

predicted_probabilities = batch_predictor.predict(dataset, feature_columns=all_features)
predicted_probabilities.show()
# {'predictions': array([1.], dtype=float32)}
# {'predictions': array([0.], dtype=float32)}
