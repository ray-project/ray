import ray
import numpy as np
from tensorflow import keras


class TFPredictor:
    def __init__(self):

        input_layer = keras.Input(shape=(100,))
        output_layer = keras.layers.Dense(1, activation="sigmoid")
        self.model = keras.Sequential([input_layer, output_layer])

    def __call__(self, batch: np.ndarray):
        return self.model(batch).numpy()


dataset = ray.data.from_numpy(np.ones((1, 100)))
scale = ray.data.ActorPoolStrategy(2)

predicted_probabilities = dataset.map_batches(TFPredictor, compute=scale)
predicted_probabilities.show(limit=1)
# [0.45119727]
