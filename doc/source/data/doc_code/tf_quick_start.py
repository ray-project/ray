# flake8: noqa
# isort: skip_file
# fmt: off

# __tf_quickstart_load_start__
import ray
import numpy as np
from typing import Dict


ds = ray.data.from_numpy(np.ones((1, 100)))
# __tf_quickstart_load_end__


# __tf_quickstart_model_start__
class TFPredictor:
    def __init__(self):  # <1>
        from tensorflow import keras

        input_layer = keras.Input(shape=(100,))
        output_layer = keras.layers.Dense(1, activation="sigmoid")
        self.model = keras.Sequential([input_layer, output_layer])

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict:  # <2>
        return {"output": self.model(batch["data"]).numpy()}
# __tf_quickstart_model_end__


# __tf_quickstart_prediction_start__
tfp = TFPredictor()
batch = ds.take_batch(10)
test = tfp(batch)

scale = ray.data.ActorPoolStrategy(size=2)

predicted_probabilities = ds.map_batches(TFPredictor, compute=scale)
predicted_probabilities.show(limit=1)
# {'output': array([0.45119727])}
# __tf_quickstart_prediction_end__
# fmt: on
