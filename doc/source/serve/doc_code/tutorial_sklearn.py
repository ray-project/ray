# fmt: off
# __doc_import_begin__
from ray import serve

import pickle
import json
import numpy as np
import os
import tempfile
from starlette.requests import Request
from typing import Dict

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import mean_squared_error
# __doc_import_end__
# fmt: on

# __doc_instantiate_model_begin__
model = GradientBoostingClassifier()
# __doc_instantiate_model_end__

# __doc_data_begin__
iris_dataset = load_iris()
data, target, target_names = (
    iris_dataset["data"],
    iris_dataset["target"],
    iris_dataset["target_names"],
)

np.random.shuffle(data), np.random.shuffle(target)
train_x, train_y = data[:100], target[:100]
val_x, val_y = data[100:], target[100:]
# __doc_data_end__

# __doc_train_model_begin__
model.fit(train_x, train_y)
print("MSE:", mean_squared_error(model.predict(val_x), val_y))

# Save the model and label to file
MODEL_PATH = os.path.join(
    tempfile.gettempdir(), "iris_model_gradient_boosting_classifier.pkl"
)
LABEL_PATH = os.path.join(tempfile.gettempdir(), "iris_labels.json")

with open(MODEL_PATH, "wb") as f:
    pickle.dump(model, f)
with open(LABEL_PATH, "w") as f:
    json.dump(target_names.tolist(), f)
# __doc_train_model_end__


# __doc_define_servable_begin__
@serve.deployment
class BoostingModel:
    def __init__(self, model_path: str, label_path: str):
        with open(model_path, "rb") as f:
            self.model = pickle.load(f)
        with open(label_path) as f:
            self.label_list = json.load(f)

    async def __call__(self, starlette_request: Request) -> Dict:
        payload = await starlette_request.json()
        print("Worker: received starlette request with data", payload)

        input_vector = [
            payload["sepal length"],
            payload["sepal width"],
            payload["petal length"],
            payload["petal width"],
        ]
        prediction = self.model.predict([input_vector])[0]
        human_name = self.label_list[prediction]
        return {"result": human_name}
        # __doc_define_servable_end__


# __doc_deploy_begin__
boosting_model = BoostingModel.bind(MODEL_PATH, LABEL_PATH)
# __doc_deploy_end__
