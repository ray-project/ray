# fmt: off
import ray
# __doc_import_begin__
from ray import serve

import pickle
import json
import numpy as np
import requests
import os
import tempfile

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import mean_squared_error
# __doc_import_end__
# fmt: on

# __doc_train_model_begin__
# Load data
iris_dataset = load_iris()
data, target, target_names = (
    iris_dataset["data"],
    iris_dataset["target"],
    iris_dataset["target_names"],
)

# Instantiate model
model = GradientBoostingClassifier()

# Training and validation split
np.random.shuffle(data), np.random.shuffle(target)
train_x, train_y = data[:100], target[:100]
val_x, val_y = data[100:], target[100:]

# Train and evaluate models
model.fit(train_x, train_y)
print("MSE:", mean_squared_error(model.predict(val_x), val_y))

# Save the model and label to file
MODEL_PATH = os.path.join(tempfile.gettempdir(), "iris_model_logistic_regression.pkl")
LABEL_PATH = os.path.join(tempfile.gettempdir(), "iris_labels.json")

with open(MODEL_PATH, "wb") as f:
    pickle.dump(model, f)
with open(LABEL_PATH, "w") as f:
    json.dump(target_names.tolist(), f)
# __doc_train_model_end__


# __doc_define_servable_begin__
@serve.deployment(route_prefix="/regressor")
class BoostingModel:
    def __init__(self):
        with open(MODEL_PATH, "rb") as f:
            self.model = pickle.load(f)
        with open(LABEL_PATH) as f:
            self.label_list = json.load(f)

    async def __call__(self, starlette_request):
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

ray.init(num_cpus=8)
# __doc_deploy_begin__
serve.start()
BoostingModel.deploy()
# __doc_deploy_end__

# __doc_query_begin__
sample_request_input = {
    "sepal length": 1.2,
    "sepal width": 1.0,
    "petal length": 1.1,
    "petal width": 0.9,
}
response = requests.get("http://localhost:8000/regressor", json=sample_request_input)
print(response.text)
# Result:
# {
#  "result": "versicolor"
# }
# __doc_query_end__
