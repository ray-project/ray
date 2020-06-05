# yapf: disable
# __doc_import_begin__
from ray import serve

import pickle
import json
import numpy as np
import requests

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import mean_squared_error
# __doc_import_end__
# yapf: enable

# __doc_train_model_begin__
# Load data
iris_dataset = load_iris()
data, target, target_names = iris_dataset["data"], iris_dataset[
    "target"], iris_dataset["target_names"]

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
with open("/tmp/iris_model_logistic_regression.pkl", "wb") as f:
    pickle.dump(model, f)
with open("/tmp/iris_labels.json", "w") as f:
    json.dump(target_names.tolist(), f)
# __doc_train_model_end__


# __doc_define_servable_begin__
class BoostingModel:
    def __init__(self):
        with open("/tmp/iris_model_logistic_regression.pkl", "rb") as f:
            self.model = pickle.load(f)
        with open("/tmp/iris_labels.json") as f:
            self.label_list = json.load(f)

    def __call__(self, flask_request):
        payload = flask_request.json
        print("Worker: received flask request with data", payload)

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
serve.init()
serve.create_backend("lr:v1", BoostingModel)
serve.create_endpoint("iris_classifier", backend="lr:v1", route="/regressor")
# __doc_deploy_end__

# __doc_query_begin__
sample_request_input = {
    "sepal length": 1.2,
    "sepal width": 1.0,
    "petal length": 1.1,
    "petal width": 0.9,
}
response = requests.get(
    "http://localhost:8000/regressor", json=sample_request_input)
print(response.text)
# Result:
# {
#  "result": "versicolor"
# }
# __doc_query_end__
