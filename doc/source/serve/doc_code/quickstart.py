<<<<<<< HEAD
import requests
from ray import serve


# 1: Define a Ray Serve deployment.
@serve.deployment(route_prefix="/")
class MyModelDeployment:
    def __init__(self, msg: str):
        # Initialize model state: could be very large neural net weights.
        self._msg = msg

    def __call__(self, request):
        return {"result": self._msg}


# 2: Deploy the model.
serve.start()
MyModelDeployment.deploy(msg="Hello world!")

# 3: Query the deployment and print the result.
print(requests.get("http://localhost:8000/").json())
# {'result': 'Hello world!'}
=======
# flake8: noqa
# fmt: off

# __serve_example_begin__
import requests

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier

from ray import serve

serve.start()

# Train model.
iris_dataset = load_iris()
model = GradientBoostingClassifier()
model.fit(iris_dataset["data"], iris_dataset["target"])


@serve.deployment(route_prefix="/iris")
class BoostingModel:
    def __init__(self, model):
        self.model = model
        self.label_list = iris_dataset["target_names"].tolist()

    async def __call__(self, request):
        payload = (await request.json())["vector"]
        print(f"Received http request with data {payload}")

        prediction = self.model.predict([payload])[0]
        human_name = self.label_list[prediction]
        return {"result": human_name}


# Deploy model.
BoostingModel.deploy(model)

# Query it!
sample_request_input = {"vector": [1.2, 1.0, 1.1, 0.9]}
response = requests.get(
    "http://localhost:8000/iris", json=sample_request_input)
print(response.text)
# __serve_example_end__
>>>>>>> f99aa5cb4093227445962d0a7e7ceb8a7d91e46c
