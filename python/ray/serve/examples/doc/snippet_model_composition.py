from random import random

import requests

from ray import serve

serve.init()


def model_one(_unused_flask_request, data=None):
    print("Model 1 called with data ", data)
    return random()


def model_two(_unused_flask_request, data=None):
    print("Model 2 called with data ", data)
    return data


class ComposedModel:
    def __init__(self):
        self.model_one = serve.get_handle("model_one")
        self.model_two = serve.get_handle("model_two")

    async def __call__(self, flask_request):
        data = flask_request.data

        score = await self.model_one.remote(data=data)
        if score > 0.5:
            result = await self.model_two.remote(data=data)
            result = {"model_used": 2, "score": score}
        else:
            result = {"model_used": 1, "score": score}

        return result


serve.create_backend("model_one", model_one)
serve.create_endpoint("model_one", backend="model_one")

serve.create_backend("model_two", model_two)
serve.create_endpoint("model_two", backend="model_two")

serve.create_backend(
    "composed_backend", ComposedModel, config={"max_concurrent_queries": 10})
serve.create_endpoint(
    "composed", backend="composed_backend", route="/composed")

for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/composed", data="hey!")
    print(resp.json())
# Output
# {'model_used': 2, 'score': 0.6250189863595503}
# {'model_used': 1, 'score': 0.03146855349621436}
# {'model_used': 2, 'score': 0.6916977560006987}
# {'model_used': 2, 'score': 0.8169693450866928}
# {'model_used': 2, 'score': 0.9540681979573862}
