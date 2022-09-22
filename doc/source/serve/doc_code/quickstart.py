import requests
from starlette.requests import Request
from typing import Dict

from ray import serve


# 1: Define a Ray Serve deployment.
@serve.deployment(route_prefix="/")
class MyModelDeployment:
    def __init__(self, msg: str):
        # Initialize model state: could be very large neural net weights.
        self._msg = msg

    def __call__(self, request: Request) -> Dict:
        return {"result": self._msg}


# 2: Deploy the model.
serve.run(MyModelDeployment.bind(msg="Hello world!"))

# 3: Query the deployment and print the result.
print(requests.get("http://localhost:8000/").json())
# {'result': 'Hello world!'}
