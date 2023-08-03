import requests
from starlette.requests import Request
from typing import Dict

from ray import serve


# 1: Define a Ray Serve application.
@serve.deployment(route_prefix="/")
class MyModelDeployment:
    def __init__(self, msg: str):
        # Initialize model state: could be very large neural net weights.
        self._msg = msg

    def __call__(self, request: Request) -> Dict:
        return {"result": self._msg}


app = MyModelDeployment.bind(msg="Hello world!")

# 2: Deploy the application locally.
serve.run(app)

# 3: Query the application and print the result.
print(requests.get("http://localhost:8000/").json())
# {'result': 'Hello world!'}
