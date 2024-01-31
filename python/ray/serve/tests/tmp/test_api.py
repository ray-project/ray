import requests
from fastapi import FastAPI

from ray import serve

app = FastAPI()

@serve.deployment(route_prefix="/hello")
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"

# Test FastAPI
serve.run(MyFastAPIDeployment.bind(), name="FastAPI")
assert requests.get("http://127.0.0.1:8000/hello").text == '"Hello, world!"'
