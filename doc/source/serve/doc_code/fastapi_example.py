import requests
from fastapi import FastAPI
from ray import serve

# 1: Define a FastAPI app and wrap it in a deployment with a route handler.
app = FastAPI()


@serve.deployment(route_prefix="/")
@serve.ingress(app)
class FastAPIDeployment:
    # FastAPI will automatically parse the HTTP request for us.
    @app.get("/hello")
    def say_hello(self, name: str) -> str:
        return f"Hello {name}!"


# 2: Deploy the deployment.
serve.run(FastAPIDeployment.bind())

# 3: Query the deployment and print the result.
print(requests.get("http://localhost:8000/hello", params={"name": "Theodore"}).json())
# "Hello Theodore!"
