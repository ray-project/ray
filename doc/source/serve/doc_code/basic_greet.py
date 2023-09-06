import requests

# __serve_example_begin__
from starlette.requests import Request

from ray import serve


@serve.deployment
async def greeter(request: Request) -> str:
    name = (await request.json())["name"]
    return {"greeting": f"Good morning {name}!"}


app = greeter.bind()
# __serve_example_end__

# Test
if __name__ == "__main__":
    serve.run(app)
    resp = requests.post("http://localhost:8000/", json={"name": "Bob"}).json()
    assert resp["greeting"] == "Good morning Bob!", resp
