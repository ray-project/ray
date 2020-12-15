import ray
from ray import serve
import requests

ray.init()
client = serve.start()

class Endpoint:
    def __call__(self, request):
        print(request.args)

client.create_backend("backend", Endpoint)
client.create_endpoint(
    "endpoint", backend="backend", route="/endpoint", methods=["POST"])

ground_truth = {
    "args": {
        "arg1": "1",
        "arg2": "2"
    },
    "headers": {
        "X-Custom-Header": "value"
    },
    "method": "POST",
    "json": {
        "json_key": "json_val"
    }
}

resp_web = requests.post(
    "http://127.0.0.1:8000/endpoint?arg1=1&arg2=2",
    headers=ground_truth["headers"],
    json=ground_truth["json"]).json()

handle = client.get_handle("endpoint")
resp_handle = ray.get(
    handle.options(
        http_method=ground_truth["method"],
        http_headers=ground_truth["headers"]).remote(
            ground_truth["json"], **ground_truth["args"]))

for resp in [resp_web, resp_handle]:
    for field in ["args", "method", "json"]:
        pass # assert resp[field] == ground_truth[field]
    resp["headers"]["X-Custom-Header"] == "value"