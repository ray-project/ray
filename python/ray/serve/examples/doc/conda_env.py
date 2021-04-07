import requests
from ray import serve
import tensorflow as tf

serve.start()


def tf_version(request):
    return ("Tensorflow " + tf.__version__)


serve.create_backend(
    "tf1", tf_version, ray_actor_options={"runtime_env": {
        "conda": "ray-tf1"
    }})
serve.create_endpoint("tf1", backend="tf1", route="/tf1")
serve.create_backend(
    "tf2", tf_version, ray_actor_options={"runtime_env": {
        "conda": "ray-tf2"
    }})
serve.create_endpoint("tf2", backend="tf2", route="/tf2")

print(requests.get("http://127.0.0.1:8000/tf1").text)  # Tensorflow 1.15.0
print(requests.get("http://127.0.0.1:8000/tf2").text)  # Tensorflow 2.3.0
