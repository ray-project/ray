import requests
from ray import serve
import tensorflow as tf

serve.start()


@serve.deployment
def tf_version(request):
    return ("Tensorflow " + tf.__version__)


tf_version.options(
    name="tf1", ray_actor_options={
        "runtime_env": {
            "conda": "ray-tf1"
        }
    }).deploy()
tf_version.options(
    name="tf2", ray_actor_options={
        "runtime_env": {
            "conda": "ray-tf2"
        }
    }).deploy()

print(requests.get("http://127.0.0.1:8000/tf1").text)  # Tensorflow 1.15.0
print(requests.get("http://127.0.0.1:8000/tf2").text)  # Tensorflow 2.3.0
