import requests
from ray import serve
import tensorflow as tf

serve.start()


@serve.deployment
def tf_version(request):
    return ("Tensorflow " + tf.__version__)


tf_version.options(
    name="tf1",
    ray_actor_options={
        "runtime_env": {
            "pip": ["ray[serve]", "tensorflow==1.15.0"]
        }
    }).deploy()
tf_version.options(
    name="tf2",
    ray_actor_options={
        "runtime_env": {
            "pip": ["ray[serve]", "tensorflow==2.3.0"]
        }
    }).deploy()

assert requests.get("http://127.0.0.1:8000/tf1").text == "1.15.0"
assert requests.get("http://127.0.0.1:8000/tf2").text == "2.3.0"
