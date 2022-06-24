import requests
from ray import serve

serve.start()


@serve.deployment
def requests_version(request):
    return requests.__version__


requests_version.options(
    name="25",
    ray_actor_options={"runtime_env": {"pip": ["requests==2.25.1"]}},
).deploy()
requests_version.options(
    name="26",
    ray_actor_options={"runtime_env": {"pip": ["requests==2.26.0"]}},
).deploy()

assert requests.get("http://127.0.0.1:8000/25").text == "2.25.1"
assert requests.get("http://127.0.0.1:8000/26").text == "2.26.0"
