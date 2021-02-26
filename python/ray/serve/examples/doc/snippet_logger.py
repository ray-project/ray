import logging
import ray
from ray import serve
import requests

ray.init()
client = serve.start()

logger = logging.getLogger("ray")


def f(request):
    logger.info("Some info!")


client.create_backend("my_backend", f)
client.create_endpoint("my_endpoint", backend="my_backend", route="/f")

requests.get("http://127.0.0.1:8000/f")
