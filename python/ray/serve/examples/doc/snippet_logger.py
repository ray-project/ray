import logging
import ray
from ray import serve
import requests

ray.init()
serve.start()

logger = logging.getLogger("ray")


@serve.deployment
def f(*args):
    logger.info("Some info!")


f.deploy()

requests.get("http://127.0.0.1:8000/f")
