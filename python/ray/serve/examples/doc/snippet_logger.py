import logging

from ray import serve
import requests

serve.start()

logger = logging.getLogger("ray")


@serve.deployment
def f(*_args):
    logger.info("Some info!")


f.deploy()

requests.get("http://127.0.0.1:8000/f")
