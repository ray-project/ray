# flake8: noqa

# __start__
# File name: monitoring.py

from ray import serve
import logging
from starlette.requests import Request

logger = logging.getLogger("ray.serve")


@serve.deployment
class SayHello:
    async def __call__(self, request: Request) -> str:
        logger.info("Hello world!")
        return "hi"


say_hello = SayHello.bind()
# __end__

# serve.run(say_hello)

# import requests
# response = requests.get("http://localhost:8000/")
# assert response.text == "hi"
