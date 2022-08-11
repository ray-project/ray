# __start_monitoring__
# File name: monitoring.py

from ray import serve

import logging
import requests
from starlette.requests import Request

logger = logging.getLogger("ray.serve")


@serve.deployment
class SayHello:
    async def __call__(self, request: Request) -> str:
        logger.info("Hello world!")
        return "hi"


say_hello = SayHello.bind()

serve.run(say_hello)

requests.get("http://localhost:8000/")
# __end_monitoring__
