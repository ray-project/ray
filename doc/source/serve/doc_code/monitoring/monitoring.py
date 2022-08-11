# __start__
# File name: monitoring.py

from ray import serve

import logging
import requests

logger = logging.getLogger("ray.serve")


@serve.deployment
class SayHello:
    async def __call__(self, request):
        logger.info("Hello world!")
        return "hi"


say_hello = SayHello.bind()

serve.run(say_hello)

requests.get("http://localhost:8000/")
# __end__

response = requests.get("http://localhost:8000/")
assert response.text == "hi"
