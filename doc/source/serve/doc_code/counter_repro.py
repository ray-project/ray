from ray import serve

import logging
from starlette.requests import Request

logger = logging.getLogger("ray.serve")


@serve.deployment
class Counter:
    def __init__(self):
        self.count = 0

    async def __call__(self, request: Request) -> int:
        self.count += 1
        return self.count


counter = Counter.bind()
