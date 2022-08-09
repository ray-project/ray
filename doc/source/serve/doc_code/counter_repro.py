from ray import serve

import logging

logger = logging.getLogger("ray.serve")


@serve.deployment
class Counter:
    def __init__(self):
        self.count = 0

    async def __call__(self, request):
        self.count += 1
        return self.count


counter = Counter.bind()
