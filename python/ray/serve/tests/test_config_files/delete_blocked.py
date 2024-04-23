import asyncio

from ray import serve


@serve.deployment
class A:
    async def __del__(self):
        while True:
            await asyncio.sleep(0.1)


app = A.bind()
