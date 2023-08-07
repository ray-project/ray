from ray import serve
import asyncio


@serve.deployment
class A:
    async def __del__(self):
        while True:
            await asyncio.sleep(0.1)


app = A.bind()
