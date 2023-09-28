from fastapi import FastAPI
from ray import serve
from starlette.responses import StreamingResponse
from starlette.requests import Request
import requests
import time
import asyncio
from  aviary.backend.server.models import AviaryModelResponse
import time


app = FastAPI()


@serve.deployment(max_concurrent_queries=1000)
@serve.ingress(app)
class Router:
    def __init__(self, handle) -> None:
        self._h = handle.options(stream=True)
        self.total_recieved = 0

    @app.get("/")
    def stream_hi(self, request: Request) -> StreamingResponse:
        async def consume_obj_ref_gen():
            obj_ref_gen = await self._h.hi_gen.remote()
            start = time.time()
            num_recieved = 0
            async for chunk in obj_ref_gen:
                chunk = await chunk
                num_recieved +=1 
                yield str(chunk.json())
            delta = time.time() - start
            print(f"**request throughput: {num_recieved / delta}")
        return StreamingResponse(consume_obj_ref_gen(), media_type="text/plain")


@serve.deployment(max_concurrent_queries=1000)
class SimpleGenerator:
    async def hi_gen(self):
        start = time.time()
        for i in range(100):
            #await asyncio.sleep(0.001)
            time.sleep(0.001) # if change to async sleep, i don't see crash.
            yield AviaryModelResponse(generated_text="abcd")
        delta = time.time() - start
        print(f"**model throughput: {100 / delta}")


serve.run(Router.bind(SimpleGenerator.bind()))

time.sleep(3600)
