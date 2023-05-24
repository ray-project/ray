import asyncio
import pytest
from typing import Generator

from fastapi import FastAPI
import requests
from starlette.responses import StreamingResponse
from starlette.requests import Request

import ray
from ray import serve
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING

def make_streaming_request() -> Generator[str, None, None]:
    r = requests.get("http://localhost:8000", stream=True)
    r.raise_for_status()
    for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
        yield chunk

@pytest.mark.skipif(not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING, reason="Streaming feature flag is disabled.")
@pytest.mark.parametrize("use_fastapi", [False, True])
def test_basic(serve_instance, use_fastapi: bool):
    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            async def hi_gen(self):
                for i in range(10):
                    yield f"hi_{i}"
                    await asyncio.sleep(0.01)

            @app.get("/")
            def stream_hi(self, request: Request) -> StreamingResponse:
                return StreamingResponse(self.hi_gen(), media_type="text/plain")
    else:
        @serve.deployment
        class SimpleGenerator:
            async def hi_gen(self):
                for i in range(10):
                    yield f"hi_{i}"
                    await asyncio.sleep(0.01)

            def __call__(self, request: Request) -> StreamingResponse:
                return StreamingResponse(self.hi_gen(), media_type="text/plain")

    serve.run(SimpleGenerator.bind())

    for i, chunk in enumerate(make_streaming_request()):
        assert chunk == f"hi_{i}"

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
