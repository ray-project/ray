import asyncio
import pytest
import sys

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import requests
from starlette.responses import StreamingResponse
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

import ray

from ray import serve
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
@pytest.mark.parametrize("route_prefix", [None, "/prefix"])
def test_send_recv_text_and_binary(serve_instance, route_prefix: str):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class WebSocketServer:
        @app.websocket("/")
        async def ws_handler(self, ws: WebSocket):
            await ws.accept()

            text = await ws.receive_text()
            await ws.send_text(text)

            bytes = await ws.receive_bytes()
            await ws.send_bytes(bytes)

    if route_prefix is not None:
        WebSocketServer = WebSocketServer.options(route_prefix=route_prefix)

    serve.run(WebSocketServer.bind())

    msg = "Hello world!"
    if route_prefix:
        url = f"ws://localhost:8000{route_prefix}/"
    else:
        url = "ws://localhost:8000/"
    with connect(url) as websocket:
        websocket.send(msg)
        assert websocket.recv() == msg

        websocket.send(msg.encode("utf-8"))
        assert websocket.recv().decode("utf-8") == msg


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
def test_client_disconnect(serve_instance):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class WebSocketServer:
        def __init__(self):
            self._disconnected = asyncio.Event()

        async def wait_for_disconnect(self):
            await self._disconnected.wait()

        @app.websocket("/")
        async def ws_handler(self, ws: WebSocket):
            await ws.accept()

            try:
                await ws.receive_text()
            except WebSocketDisconnect:
                print("Client disconnected.")
                self._disconnected.set()

    h = serve.run(WebSocketServer.bind())
    wait_ref = h.wait_for_disconnect.remote()

    with connect("ws://localhost:8000"):
        print("Client connected.")

    ray.get(wait_ref)


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
def test_server_disconnect(serve_instance):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class WebSocketServer:
        @app.websocket("/")
        async def ws_handler(self, ws: WebSocket):
            await ws.accept()

    serve.run(WebSocketServer.bind())
    with connect("ws://localhost:8000") as websocket:
        with pytest.raises(ConnectionClosed):
            websocket.recv()


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
def test_unary_streaming_websocket_same_deployment(serve_instance):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class RenaissanceMan:
        @app.get("/")
        def say_hi(self):
            return "hi"

        @app.get("/stream")
        def gen_hi(self) -> StreamingResponse:
            def gen():
                for i in range(5):
                    yield "hi"

            return StreamingResponse(gen(), media_type="text/plain")

        @app.websocket("/ws")
        async def ws_hi(self, ws: WebSocket):
            try:
                await ws.accept()
                await ws.send_text(await ws.receive_text())
            except WebSocketDisconnect:
                pass

    serve.run(RenaissanceMan.bind())

    assert requests.get("http://localhost:8000/").json() == "hi"

    r = requests.get("http://localhost:8000/stream", stream=True)
    r.raise_for_status()
    for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
        assert chunk == "hi"

    with connect("ws://localhost:8000/ws") as ws:
        ws.send("hi")
        assert ws.recv() == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
