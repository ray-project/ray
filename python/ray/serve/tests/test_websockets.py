import asyncio
import pytest

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

import ray

from ray import serve
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="Streaming feature flag is disabled.",
)
def test_send_recv_text_and_binary(serve_instance):
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

    serve.run(WebSocketServer.bind())

    msg = "Hello world!"
    with connect("ws://localhost:8000") as websocket:
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
        pass

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
            websocket.send("")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
