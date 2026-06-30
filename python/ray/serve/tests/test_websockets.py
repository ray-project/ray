import asyncio
import sys

import httpx
import pytest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.responses import StreamingResponse
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.test_utils import get_application_url, get_metric_dictionaries


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

    serve.run(WebSocketServer.bind(), route_prefix=route_prefix or "/")

    msg = "Hello world!"
    url = f"{get_application_url(is_websocket=True, use_localhost=True)}/"

    with connect(url) as websocket:
        websocket.send(msg)
        assert websocket.recv() == msg

        websocket.send(msg.encode("utf-8"))
        assert websocket.recv().decode("utf-8") == msg


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
    wait_response = h.wait_for_disconnect.remote()
    url = f"{get_application_url(is_websocket=True)}/"

    with connect(url):
        print("Client connected.")

    wait_response.result()


def test_client_disconnect_records_request_metric(serve_instance):
    """Test that a mid-stream WebSocket client disconnect is still counted in request metrics."""
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class WebSocketServer:
        @app.websocket("/")
        async def ws_handler(self, ws: WebSocket):
            await ws.accept()
            # Stream densely so the proxy is mid-send when the client drops.
            try:
                while True:
                    await ws.send_text("data")
            except (WebSocketDisconnect, ConnectionClosed, RuntimeError):
                pass

    app_name = "ws_disconnect_metric_app"
    serve.run(WebSocketServer.bind(), name=app_name)
    url = f"{get_application_url(is_websocket=True, use_localhost=True, app_name=app_name)}/"

    # Drain frames, then drop the socket abruptly to force a mid-send disconnect.
    with connect(url) as client:
        for _ in range(5):
            client.recv()
        client.socket.close()

    # Metric is cumulative across the session fixture; require this app's 1006 count.
    def request_recorded() -> bool:
        reqs = get_metric_dictionaries("ray_serve_num_http_requests_total")
        ws_reqs = [
            m for m in reqs if m["method"] == "WS" and m["application"] == app_name
        ]
        assert ws_reqs, f"WS request for {app_name} not recorded: {reqs}"
        assert any(
            m["status_code"] == "1006" for m in ws_reqs
        ), f"expected disconnect code 1006, got: {ws_reqs}"
        return True

    wait_for_condition(request_recorded, timeout=20)


@pytest.mark.skipif(sys.platform == "win32", reason="Hanging on Windows.")
def test_server_disconnect(serve_instance):
    """Test that server can properly close WebSocket connections."""
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class WebSocketServer:
        @app.websocket("/")
        async def ws_handler(self, ws: WebSocket):
            await ws.accept()
            # Wait for client message, then close with specific code
            message = await ws.receive_text()
            close_code = int(message)
            await ws.close(code=close_code)

    serve.run(WebSocketServer.bind())
    url = f"{get_application_url(is_websocket=True)}/"

    # Test normal close (code 1000)
    with connect(url) as websocket:
        websocket.send("1000")
        with pytest.raises(ConnectionClosed):
            websocket.recv()

    # Test abnormal close (code 1011)
    with connect(url) as websocket:
        websocket.send("1011")
        with pytest.raises(ConnectionClosed):
            websocket.recv()


def test_unary_streaming_websocket_same_deployment(serve_instance):
    app = FastAPI()

    signal_actor = SignalActor.remote()

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
                    ray.get(signal_actor.wait.remote())

            return StreamingResponse(gen(), media_type="text/plain")

        @app.websocket("/ws")
        async def ws_hi(self, ws: WebSocket):
            try:
                await ws.accept()
                await ws.send_text(await ws.receive_text())
            except WebSocketDisconnect:
                pass

    serve.run(RenaissanceMan.bind())

    http_url = get_application_url()
    assert httpx.get(http_url).json() == "hi"

    with httpx.stream("GET", f"{http_url}/stream") as r:
        r.raise_for_status()
        for chunk in r.iter_text():
            assert chunk == "hi"
            ray.get(signal_actor.send.remote())

    url = get_application_url(is_websocket=True)
    with connect(f"{url}/ws") as ws:
        ws.send("hi")
        assert ws.recv() == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
