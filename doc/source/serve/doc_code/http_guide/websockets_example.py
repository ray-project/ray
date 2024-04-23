# flake8: noqa

# __websocket_serve_app_start__
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from ray import serve


app = FastAPI()


@serve.deployment
@serve.ingress(app)
class EchoServer:
    @app.websocket("/")
    async def echo(self, ws: WebSocket):
        await ws.accept()

        try:
            while True:
                text = await ws.receive_text()
                await ws.send_text(text)
        except WebSocketDisconnect:
            print("Client disconnected.")


serve_app = serve.run(EchoServer.bind())
# __websocket_serve_app_end__

# __websocket_serve_client_start__
from websockets.sync.client import connect

with connect("ws://localhost:8000") as websocket:
    websocket.send("Eureka!")
    assert websocket.recv() == "Eureka!"

    websocket.send("I've found it!")
    assert websocket.recv() == "I've found it!"
# __websocket_serve_client_end__
