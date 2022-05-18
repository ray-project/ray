import gradio as gr
import starlette

from ray.serve.http_util import ASGIHTTPSender


class GradioIngress:
    def __init__(self, io):
        self.app = gr.networking.configure_app(gr.routes.create_app(), io)

    async def __call__(self, request: starlette.requests.Request):
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()
