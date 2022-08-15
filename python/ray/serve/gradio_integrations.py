from ray import serve
from ray.serve._private.http_util import ASGIHTTPSender
from ray.util.annotations import PublicAPI

import starlette

try:
    import gradio as gr
except ModuleNotFoundError:
    print("Gradio isn't installed. Run `pip install gradio` to install Gradio.")
    raise


@PublicAPI(stability="alpha")
class GradioIngress:
    """User-facing class that wraps a Gradio App in a Serve Deployment"""

    def __init__(self, io: gr.Blocks):
        self.app = gr.routes.App.create_app(io)

    async def __call__(self, request: starlette.requests.Request):
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()


GradioServer = serve.deployment(GradioIngress)
