from fastapi import FastAPI, Request, Response

from ray import serve

http_app = FastAPI()
router_app = FastAPI()


@serve.deployment(graceful_shutdown_timeout_s=1, graceful_shutdown_wait_loop_s=0.1)
@serve.ingress(http_app)
class Ingress:
    @http_app.post("/")
    async def handle_request(self, request: Request):
        return await request.json()


@serve.deployment(graceful_shutdown_timeout_s=1, graceful_shutdown_wait_loop_s=0.1)
@serve.ingress(router_app)
class Router:
    def __init__(self):
        self._num_requests = 0

    async def get_num_requests(self):
        return self._num_requests

    @router_app.post("/internal/route")
    async def route(self):
        self._num_requests += 1
        return Response(status_code=503)


app = Ingress.bind()._with_ingress_request_router(Router.bind())
