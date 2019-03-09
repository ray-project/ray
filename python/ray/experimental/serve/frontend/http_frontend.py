from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse

import ray


def unwrap(future):
    """Unwrap the result from ray.experimental.server router.
    Router returns a list of object ids when you call them.
    """

    return ray.get(future)[0]


@ray.remote
class HTTPFrontendActor:
    """HTTP API for an Actor. This exposes /{actor_name} endpoint for query.

    Request:
        GET /{actor_name} or POST /{actor_name}
        Content-type: application/json
        {
            "slo_ms": float,
            "input": any
        }
    Response:
        Content-type: application/json
        {
            "success": bool,
            "actor": str,
            "result": any
        }
    """

    def __init__(self, ip="0.0.0.0", port=8080, router="DefaultRouter"):
        self.ip = ip
        self.port = port
        self.router = ray.experimental.named_actors.get_actor(router)

    def start(self):
        default_app = Starlette()

        @default_app.route("/{actor}", methods=["GET", "POST"])
        async def dispatch_remote_function(request):
            data = await request.json()
            actor_name = request.path_params["actor"]

            slo_seconds = data.pop("slo_ms") / 1000
            deadline = time.perf_counter() + slo_seconds

            inp = data.pop("input")

            result_future = unwrap(
                self.router.call.remote(actor_name, inp, deadline))

            # TODO(simon): change to asyncio ray.get
            result = ray.get(result_future)

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        uvicorn.run(default_app, host=self.ip, port=self.port)
