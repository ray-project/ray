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

        @default_app.route("/{actor}/replicas", methods=["POST"])
        async def set_replication_factor(request):
            data = await request.json()
            actor_name = request.path_params["actor"]
            num_replicas = data.pop("num_replicas")

            self.router.set_replication_factor.remote(actor_name, num_replicas)
            result = num_replicas

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/replicas", methods=["GET"])
        async def get_replication_factor(request):
            actor_name = request.path_params["actor"]

            result =  self.router.get_replication_factor.remote(actor_name)

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/batch", methods=["POST"])
        async def set_max_batch_size(request):
            data = await request.json()
            actor_name = request.path_params["actor"]
            max_batch_size = data.pop("max_batch_size")

            self.router.set_max_batch_size.remote(actor_name, max_batch_size)
            result = max_batch_size

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/batch", methods=["GET"])
        async def get_max_batch_size(request):
            actor_name = request.path_params["actor"]

            result = self.router.get_max_batch_size.remote(actor_name)

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/resource", methods=["POST"])
        async def use_compute_resource(request):
            actor_name = request.path_params["actor"]
            result = self.router.set_actor_compute_resource.remote(actor_name, await request.json())

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/resource", methods=["GET"])
        async def get_compute_resource(request):
            actor_name = request.path_params["actor"]

            result = self.router.get_actor_compute_resource.remote(actor_name)

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/resource/cpu", methods=["DELETE"])
        async def reset_num_cpus(request):
            actor_name = request.path_params["actor"]

            result = self.router.reset_actor_compute_resource.remote(actor_name, 'num_cpus')

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/resource/gpu", methods=["DELETE"])
        async def reset_num_gpus(request):
            actor_name = request.path_params["actor"]

            result = self.router.reset_actor_compute_resource.remote(actor_name, 'num_gpus')

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        @default_app.route("/{actor}/resource/cpu", methods=["DELETE"])
        async def reset_resources(request):
            actor_name = request.path_params["actor"]

            result = self.router.reset_actor_compute_resource.remote(actor_name, 'resources')

            return JSONResponse({
                "success": True,
                "actor": actor_name,
                "result": result
            })
        uvicorn.run(default_app, host=self.ip, port=self.port)