from starlette.applications import Starlette
from starlette.responses import JSONResponse
import uvicorn

import ray


@ray.remote
class HTTPFrontendActor:
    def __init__(self, ip="0.0.0.0", port=8080, router="DefaultRouter"):
        self.ip = ip
        self.port = port
        self.router = ray.experimental.named_actors.get_actor(router)

    def start(self):
        default_app = Starlette()

        @default_app.route("/{actor}/{method}", methods=["GET", "POST"])
        async def dispatch_remote_function(request):
            data = await request.json()
            actor_name = request.path_params["actor"]
            actor_method = request.path_params["method"]
            
            result_future = self.router.send.remote(actor_name, actor_method, data)
            result = ray.get(result_future)

            return JSONResponse(
                {
                    "success": True,
                    "actor": actor,
                    "method": actor_method,
                    "result": result,
                }
            )

        uvicorn.run(default_app, host=self.ip, port=self.port)
