import ray
import asyncio
from aiohttp import web
import ray.cloudpickle as cloudpickle

"""
A script that spins up a detached actor and exits.
TODO: now there's no way for a driver to send back info to the job submission client. So
we have no way to tell the client our http address. let's add
change-job-metadata-within-job or return-value feature to the job submission. Before
that I will use fixed http addr.
"""


@ray.remote
class DriverActor:
    def __init__(self, name) -> None:

        # if client died and restarted, it can reconnect via this name as actor name
        self.client_session_name = name
        # TODO: how to do gc?
        self.put_objects = []
        self.running_jobs = []
        self.actors = []

        asyncio.get_running_loop().create_task(self.run_http_server())

    async def run_http_server(self):
        app = web.Application()
        app.add_routes(
            [
                web.post("/get", self.get),
                web.post("/put", self.put),
                web.post("/remotefunction/remote", self.remotefunction_remote),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        # TODO: properly set ip address + port
        # TODO: how to send this addr back to the client?
        site = web.TCPSite(runner, "127.0.0.1", 25001)
        print("running http server!")
        await site.start()

    async def get(self, request):
        body = await request.read()
        obj_ref = ray.ObjectRef(body)
        print(f"get ref {obj_ref}")
        obj = await obj_ref
        print(f"got {obj} from {obj_ref}")
        data = cloudpickle.dumps(obj)
        return web.Response(body=data)

    async def put(self, request):
        body = await request.read()
        obj = cloudpickle.loads(body)
        obj_ref = ray.put(obj)
        self.put_objects.append(obj_ref)  # keeping the obj refs...
        print(f"put {body} to {obj_ref}")
        return web.Response(body=obj_ref.binary())

    async def remotefunction_remote(self, request):
        body = await request.read()
        func, args, kwargs, task_options = cloudpickle.loads(body)
        print(f"remote: {(func, args, kwargs, task_options)}")
        obj_ref = ray.remote(func)._remote(args, kwargs, **task_options)
        self.put_objects.append(obj_ref)  # keeping the obj refs...
        print(f"remotefunction_remote {func} to {obj_ref}")
        return web.Response(body=obj_ref.binary())


import time

if __name__ == "__main__":
    ray.init()
    # TODO: args: client_session_name
    # TODO: only create if actor not already exists
    # TODO: make "detach" configurable by job submission args
    DriverActor.options(name="client_session_name", lifetime="detached").remote(
        "client_session_name"
    )
    # TODO: if detach, make health check ping and return
    while True:
        time.sleep(1000)
