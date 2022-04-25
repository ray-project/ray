Out-of-band Communication
=========================

Normally the actors communication is done through actor method calls and data is shared through distributed object store,
however there are some use cases where out-of-band communication can be used.

Wrap Library Processes
----------------------
Many libraries already have mature, high-performance internal communication stacks and
they leverage Ray as a language-integrated actor scheduler.
The actual communication between actors is mostly done out-of-band using existing communication stacks.
For example, Horovod-on-Ray uses NCCL or MPI-based collective communications, and RayDP uses Spark's internal RPC and object manager.
See `Ray Distributed Library Patterns <https://www.anyscale.com/blog/ray-distributed-library-patterns>`_ for more details.

GPU-to-GPU Data Transfer
-----------
Currently Ray object store only manages CPU memory so efficient GPU-to-GPU data transfer needs to be done out-of-band (e.g. ``cudaMemcpyPeer``).

Http Server
-----------
You can start a http server inside the actor and expose http endpoints to clients
so users outside of the ray cluster can communicate with the actor.

.. tabbed:: Python

    .. code-block:: python

        import ray
        import asyncio
        import requests
        from aiohttp import web

        @ray.remote
        class Counter:
            async def __init__(self):
                self.counter = 0
                asyncio.get_event_loop().create_task(self.run_http_server())

            async def run_http_server(self):
                app = web.Application()
                app.add_routes([web.get("/", self.get)])
                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, "localhost", 8080)
                await site.start()

            async def get(self, request):
                return web.Response(text=str(self.counter))

            async def increment(self):
                self.counter = self.counter + 1

        ray.init()
        counter = Counter.remote()
        [ray.get(counter.increment.remote()) for i in range(5)]
        r = requests.get("http://localhost:8080/")
        # Should print "5"
        print(r.text)

Similarly, you can expose other types of servers as well (e.g. grpc server).
