import ray
import asyncio
import requests
from aiohttp import web


@ray.remote
class Counter:
    async def __init__(self):
        self.counter = 0
        asyncio.get_running_loop().create_task(self.run_http_server())

    async def run_http_server(self):
        app = web.Application()
        app.add_routes([web.get("/", self.get)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 25001)
        await site.start()

    async def get(self, request):
        return web.Response(text=str(self.counter))

    async def increment(self):
        self.counter = self.counter + 1


ray.init()
counter = Counter.remote()
[ray.get(counter.increment.remote()) for i in range(5)]
r = requests.get("http://127.0.0.1:25001/")
assert r.text == "5"
