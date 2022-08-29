from ray import serve
from ray.serve.deployment_graph import RayServeDAGHandle
import os


@serve.deployment
class f:
    def __init__(self, name: str = "default_name"):
        self.name = name

    def reconfigure(self, config: dict):
        self.name = config.get("name", "default_name")

    async def __call__(self):
        return os.getpid()


@serve.deployment
class BasicDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await self.dag.remote()


node = f.bind()
bnode = BasicDriver.bind(node)
