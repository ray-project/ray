from ray import serve
from ray.serve.deployment_graph import RayServeDAGHandle


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
def f(*args):
    return "wonderful world"


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
class BasicDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await (await self.dag.remote())


FNode = f.bind()
DagNode = BasicDriver.bind(FNode)
