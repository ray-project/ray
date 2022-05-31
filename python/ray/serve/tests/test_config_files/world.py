from ray import serve
from ray.serve.deployment_graph import RayServeDAGHandle

@serve.deployment
def f(*args):
    return "wonderful world"


@serve.deployment
class BasicDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await self.dag.remote()


FNode = f.bind()
DagNode = BasicDriver.bind(FNode)