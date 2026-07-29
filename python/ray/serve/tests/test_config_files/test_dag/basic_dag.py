from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment(
    ray_actor_options={
        "num_cpus": 0.1,
    }
)
def f(*args):
    return "wonderful world"


@serve.deployment(
    ray_actor_options={
        "num_cpus": 0.1,
    }
)
class BasicDriver:
    def __init__(self, h: DeploymentHandle):
        self._h = h

    async def __call__(self):
        return await self._h.remote()


FNode = f.bind()
DagNode = BasicDriver.bind(FNode)
