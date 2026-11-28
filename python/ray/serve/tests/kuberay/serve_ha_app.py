"""Serve app for the KubeRay high-availability test in serve_ha.py.

Each request crosses every deployment: Ingress -> a -> (b, d, e) -> c.
For input `val`, the response is `3 * val + 6`.
"""

from starlette.requests import Request

from ray import serve
from ray.serve.handle import DeploymentHandle

# Two replicas per deployment, at most one per node, so deleting any one worker
# Pod leaves every deployment with a running replica.
deployment = serve.deployment(
    num_replicas=2, max_replicas_per_node=1, ray_actor_options={"num_cpus": 0.1}
)


@deployment
def a(val: int) -> int:
    return val


@deployment
def b(val: int) -> int:
    return val + 1


@deployment
def c(v1: int, v2: int, v3: int) -> int:
    return v1 + v2 + v3


@deployment
def d(val: int) -> int:
    return val + 2


@deployment
def e(val: int) -> int:
    return val + 3


@deployment
class Ingress:
    def __init__(
        self,
        a: DeploymentHandle,
        b: DeploymentHandle,
        c: DeploymentHandle,
        d: DeploymentHandle,
        e: DeploymentHandle,
    ):
        self._a, self._b, self._c, self._d, self._e = a, b, c, d, e

    async def __call__(self, request: Request) -> int:
        oa = self._a.remote(int(request.query_params["val"]))
        return await self._c.remote(
            self._b.remote(oa), self._d.remote(oa), self._e.remote(oa)
        )


app = Ingress.bind(a.bind(), b.bind(), c.bind(), d.bind(), e.bind())
