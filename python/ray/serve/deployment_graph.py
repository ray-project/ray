import ray
from ray import cloudpickle
from ray.util.annotations import PublicAPI

from ray.dag.class_node import ClassNode  # noqa: F401
from ray.dag.function_node import FunctionNode  # noqa: F401
from ray.dag.input_node import InputNode  # noqa: F401
from ray.dag import DAGNode  # noqa: F401


@PublicAPI(stability="alpha")
class RayServeDAGHandle:
    """Resolved from a DeploymentNode at runtime.

    This can be used to call the DAG from a driver deployment to efficiently
    orchestrate a deployment graph.
    """

    def __init__(self, pickled_dag_node: bytes) -> None:
        self.pickled_dag_node = pickled_dag_node

        # NOTE(simon): Making this lazy to avoid deserialization in controller for now
        # This would otherwise hang because it's trying to get handles from within
        # the controller.
        self.dag_node = None

    @classmethod
    def _deserialize(cls, *args):
        """Required for this class's __reduce__ method to be pickleable."""
        return cls(*args)

    def __reduce__(self):
        return RayServeDAGHandle._deserialize, (self.pickled_dag_node,)

    async def remote(
        self, *args, _ray_cache_refs: bool = False, **kwargs
    ) -> ray.ObjectRef:
        """Execute the request, returns a ObjectRef representing final result."""
        if self.dag_node is None:
            self.dag_node = cloudpickle.loads(self.pickled_dag_node)

        return await self.dag_node.execute(
            *args, _ray_cache_refs=_ray_cache_refs, **kwargs
        )
