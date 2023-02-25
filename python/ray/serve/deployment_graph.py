import json
import os
import ray

from ray.dag.class_node import ClassNode  # noqa: F401
from ray.dag.function_node import FunctionNode  # noqa: F401
from ray.dag.input_node import InputNode  # noqa: F401
from ray.dag import DAGNode  # noqa: F401
from ray.serve._private.constants import (
    SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY,
)
from ray.util.annotations import PublicAPI

FLAG_SERVE_DEPLOYMENT_HANDLE_IS_SYNC = (
    os.environ.get(SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY, "0") == "1"
)


@PublicAPI(stability="alpha")
class RayServeDAGHandle:
    """Resolved from a DeploymentNode at runtime.

    This can be used to call the DAG from a driver deployment to efficiently
    orchestrate a deployment graph.
    """

    def __init__(self, dag_node_json: str) -> None:
        self.dag_node_json = dag_node_json

        # NOTE(simon): Making this lazy to avoid deserialization in controller for now
        # This would otherwise hang because it's trying to get handles from within
        # the controller.
        self.dag_node = None

    @classmethod
    def _deserialize(cls, *args):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(*args)

    def __reduce__(self):
        return RayServeDAGHandle._deserialize, (self.dag_node_json,)

    async def remote(
        self, *args, _ray_cache_refs: bool = False, **kwargs
    ) -> ray.ObjectRef:
        """Execute the request, returns a ObjectRef representing final result."""
        if self.dag_node is None:
            from ray.serve._private.json_serde import dagnode_from_json

            self.dag_node = json.loads(
                self.dag_node_json, object_hook=dagnode_from_json
            )

        if FLAG_SERVE_DEPLOYMENT_HANDLE_IS_SYNC:
            return self.dag_node.execute(
                *args, _ray_cache_refs=_ray_cache_refs, **kwargs
            )
        else:
            return await self.dag_node.execute(
                *args, _ray_cache_refs=_ray_cache_refs, **kwargs
            )
