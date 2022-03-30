import json
from ray.experimental.dag.class_node import ClassNode
from ray.experimental.dag.function_node import FunctionNode
from ray.experimental.dag import DAGNode
from ray.util.annotations import PublicAPI


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

    def remote(self, *args, **kwargs):
        from ray.serve.pipeline.json_serde import dagnode_from_json

        if self.dag_node is None:
            self.dag_node = json.loads(
                self.dag_node_json, object_hook=dagnode_from_json
            )
        return self.dag_node.execute(*args, **kwargs)


@PublicAPI(stability="alpha")
class DeploymentMethodNode(DAGNode):
    """Represents a method call on a bound deployment node.

    These method calls can be composed into an optimized call DAG and passed
    to a "driver" deployment that will orchestrate the calls at runtime.

    This class cannot be called directly. Instead, when it is bound to a
    deployment node, it will be resolved to a DeployedCallGraph at runtime.
    """

    # TODO (jiaodong): Later unify and refactor this with pipeline node class
    pass


@PublicAPI(stability="alpha")
class DeploymentNode(ClassNode):
    """Represents a deployment with its bound config options and arguments.

    The bound deployment can be run using serve.run().

    A bound deployment can be passed as an argument to other bound deployments
    to build a deployment graph. When the graph is deployed, the
    bound deployments passed into a constructor will be converted to
    RayServeHandles that can be used to send requests.

    Calling deployment.method.bind() will return a DeploymentMethodNode
    that can be used to compose an optimized call graph.
    """

    # TODO (jiaodong): Later unify and refactor this with pipeline node class
    def bind(self, *args, **kwargs):
        """Bind the default __call__ method and return a DeploymentMethodNode"""
        return self.__call__.bind(*args, **kwargs)


@PublicAPI(stability="alpha")
class DeploymentFunctionNode(FunctionNode):
    """Represents a serve.deployment decorated function from user.

    It's the counterpart of DeploymentNode that represents function as body
    instead of class.
    """

    pass
