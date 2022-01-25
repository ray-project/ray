import ray
from ray.experimental.dag.dag_node import DAGNode

from typing import Optional


class ActorNode(DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    # TODO(ekl) support actor options
    def __init__(self, actor_cls: type, cls_args, cls_kwargs):
        self._actor_cls = actor_cls
        self._last_call: Optional["ActorMethodNode"] = None
        DAGNode.__init__(self, cls_args, cls_kwargs)

    def _copy(self, new_args, new_kwargs):
        return ActorNode(self._actor_cls, new_args, new_kwargs)

    def _execute(self):
        return ray.remote(self._actor_cls).remote(*self._bound_args,
                                                  **self._bound_kwargs)

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._actor_cls, method_name)
        call_node = _UnboundActorMethodNode(self, method_name)
        return call_node

    def __str__(self):
        return "ActorNode(cls={}, args={}, kwargs={})".format(
            self._actor_cls, self._bound_args, self._bound_kwargs)


class _UnboundActorMethodNode(object):
    def __init__(self, actor: ActorNode, method_name: str):
        self._actor = actor
        self._method_name = method_name

    def _bind(self, *args, **kwargs):
        node = ActorMethodNode(self._actor, self._actor._last_call,
                               self._method_name, args, kwargs)
        self._actor._last_call = node
        return node


class ActorMethodNode(DAGNode):
    """Represents an actor method invocation in a Ray task DAG."""

    # TODO(ekl) support method options
    def __init__(self, actor: ActorNode,
                 prev_call: Optional["ActorMethodNode"], method_name: str,
                 method_args, method_kwargs):
        # The actor creation task dependency is encoded as the first argument,
        # which ensures it is executed prior to this node.
        self._actor: ActorNode = actor
        # Ordering dependency is encoded as the second argument, which ensures
        # it is executed prior to this node.
        self._prev_call: Optional[ActorMethodNode] = prev_call
        self._method_name: str = method_name
        DAGNode.__init__(self, (actor, prev_call) + method_args, method_kwargs)

    def _copy(self, new_args, new_kwargs):
        return ActorMethodNode(new_args[0], new_args[1], self._method_name,
                               new_args[2:], new_kwargs)

    def _execute(self):
        actor_handle = self._bound_args[0]
        return getattr(actor_handle, self._method_name).remote(
            *self._bound_args[2:], **self._bound_kwargs)

    def __str__(self):
        return ("ActorMethodNode(actor={}, prev_call={}, method={}, "
                "args={}, kwargs={})").format(
                    self._actor, self._prev_call, self._method_name,
                    self._bound_args[2:], self._bound_kwargs)
