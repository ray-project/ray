import ray
from ray.experimental.dag.dag_node import DAGNode
import ray.experimental.dag as dag

from typing import Any, Dict, List, Optional, Tuple


class ActorNode(DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    def __init__(self, actor_cls: type, cls_args, cls_kwargs, cls_options):
        self._actor_cls = actor_cls
        self._last_call: Optional["ActorMethodNode"] = None
        DAGNode.__init__(self, cls_args, cls_kwargs, cls_options)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ):
        return ActorNode(self._actor_cls, new_args, new_kwargs, new_options)

    def _execute_impl(self, *args, **kwargs):
        if self._bound_options:
            return (
                ray.remote(self._actor_cls)
                .options(**self._bound_options)
                .remote(*self._bound_args, **self._bound_kwargs)
            )
        else:
            return ray.remote(self._actor_cls).remote(
                *self._bound_args, **self._bound_kwargs
            )

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._actor_cls, method_name)
        call_node = _UnboundActorMethodNode(self, method_name)
        return call_node


class _UnboundActorMethodNode(object):
    def __init__(self, actor: ActorNode, method_name: str):
        self._actor = actor
        self._method_name = method_name
        self._options = {}

    def _bind(self, *args, **kwargs):
        node = ActorMethodNode(
            self._actor,
            self._actor._last_call,
            self._method_name,
            args,
            kwargs,
            self._options,
        )
        self._actor._last_call = node
        return node

    def options(self, **options):
        self._options = options
        return self


class ActorMethodNode(DAGNode):
    """Represents an actor method invocation in a Ray task DAG."""

    def __init__(
        self,
        actor: ActorNode,
        prev_call: Optional["ActorMethodNode"],
        method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
    ):
        self._method_name: str = method_name
        # The actor creation task dependency is encoded as the first argument,
        # and the ordering dependency as the second, which ensures they are
        # executed prior to this node.
        DAGNode.__init__(
            self,
            (actor, prev_call) + method_args,
            method_kwargs,
            method_options,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ):
        return ActorMethodNode(
            new_args[0],
            new_args[1],
            self._method_name,
            new_args[2:],
            new_kwargs,
            new_options,
        )

    def _execute_impl(self, *args, **kwargs):
        actor_handle = self._bound_args[0]
        if self._bound_options:
            return (
                getattr(actor_handle, self._method_name)
                .options(**self._bound_options)
                .remote(
                    *self._bound_args[2:],
                    **self._bound_kwargs,
                )
            )
        else:
            if len(self._bound_args[2:]) == 1 and self._bound_args[2:][0] == dag.ENTRY_POINT:
                print("YOOOOOO !!! Use execute args !!")
                return getattr(actor_handle, self._method_name).remote(
                    *args, **kwargs
                )
            else:
                return getattr(actor_handle, self._method_name).remote(
                    *self._bound_args[2:], **self._bound_kwargs
                )
