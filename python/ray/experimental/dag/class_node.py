import ray
from ray.experimental.dag.dag_node import DAGNode

from typing import Any, Dict, List, Optional, Tuple


class ClassNode(DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    def __init__(self, cls: type, cls_args, cls_kwargs, cls_options):
        self._body = cls
        self._last_call: Optional["ClassMethodNode"] = None
        DAGNode.__init__(self, cls_args, cls_kwargs, cls_options)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ):
        return ClassNode(self._body, new_args, new_kwargs, new_options)

    def _execute_impl(self):
        """Executor of ClassNode by ray.remote()"""
        if self._bound_options:
            return (
                ray.remote(self._body)
                .options(**self._bound_options)
                .remote(*self._bound_args, **self._bound_kwargs)
            )
        else:
            return ray.remote(self._body).remote(
                *self._bound_args, **self._bound_kwargs
            )

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._body, method_name)
        call_node = _UnboundClassMethodNode(self, method_name)
        return call_node


class _UnboundClassMethodNode(object):
    def __init__(self, actor: ClassNode, method_name: str):
        self._actor = actor
        self._method_name = method_name
        self._options = {}

    def _bind(self, *args, **kwargs):
        node = ClassMethodNode(
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


class ClassMethodNode(DAGNode):
    """Represents an actor method invocation in a Ray function DAG."""

    def __init__(
        self,
        actor: ClassNode,
        prev_call: Optional["ClassMethodNode"],
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
        return ClassMethodNode(
            new_args[0],
            new_args[1],
            self._method_name,
            new_args[2:],
            new_kwargs,
            new_options,
        )

    def _execute_impl(self):
        """Executor of ClassMethodNode by ray.remote()"""
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
            return getattr(actor_handle, self._method_name).remote(
                *self._bound_args[2:], **self._bound_kwargs
            )
