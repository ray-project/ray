from multiprocessing.sharedctypes import Value
import ray
import ray.experimental.dag as ray_dag

from typing import Any, Dict, List, Optional, Tuple


class ClassNode(ray_dag.DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    def __init__(
        self,
        cls: type,
        cls_args,
        cls_kwargs,
        cls_options,
        kwargs_to_resolve=None,
    ):
        if len(cls_args) == 1 and cls_args[0] == ray_dag.DAG_ENTRY_POINT:
            raise ValueError(
                "DAG_ENTRY_POINT cannot be used as ClassNode args. "
                "Please bind to function or class method only."
            )
        self._body = cls
        self._last_call: Optional["ClassMethodNode"] = None
        ray_dag.DAGNode.__init__(
            self,
            cls_args,
            cls_kwargs,
            cls_options,
            kwargs_to_resolve=kwargs_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_kwargs_to_resolve: Dict[str, Any],
    ):
        return ClassNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            kwargs_to_resolve=new_kwargs_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
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
        kwargs_to_resolve = {
            "method_name": self._method_name,
            "parent_class_node": self._actor,
            "prev_class_method_call": self._actor._last_call,
        }

        node = ClassMethodNode(
            args, kwargs, self._options, kwargs_to_resolve=kwargs_to_resolve
        )
        self._actor._last_call = node
        return node

    def options(self, **options):
        self._options = options
        return self


class ClassMethodNode(ray_dag.DAGNode):
    """Represents an actor method invocation in a Ray function DAG."""

    def __init__(
        self,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        kwargs_to_resolve: Dict[str, Any],
    ):
        self._bound_args = [] if method_args is None else method_args
        self._bound_kwargs = {} if method_kwargs is None else method_kwargs
        self._bound_options = {} if method_options is None else method_options
        # Parse kwargs_to_resolve and assign to variables
        self._method_name: str = kwargs_to_resolve.get("method_name")
        self._parent_class_node: ClassNode = kwargs_to_resolve.get("parent_class_node")
        self._prev_class_method_call: Optional[ClassMethodNode] = kwargs_to_resolve.get(
            "prev_class_method_call", None
        )
        # The actor creation task dependency is encoded as the first argument,
        # and the ordering dependency as the second, which ensures they are
        # executed prior to this node.
        ray_dag.DAGNode.__init__(
            self,
            method_args,
            method_kwargs,
            method_options,
            kwargs_to_resolve=kwargs_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_kwargs_to_resolve: Dict[str, Any],
    ):
        return ClassMethodNode(
            new_args,
            new_kwargs,
            new_options,
            kwargs_to_resolve=new_kwargs_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of ClassMethodNode by ray.remote()"""
        method_body = getattr(self._parent_class_node, self._method_name)
        # import ipdb
        # ipdb.set_trace()
        if (
            len(self._bound_args) == 1
            and self._bound_args[0] == ray_dag.DAG_ENTRY_POINT
        ):
            # DAG entrypoint, execute with user input rather than bound args.
            return method_body.options(**self._bound_options).remote(*args, **kwargs)
        else:
            # Execute with bound args.
            return method_body.options(**self._bound_options).remote(
                *self._bound_args,
                **self._bound_kwargs,
            )
