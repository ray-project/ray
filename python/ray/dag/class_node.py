from weakref import ReferenceType

import ray
from ray.dag.dag_node import DAGNode
from ray.dag.input_node import InputNode
from ray.dag.format_utils import get_dag_node_str
from ray.dag.constants import (
    PARENT_CLASS_NODE_KEY,
    PREV_CLASS_METHOD_CALL_KEY,
    BIND_INDEX_KEY,
    IS_CLASS_METHOD_OUTPUT_KEY,
)
from ray.util.annotations import DeveloperAPI

from typing import Any, Dict, List, Union, Tuple, Optional


@DeveloperAPI
class ClassNode(DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    def __init__(
        self,
        cls,
        cls_args,
        cls_kwargs,
        cls_options,
        other_args_to_resolve=None,
    ):
        self._body = cls
        self._last_call: Optional["ClassMethodNode"] = None
        super().__init__(
            cls_args,
            cls_kwargs,
            cls_options,
            other_args_to_resolve=other_args_to_resolve,
        )

        if self._contains_input_node():
            raise ValueError(
                "InputNode handles user dynamic input the DAG, and "
                "cannot be used as args, kwargs, or other_args_to_resolve "
                "in ClassNode constructor because it is not available at "
                "class construction or binding time."
            )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return ClassNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of ClassNode by ray.remote()

        Args and kwargs are to match base class signature, but not in the
        implementation. All args and kwargs should be resolved and replaced
        with value in bound_args and bound_kwargs via bottom-up recursion when
        current node is executed.
        """
        return (
            ray.remote(self._body)
            .options(**self._bound_options)
            .remote(*self._bound_args, **self._bound_kwargs)
        )

    def _contains_input_node(self) -> bool:
        """Check if InputNode is used in children DAGNodes with current node
        as the root.
        """
        children_dag_nodes = self._get_all_child_nodes()
        for child in children_dag_nodes:
            if isinstance(child, InputNode):
                return True
        return False

    def __getattr__(self, method_name: str):
        # User trying to call .bind() without a bind class method
        if method_name == "bind" and "bind" not in dir(self._body):
            raise AttributeError(f".bind() cannot be used again on {type(self)} ")
        # Raise an error if the method is invalid.
        getattr(self._body, method_name)
        call_node = _UnboundClassMethodNode(self, method_name, {})
        return call_node

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))


class _UnboundClassMethodNode(object):
    def __init__(self, actor: ClassNode, method_name: str, options: dict):
        # TODO(sang): Theoretically, We should use weakref cuz it is
        # a circular dependency but when I used weakref, it fails
        # because we cannot serialize the weakref.
        self._actor = actor
        self._method_name = method_name
        self._options = options

    def bind(self, *args, **kwargs):
        other_args_to_resolve = {
            PARENT_CLASS_NODE_KEY: self._actor,
            PREV_CLASS_METHOD_CALL_KEY: self._actor._last_call,
        }

        node = ClassMethodNode(
            self._method_name,
            args,
            kwargs,
            self._options,
            other_args_to_resolve=other_args_to_resolve,
        )
        self._actor._last_call = node
        return node

    def __getattr__(self, attr: str):
        if attr == "remote":
            raise AttributeError(
                ".remote() cannot be used on ClassMethodNodes. Use .bind() instead "
                "to express an symbolic actor call."
            )
        else:
            return self.__getattribute__(attr)

    def options(self, **options):
        self._options = options
        return self


class _ClassMethodOutput:
    """Represents a class method output in a Ray function DAG."""

    def __init__(self, class_method_call: "ClassMethodNode", output_idx: int):
        # The upstream class method call that returns multiple values.
        self._class_method_call = class_method_call
        # The output index of the return value from the upstream class method call.
        self._output_idx = output_idx

    @property
    def class_method_call(self) -> "ClassMethodNode":
        return self._class_method_call

    @property
    def output_idx(self) -> int:
        return self._output_idx


@DeveloperAPI
class ClassMethodNode(DAGNode):
    """Represents an actor method invocation in a Ray function DAG."""

    def __init__(
        self,
        method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        other_args_to_resolve: Dict[str, Any],
    ):
        self._bound_args = method_args or []
        self._bound_kwargs = method_kwargs or {}
        self._bound_options = method_options or {}
        self._method_name: str = method_name
        # Parse other_args_to_resolve and assign to variables
        self._parent_class_node: Union[
            ClassNode, ReferenceType["ray._private.actor.ActorHandle"]
        ] = other_args_to_resolve.get(PARENT_CLASS_NODE_KEY)
        # Used to track lineage of ClassMethodCall to preserve deterministic
        # submission and execution order.
        self._prev_class_method_call: Optional[
            ClassMethodNode
        ] = other_args_to_resolve.get(PREV_CLASS_METHOD_CALL_KEY, None)
        # The index/order when bind() is called on this class method
        self._bind_index: Optional[int] = other_args_to_resolve.get(
            BIND_INDEX_KEY, None
        )
        # Represent if the ClassMethodNode is a class method output. If True,
        # the node is a placeholder for a return value from the ClassMethodNode
        # that returns multiple values. If False, the node is a class method call.
        self._is_class_method_output: bool = other_args_to_resolve.get(
            IS_CLASS_METHOD_OUTPUT_KEY, False
        )
        # Represents the return value from the upstream ClassMethodNode that
        # returns multiple values. If the node is a class method call, this is None.
        self._class_method_output: Optional[_ClassMethodOutput] = None
        if self._is_class_method_output:
            # Set the upstream ClassMethodNode and the output index of the return
            # value from `method_args`.
            self._class_method_output = _ClassMethodOutput(
                method_args[0], method_args[1]
            )

        # The actor creation task dependency is encoded as the first argument,
        # and the ordering dependency as the second, which ensures they are
        # executed prior to this node.
        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve=other_args_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return ClassMethodNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of ClassMethodNode by ray.remote()

        Args and kwargs are to match base class signature, but not in the
        implementation. All args and kwargs should be resolved and replaced
        with value in bound_args and bound_kwargs via bottom-up recursion when
        current node is executed.
        """
        if self.is_class_method_call:
            method_body = getattr(self._parent_class_node, self._method_name)
            # Execute with bound args.
            return method_body.options(**self._bound_options).remote(
                *self._bound_args,
                **self._bound_kwargs,
            )
        else:
            assert self._class_method_output is not None
            return self._bound_args[0][self._class_method_output.output_idx]

    def __str__(self) -> str:
        return get_dag_node_str(self, f"{self._method_name}()")

    def __repr__(self) -> str:
        return self.__str__()

    def get_method_name(self) -> str:
        return self._method_name

    def _get_bind_index(self) -> int:
        return self._bind_index

    def _get_remote_method(self, method_name):
        method_body = getattr(self._parent_class_node, method_name)
        return method_body

    def _get_actor_handle(self) -> Optional["ray.actor.ActorHandle"]:
        if not isinstance(self._parent_class_node, ray.actor.ActorHandle):
            return None
        return self._parent_class_node

    @property
    def num_returns(self) -> int:
        """
        Return the number of return values from the class method call. If the
        node is a class method output, return the number of return values from
        the upstream class method call.
        """

        if self.is_class_method_call:
            num_returns = self._bound_options.get("num_returns", None)
            if num_returns is None:
                method = self._get_remote_method(self._method_name)
                num_returns = method.__getstate__()["num_returns"]
            return num_returns
        else:
            assert self._class_method_output is not None
            return self._class_method_output.class_method_call.num_returns

    @property
    def is_class_method_call(self) -> bool:
        """
        Return True if the node is a class method call, False if the node is a
        class method output.
        """
        return not self._is_class_method_output

    @property
    def is_class_method_output(self) -> bool:
        """
        Return True if the node is a class method output, False if the node is a
        class method call.
        """
        return self._is_class_method_output

    @property
    def class_method_call(self) -> Optional["ClassMethodNode"]:
        """
        Return the upstream class method call that returns multiple values. If
        the node is a class method output, return None.
        """

        if self._class_method_output is None:
            return None
        return self._class_method_output.class_method_call

    @property
    def output_idx(self) -> Optional[int]:
        """
        Return the output index of the return value from the upstream class
        method call that returns multiple values. If the node is a class method
        call, return None.
        """

        if self._class_method_output is None:
            return None
        return self._class_method_output.output_idx
