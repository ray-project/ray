import ray
from ray.experimental.dag.py_obj_scanner import _PyObjScanner
from ray.experimental.dag.format_utils import (
    get_args_lines,
    get_kwargs_lines,
    get_options_lines,
    get_kwargs_to_resolve_lines,
    get_indentation,
)
import ray.experimental.dag as ray_dag

from typing import (
    Optional,
    Union,
    List,
    Tuple,
    Dict,
    Any,
    TypeVar,
    Callable,
    Set,
)
import uuid

T = TypeVar("T")


class DAGNode:
    """Abstract class for a node in a Ray task graph.

    A node has a type (e.g., FunctionNode), data (e.g., function options and body),
    arguments (Python values, DAGNodes, and DAGNodes nested within Python
    argument values) and options (Ray API .options() used for function, class
    or class method)
    """

    def __init__(
        self,
        args: Tuple[Any],
        kwargs: Dict[str, Any],
        options: Dict[str, Any],
        kwargs_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        # Bound node arguments, ex: func_or_class.bind(1)
        self._bound_args: Tuple[Any] = args or []
        # Bound node keyword arguments, ex: func_or_class.bind(a=1)
        self._bound_kwargs: Dict[str, Any] = kwargs or {}
        # Bound node options arguments, ex: func_or_class.options(num_cpus=2)
        self._bound_options: Dict[str, Any] = options or {}
        # Bound kwargs to resolve that's specific to subclass implementation
        self._bound_kwargs_to_resolve: Optional[Dict[str, Any]] = (
            kwargs_to_resolve or {}
        )
        # UUID that is not changed over copies of this node.
        self._stable_uuid = uuid.uuid4().hex

    def get_args(self) -> Tuple[Any]:
        """Return the tuple of arguments for this node."""

        return self._bound_args

    def get_kwargs(self) -> Dict[str, Any]:
        """Return the dict of keyword arguments for this node."""

        return self._bound_kwargs.copy()

    def get_options(self) -> Dict[str, Any]:
        """Return the dict of options arguments for this node."""

        return self._bound_options.copy()

    def get_kwargs_to_resolve(self) -> Dict[str, Any]:

        return self._bound_kwargs_to_resolve.copy()

    def execute(self, *args, **kwargs) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this DAG using the Ray default executor."""
        return self._apply_recursive(lambda node: node._execute_impl(*args, **kwargs))

    def _get_toplevel_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes specified as top-level args.

        For example, in `f.remote(a, [b])`, only `a` is a top-level arg.

        This set of nodes are those that are typically resolved prior to
        task execution in Ray. This does not include nodes nested within args.
        For that, use ``_get_all_child_nodes()``.
        """

        children = set()
        for a in self.get_args():
            if isinstance(a, DAGNode):
                children.add(a)
        for a in self.get_kwargs().values():
            if isinstance(a, DAGNode):
                children.add(a)
        return children

    def _get_all_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes referenced by the args of this node.

        For example, in `f.remote(a, [b])`, this includes both `a` and `b`.

        Args:
            f (_PyObjScanner): A CloudPickler scanner object that finds and
                keeps track of DAGNode objects in current shallow layer for
                replacement later on.
        """

        scanner = _PyObjScanner()
        children = set()
        for n in scanner.find_nodes(
            [
                self._bound_args,
                self._bound_kwargs,
                self._bound_kwargs_to_resolve,
            ]
        ):
            children.add(n)
        return children

    def _apply_and_replace_all_child_nodes(
        self, fn: "Callable[[DAGNode], T]"
    ) -> "DAGNode":
        """Apply and replace all immediate child nodes using a given function.

        This is a shallow replacement only. To recursively transform nodes in
        the DAG, use ``_apply_recursive()``.

        Args:
            fn: Callable that will be applied once to each child of this node.

        Returns:
            New DAGNode after replacing all child nodes.
        """

        replace_table = {}
        # CloudPickler scanner object for current layer of DAGNode. Same
        # scanner should be use for a full find & replace cycle.
        scanner = _PyObjScanner()
        # Find all first-level nested DAGNode children in args.
        # Update replacement table and execute the replace.
        for node in scanner.find_nodes(
            [
                self._bound_args,
                self._bound_kwargs,
                self._bound_kwargs_to_resolve,
            ]
        ):
            if node not in replace_table:
                replace_table[node] = fn(node)
        new_args, new_kwargs, new_kwargs_to_resolve = scanner.replace_nodes(
            replace_table
        )

        # Return updated copy of self.
        return self._copy(
            new_args, new_kwargs, self.get_options(), new_kwargs_to_resolve
        )

    def _apply_recursive(self, fn: "Callable[[DAGNode], T]") -> T:
        """Apply callable on each node in this DAG in a bottom-up tree walk.

        Args:
            fn: Callable that will be applied once to each node in the
                DAG. It will be applied recursively bottom-up, so nodes can
                assume the fn has been applied to their args already.

        Returns:
            Return type of the fn after application to the tree.
        """

        class _CachingFn:
            def __init__(self, fn):
                self.cache = {}
                self.fn = fn

            def __call__(self, node):
                if node._stable_uuid not in self.cache:
                    self.cache[node._stable_uuid] = self.fn(node)
                return self.cache[node._stable_uuid]

        if not type(fn).__name__ == "_CachingFn":
            fn = _CachingFn(fn)

        return fn(
            self._apply_and_replace_all_child_nodes(
                lambda node: node._apply_recursive(fn)
            )
        )

    def _execute_impl(self) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this node, assuming args have been transformed already."""
        raise NotImplementedError

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_kwargs_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        raise NotImplementedError

    def _copy(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_kwargs_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        instance = self._copy_impl(
            new_args, new_kwargs, new_options, new_kwargs_to_resolve
        )
        instance._stable_uuid = self._stable_uuid
        return instance

    def __str__(self) -> str:
        indent = get_indentation()

        if isinstance(self, (ray_dag.FunctionNode, ray_dag.ClassNode)):
            body_line = str(self._body)
        elif isinstance(self, ray_dag.ClassMethodNode):
            body_line = f"{self._method_name}()"

        args_line = get_args_lines(self._bound_args)
        kwargs_line = get_kwargs_lines(self._bound_kwargs)
        options_line = get_options_lines(self._bound_options)
        kwargs_to_resolve_line = get_kwargs_to_resolve_lines(
            self._bound_kwargs_to_resolve
        )
        node_type = f"{self.__class__.__name__}"

        return (
            f"({node_type})(\n"
            f"{indent}body={body_line}\n"
            f"{indent}args={args_line}\n"
            f"{indent}kwargs={kwargs_line}\n"
            f"{indent}options={options_line}\n"
            f"{indent}kwargs_to_resolve={kwargs_to_resolve_line}\n"
            f")"
        )

    def __reduce__(self):
        """We disallow serialization to prevent inadvertent closure-capture.

        Use ``.to_json()`` and ``.from_json()`` to convert DAGNodes to a
        serializable form.
        """
        raise ValueError("DAGNode cannot be serialized.")
