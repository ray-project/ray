import ray
from ray.experimental.dag.py_obj_scanner import _PyObjScanner

from typing import Optional, Union, List, Tuple, Dict, Any, TypeVar, Callable, Set
import uuid

T = TypeVar("T")


class DAGNode:
    """Abstract class for a node in a Ray task graph.

    A node has a type (e.g., TaskNode), data (e.g., function options and body),
    and arguments (Python values, DAGNodes, and DAGNodes nested within Python
    argument values).
    """

    def __init__(
        self,
        args: Tuple[Any],
        kwargs: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None,
    ):
        # Bound node arguments, ex: func_or_class.bind(1)
        self._bound_args: Tuple[Any] = args
        # Bound node keyword arguments, ex: func_or_class.bind(a=1)
        self._bound_kwargs: Dict[str, Any] = kwargs
        # Bound node options arguments, ex: func_or_class.options(num_cpus=2)
        self._bound_options: Optional[Dict[str, Any]] = options
        # UUID that is not changed over copies of this node.
        self._stable_uuid = uuid.uuid4().hex

    def get_args(self) -> Tuple[Any]:
        """Return the tuple of arguments for this node."""

        return self._bound_args

    def get_kwargs(self) -> Dict[str, Any]:
        """Return the dict of keyword arguments for this node."""

        return self._bound_kwargs.copy()

    def get_options(self) -> Optional[Dict[str, Any]]:
        """Return the dict of options arguments for this node."""
        if not self._bound_options:
            return None

        return self._bound_options.copy()

    def get_toplevel_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes specified as top-level args.

        For example, in `f.remote(a, [b])`, only `a` is a top-level arg.

        This set of nodes are those that are typically resolved prior to
        task execution in Ray. This does not include nodes nested within args.
        For that, use ``get_all_child_nodes()``.
        """

        children = set()
        for a in self.get_args():
            if isinstance(a, DAGNode):
                children.add(a)
        for a in self.get_kwargs().values():
            if isinstance(a, DAGNode):
                children.add(a)
        return children

    def get_all_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes referenced by the args of this node.

        For example, in `f.remote(a, [b])`, this includes both `a` and `b`.
        """

        f = _PyObjScanner(DAGNode)
        children = set()
        for n in f.find_nodes(
            [self._bound_args, self._bound_kwargs, self._bound_options]
        ):
            children.add(n)
        return children

    def replace_all_child_nodes(self, fn: "Callable[[DAGNode], T]") -> "DAGNode":
        """Replace all immediate child nodes using a given function.

        This is a shallow replacement only. To recursively transform nodes in
        the DAG, use ``transform_up()``.

        Args:
            fn: Callable that will be applied once to each child of this node.

        Returns:
            New DAGNode after replacing all child nodes.
        """

        replace_table = {}

        # Find all first-level nested DAGNode children in args.
        f = _PyObjScanner(DAGNode)
        children = f.find_nodes(
            [self._bound_args, self._bound_kwargs, self._bound_options]
        )

        # Update replacement table and execute the replace.
        for node in children:
            if node not in replace_table:
                replace_table[node] = fn(node)
        new_args, new_kwargs, new_options = f.replace_nodes(replace_table)

        # Return updated copy of self.
        return self.copy(new_args, new_kwargs, new_options)

    def transform_up(self, visitor: "Callable[[DAGNode], T]") -> T:
        """Transform each node in this DAG in a bottom-up tree walk.

        Args:
            visitor: Callable that will be applied once to each node in the
                DAG. It will be applied recursively bottom-up, so nodes can
                assume the visitor has been applied to their args already.

        Returns:
            Return type of the visitor after application to the tree.
        """

        class _CachingVisitor:
            def __init__(self, fn):
                self.cache = {}
                self.fn = fn

            def __call__(self, node):
                if node._stable_uuid not in self.cache:
                    self.cache[node._stable_uuid] = self.fn(node)
                return self.cache[node._stable_uuid]

        if not type(visitor).__name__ == "_CachingVisitor":
            visitor = _CachingVisitor(visitor)

        return visitor(
            self.replace_all_child_nodes(lambda node: node.transform_up(visitor))
        )

    def execute(self) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this DAG using the Ray default executor."""
        return self.transform_up(lambda node: node._execute())

    def _execute(self) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this node, assuming args have been transformed already."""
        raise NotImplementedError

    def _copy(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        raise NotImplementedError

    def copy(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        instance = self._copy(new_args, new_kwargs, new_options)
        instance._stable_uuid = self._stable_uuid
        return instance

    def _get_indentation(self):
        return "    "

    def _get_args_lines(self):
        indent = self._get_indentation()
        lines = []
        for arg in self._bound_args:
            if isinstance(arg, DAGNode):
                node_repr_lines = str(arg).split("\n")
                for node_repr_line in node_repr_lines:
                    lines.append(f"{indent}" + node_repr_line)
            elif isinstance(arg, list):
                for ele in arg:
                    node_repr_lines = str(ele).split("\n")
                    for node_repr_line in node_repr_lines:
                        lines.append(f"{indent}" + node_repr_line)
            elif isinstance(arg, dict):
                for key, val in arg.items():
                    node_repr_lines = str(val).split("\n")
                    for node_repr_line in node_repr_lines:
                        lines.append(f"{indent}" + node_repr_line)
            # TODO: (jiaodong) Handle nested containers and other obj types
            else:
                lines.append(str(arg))
        if len(lines) > 0:
            args_line = "["
            for args in lines:
                args_line += f"\n{indent}{args}"
            args_line += f"\n{indent}]"
        else:
            args_line = "[]"

        return args_line

    def _get_kwargs_lines(self):
        indent = self._get_indentation()
        kwargs_lines = []
        for key, val in self._bound_kwargs.items():
            if isinstance(val, DAGNode):
                node_repr_lines = str(val).split("\n")
                for index, node_repr_line in enumerate(node_repr_lines):
                    if index == 0:
                        kwargs_lines.append(
                            f"{indent}{key}:" + f"{indent}" + node_repr_line
                        )
                    else:
                        kwargs_lines.append(f"{indent}{indent}" + node_repr_line)

            elif isinstance(val, list):
                for ele in val:
                    node_repr_lines = str(ele).split("\n")
                    for node_repr_line in node_repr_lines:
                        kwargs_lines.append(f"{indent}" + node_repr_line)
            elif isinstance(val, dict):
                for inner_key, inner_val in val.items():
                    node_repr_lines = str(inner_val).split("\n")
                    for node_repr_line in node_repr_lines:
                        kwargs_lines.append(f"{indent}" + node_repr_line)
            # TODO: (jiaodong) Handle nested containers and other obj types
            else:
                kwargs_lines.append(val)

        if len(kwargs_lines) > 0:
            kwargs_line = "{"
            for line in kwargs_lines:
                kwargs_line += f"\n{indent}{line}"
            kwargs_line += f"\n{indent}}}"
        else:
            kwargs_line = "{}"

        return kwargs_line

    def _get_options_lines(self):
        """Pretty prints .options() in DAGNode. Only prints non-empty values."""
        if not self._bound_options:
            return "{}"
        indent = self._get_indentation()
        options_lines = []
        for key, val in self._bound_options.items():
            if val:
                options_lines.append(f"{indent}{key}: " + str(val))

        options_line = "{"
        for line in options_lines:
            options_line += f"\n{indent}{line}"
        options_line += f"\n{indent}}}"
        return options_line

    def __str__(self):
        indent = self._get_indentation()
        args_line = self._get_args_lines()
        kwargs_line = self._get_kwargs_lines()
        options_line = self._get_options_lines()
        node_type = f"{self.__class__.__name__}"
        # kwargs_children = self._bound_kwargs
        return (
            f"({node_type})(\n"
            f"{indent}body={str(self._body)}\n"
            f"{indent}args={args_line}\n"
            f"{indent}kwargs={kwargs_line}\n"
            f"{indent}options={options_line}\n"
            f")"
        )

    def __reduce__(self):
        """We disallow serialization to prevent inadvertent closure-capture.

        Use ``.to_json()`` and ``.from_json()`` to convert DAGNodes to a
        serializable form.
        """
        raise ValueError("DAGNode cannot be serialized.")
