import ray
from ray.experimental.dag.py_obj_scanner import _PyObjScanner
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY

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
import json

T = TypeVar("T")


class DAGNode:
    """Abstract class for a node in a Ray task graph.

    A node has a type (e.g., FunctionNode), data (e.g., function options and
    body), arguments (Python values, DAGNodes, and DAGNodes nested within Python
    argument values) and options (Ray API .options() used for function, class
    or class method)
    """

    def __init__(
        self,
        args: Tuple[Any],
        kwargs: Dict[str, Any],
        options: Dict[str, Any],
        other_args_to_resolve: Dict[str, Any],
    ):
        """
        args:
            args (Tuple[Any]): Bound node arguments.
                ex: func_or_class.bind(1)
            kwargs (Dict[str, Any]): Bound node keyword arguments.
                ex: func_or_class.bind(a=1)
            options (Dict[str, Any]): Bound node options arguments.
                ex: func_or_class.options(num_cpus=2)
            other_args_to_resolve (Dict[str, Any]): Bound kwargs to resolve
                that's specific to subclass implementation without exposing
                as args in base class, example: ClassMethodNode
        """
        self._bound_args: Tuple[Any] = args or []
        self._bound_kwargs: Dict[str, Any] = kwargs or {}
        self._bound_options: Dict[str, Any] = options or {}
        self._bound_other_args_to_resolve: Optional[Dict[str, Any]] = (
            other_args_to_resolve or {}
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

    def get_other_args_to_resolve(self) -> Dict[str, Any]:
        """Return the dict of other args to resolve arguments for this node."""
        return self._bound_other_args_to_resolve.copy()

    def get_stable_uuid(self) -> str:
        """Return stable uuid for this node.
        1) Generated only once at first instance creation
        2) Stable across pickling, replacement and JSON serialization.
        """
        return self._stable_uuid

    def execute(self, *args, **kwargs) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this DAG using the Ray default executor."""
        return self.apply_recursive(lambda node: node._execute_impl(*args, **kwargs))

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
        for a in self.get_other_args_to_resolve().values():
            if isinstance(a, DAGNode):
                children.add(a)
        return children

    def _get_all_child_nodes(self) -> Set["DAGNode"]:
        """Return the set of nodes referenced by the args, kwargs, and
        args_to_resolve in current node, even they're deeply nested.

        Examples:
            f.remote(a, [b]) -> set(a, b)
            f.remote(a, [b], key={"nested": [c]}) -> set(a, b, c)
        """

        scanner = _PyObjScanner()
        children = set()
        for n in scanner.find_nodes(
            [
                self._bound_args,
                self._bound_kwargs,
                self._bound_other_args_to_resolve,
            ]
        ):
            children.add(n)
        return children

    def _apply_and_replace_all_child_nodes(
        self, fn: "Callable[[DAGNode], T]"
    ) -> "DAGNode":
        """Apply and replace all immediate child nodes using a given function.

        This is a shallow replacement only. To recursively transform nodes in
        the DAG, use ``apply_recursive()``.

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
                self._bound_other_args_to_resolve,
            ]
        ):
            if node not in replace_table:
                replace_table[node] = fn(node)
        new_args, new_kwargs, new_other_args_to_resolve = scanner.replace_nodes(
            replace_table
        )

        # Return updated copy of self.
        return self._copy(
            new_args, new_kwargs, self.get_options(), new_other_args_to_resolve
        )

    def apply_recursive(self, fn: "Callable[[DAGNode], T]") -> T:
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
                self.input_node_uuid = None

            def __call__(self, node):
                if node._stable_uuid not in self.cache:
                    self.cache[node._stable_uuid] = self.fn(node)
                if type(node).__name__ == "InputNode":
                    if not self.input_node_uuid:
                        self.input_node_uuid = node._stable_uuid
                    elif self.input_node_uuid != node._stable_uuid:
                        raise AssertionError(
                            "Each DAG should only have one unique InputNode."
                        )
                return self.cache[node._stable_uuid]

        if not type(fn).__name__ == "_CachingFn":
            fn = _CachingFn(fn)

        return fn(
            self._apply_and_replace_all_child_nodes(
                lambda node: node.apply_recursive(fn)
            )
        )

    def apply_functional(
        self,
        source_input_list: Any,
        predictate_fn: Callable,
        apply_fn: Callable,
    ):
        """
        Apply a given function to DAGNodes in source_input_list, and return
        the replaced inputs without mutating or coping any DAGNode.

        Args:
            source_input_list: Source inputs to extract and apply function on
                all children DAGNode instances.
            predictate_fn: Applied on each DAGNode instance found and determine
                if we should apply function to it. Can be used to filter node
                types.
            apply_fn: Function to appy on the node on bound attributes. Example:
                apply_fn = lambda node: node._get_serve_deployment_handle(
                    node._deployment, node._bound_other_args_to_resolve
                )

        Returns:
            replaced_inputs: Outputs of apply_fn on DAGNodes in
                source_input_list that passes predictate_fn.
        """
        replace_table = {}
        scanner = _PyObjScanner()
        for node in scanner.find_nodes(source_input_list):
            if predictate_fn(node) and node not in replace_table:
                replace_table[node] = apply_fn(node)

        replaced_inputs = scanner.replace_nodes(replace_table)

        return replaced_inputs

    def _execute_impl(self) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this node, assuming args have been transformed already."""
        raise NotImplementedError

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        raise NotImplementedError

    def _copy(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        """Return a copy of this node with the given new args."""
        instance = self._copy_impl(
            new_args, new_kwargs, new_options, new_other_args_to_resolve
        )
        instance._stable_uuid = self._stable_uuid
        return instance

    def __reduce__(self):
        """We disallow serialization to prevent inadvertent closure-capture.

        Use ``.to_json()`` and ``.from_json()`` to convert DAGNodes to a
        serializable form.
        """
        raise ValueError(f"DAGNode cannot be serialized. DAGNode: {str(self)}")

    def __getattr__(self, attr: str):
        if attr == "bind":
            raise AttributeError(
                f".bind() cannot be used again on {type(self)} "
                f"(args: {self.get_args()}, kwargs: {self.get_kwargs()})."
            )
        elif attr == "remote":
            raise AttributeError(
                f".remote() cannot be used on {type(self)}. To execute the task "
                "graph for this node, use .execute()."
            )
        else:
            return self.__getattribute__(attr)

    def to_json_base(
        self, encoder_cls: json.JSONEncoder, dag_node_type: str
    ) -> Dict[str, Any]:
        """
        Base JSON serializer for DAGNode types with base info. Each DAGNode
        subclass needs to update with its own fields.

        JSON serialization is not hard requirement for functionalities of a
        DAG authored at Ray level, therefore implementations here meant to
        facilitate JSON encoder implemenation in other libraries such as Ray
        Serve.

        Args:
            encoder_cls (json.JSONEncoder): JSON encoder class used to handle
                DAGNode nested in any args or options, created and passed from
                caller, and is expected to be the same across all DAGNode types.

        Returns:
            json_dict (Dict[str, Any]): JSON serialized DAGNode.
        """
        return {
            DAGNODE_TYPE_KEY: dag_node_type,
            # Will be overriden by build()
            "import_path": "",
            "args": json.dumps(self.get_args(), cls=encoder_cls),
            "kwargs": json.dumps(self.get_kwargs(), cls=encoder_cls),
            # .options() should not contain any DAGNode type
            "options": json.dumps(self.get_options()),
            "other_args_to_resolve": json.dumps(
                self.get_other_args_to_resolve(), cls=encoder_cls
            ),
            "uuid": self.get_stable_uuid(),
        }

    @staticmethod
    def from_json_base(input_json, object_hook=None):
        # Post-order JSON deserialization
        args = json.loads(input_json["args"], object_hook=object_hook)
        kwargs = json.loads(input_json["kwargs"], object_hook=object_hook)
        # .options() should not contain any DAGNode type
        options = json.loads(input_json["options"])
        other_args_to_resolve = json.loads(
            input_json["other_args_to_resolve"], object_hook=object_hook
        )
        uuid = input_json["uuid"]

        return {
            "args": args,
            "kwargs": kwargs,
            "options": options,
            "other_args_to_resolve": other_args_to_resolve,
            "uuid": uuid,
        }
