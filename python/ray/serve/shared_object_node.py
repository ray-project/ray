from typing import Any, Callable, Dict, List

import ray
from ray import cloudpickle
from ray.dag import FunctionNode


class SharedObjectNode:
    """TODO"""
    def __init__(self, *, uuid: str, fn: Callable, options: Dict[str, Any],
                 args: List[Any], kwargs: Dict[str, Any]):
        self._uuid: str = uuid
        self._serialized_fn = cloudpickle.dumps(fn)
        self._options = options
        self._serialized_args = cloudpickle.dumps(args)
        self._serialized_kwargs = cloudpickle.dumps(kwargs)

    def execute(self) -> ray.ObjectRef:
        @ray.remote
        def wrapper():
            return cloudpickle.loads(self._serialized_fn)(
                *cloudpickle.loads(self._serialized_args),
                **cloudpickle.loads(self._serialized_kwargs),
            )

        return wrapper.options(**self._options).remote()

    @classmethod
    def from_function_node(cls, node: FunctionNode):
        return cls(
            uuid=node.get_stable_uuid(),
            fn=node._body,
            args=node.get_args(),
            kwargs=node.get_kwargs(),
            options=node.get_options(),
        )
