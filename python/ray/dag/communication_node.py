from typing import Any, Dict, List, Tuple

from ray.dag import ClassMethodNode, DAGNode
from ray.dag.communication_operation import (
    _CollectiveOperation,
    _P2PRecvOperation,
    _P2PSendOperation,
)
from ray.dag.constants import COLLECTIVE_OPERATION_KEY
from ray.experimental.util.types import (
    _CollectiveOp,
    _P2POp,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class P2PSendNode(ClassMethodNode):
    """Represents a NCCL P2P send operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[ClassMethodNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="p2p_send",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )
        self._send_op = _P2PSendOperation()

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], ClassMethodNode)
        ):
            raise ValueError("Expected a single input node as ClassMethodNode")
        if isinstance(method_args[0], P2PSendNode) or isinstance(
            method_args[0], P2PRecvNode
        ):
            raise ValueError("P2P send node cannot bind to another P2P node")

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return P2PSendNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    @property
    def nccl_op_type(self) -> _P2POp:
        return _P2POp.SEND

    @property
    def nccl_op(self) -> _P2PSendOperation:
        return self._send_op


@DeveloperAPI
class P2PRecvNode(ClassMethodNode):
    """Represents a NCCL P2P recv operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[P2PSendNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="p2p_recv",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )
        self._recv_op = _P2PRecvOperation()

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], P2PSendNode)
        ):
            raise ValueError("Expected a single input node as P2PSendNode")

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return P2PRecvNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    @property
    def nccl_op_type(self) -> _P2POp:
        return _P2POp.RECV

    @property
    def nccl_op(self) -> _P2PRecvOperation:
        return self._recv_op


@DeveloperAPI
class CollectiveOutputNode(ClassMethodNode):
    """Represent an output node from a NCCL collective operation in a Ray DAG."""

    def __init__(
        self,
        method_name: str,
        method_args: Tuple[
            DAGNode,
        ],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name,
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve,
        )

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], DAGNode)
        ):
            raise ValueError("Expected a single input node")
        self._input_node = method_args[0]
        # Parse the collective operation.
        self._collective_op: _CollectiveOperation = other_args_to_resolve.get(
            COLLECTIVE_OPERATION_KEY, None
        )
        if self._collective_op is None:
            raise ValueError("Expected a collective operation")

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return CollectiveOutputNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        raise NotImplementedError(
            "CollectiveOutputNode is only supported with dag.experimental_compile()"
        )

    @property
    def nccl_op_type(self) -> _CollectiveOp:
        return self._collective_op.nccl_op_type

    @property
    def nccl_op(self) -> _CollectiveOperation:
        return self._collective_op
