from typing import Any, Dict, List, Union, Tuple

from ray.dag import (
    DAGNode,
    ClassMethodNode,
)
from ray.dag.constants import P2P_OPERATION_KEY
from ray.util.annotations import DeveloperAPI


class _P2POperation:
    """
    Represent metadata for a NCCL P2P operation.
    """

    def __init__(self):
        self._send_node: "_NCCLSendNode" = None
        self._recv_nodes: List["_NCCLRecvNode"] = []

    def set_send_node(self, send_node: "_NCCLSendNode"):
        if self._send_node is not None:
            raise ValueError("Expected a single send node")
        self._send_node = send_node

    def add_recv_node(self, recv_node: "_NCCLRecvNode"):
        self._recv_nodes.append(recv_node)

    @property
    def p2p_nodes(self) -> List["_NCCLP2PNode"]:
        return [self._send_node] + self._recv_nodes


class _NCCLP2PNode(ClassMethodNode):
    """Represents a NCCL P2P operation in a Ray DAG."""

    def __init__(
        self,
        method_name: str,
        method_args: Tuple[DAGNode],
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

        # Parse the P2P operation.
        self._p2p_op: _P2POperation = other_args_to_resolve.get(P2P_OPERATION_KEY, None)
        if self._p2p_op is None:
            raise ValueError("Expected a P2P operation")

    @property
    def p2p_op(self) -> _P2POperation:
        return self._p2p_op

    @property
    def synchronous_peers(self) -> List["_NCCLP2PNode"]:
        return self._p2p_op.p2p_nodes

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        raise NotImplementedError("Cannot copy an abstract NCCL P2P node")

    def _execute_impl(self, *args, **kwargs):
        raise NotImplementedError(
            "NCCL P2P nodes are only supported with dag.experimental_compile()"
        )


@DeveloperAPI
class _NCCLSendNode(_NCCLP2PNode):
    """Represents a NCCL P2P send operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[ClassMethodNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="nccl_send",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], ClassMethodNode)
        ):
            raise ValueError("Expected a single input node that is a ClassMethodNode")
        elif isinstance(method_args[0], _NCCLP2PNode):
            raise ValueError("NCCL send node cannot bind to another NCCL P2P node")
        self._input_node = method_args[0]
        self._p2p_op.set_send_node(self)
        self.set_requires_nccl_write(True)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _NCCLSendNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )


@DeveloperAPI
class _NCCLRecvNode(_NCCLP2PNode):
    """Represents a NCCL P2P recv operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[_NCCLSendNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="nccl_recv",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], _NCCLSendNode)
        ):
            raise ValueError("Expected a single input node that is a _NCCLSendNode")
        self._p2p_op.add_recv_node(self)
        self.set_requires_nccl_read(True)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _NCCLRecvNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )
