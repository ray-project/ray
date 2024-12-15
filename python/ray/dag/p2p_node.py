from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import torch

from ray.dag import (
    DAGNode,
    ClassMethodNode,
)
from ray.dag.nccl_operation import _NcclOperation
from ray.dag.constants import P2P_OPERATION_KEY
from ray.util.annotations import DeveloperAPI
from ray.experimental.channel import (
    ReaderInterface,
    WriterInterface,
)
from ray.experimental.util.types import P2POp

# [CL] Format. Class comments.


class _P2POperation(_NcclOperation):
    """
    Represent a group of actors in a NCCL P2P operation.
    """

    def __init__(self):
        super().__init__()
        # [CL] The input reader and output writer for the P2P operation.
        self.input_reader: Optional[ReaderInterface] = None
        self.output_writer: Optional[WriterInterface] = None

    def execute(self, op: P2POp, data: Optional["torch.Tensor"] = None) -> Any:
        """
        Execute the NCCL P2P operation.

        [CL]
        If the operation is a NCCL send, write the data to the output writer.
        If the operation is a NCCL recv, read the data from the input reader.

        Args:
            data: The data involved in the P2P operation. If the operation is a
                NCCL send, this is the data to send. If the operation is a NCCL
                recv, this should be None.

        Returns:
            If the operation is a NCCL send, return None. If the operation is a
            NCCL recv, return the received data.
        """
        # [CL]
        if op == P2POp.SEND:
            assert data is not None
            self.output_writer.write(data)
        elif op == P2POp.RECV:
            return self.input_reader.read()


class _P2PNode(ClassMethodNode):
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

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        raise NotImplementedError("Abstract _P2PNode cannot be copied")

    def _execute_impl(self, *args, **kwargs):
        raise NotImplementedError(
            "_P2PNode is only supported with dag.experimental_compile()"
        )

    @property
    def nccl_op(self) -> _P2POperation:
        return self._p2p_op


@DeveloperAPI
class _P2PSendNode(_P2PNode):
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

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], ClassMethodNode)
        ):
            raise ValueError("Expected a single input node that is a ClassMethodNode")
        elif isinstance(method_args[0], _P2PNode):
            raise ValueError("NCCL send node cannot bind to another NCCL P2P node")
        self.requires_nccl_write = True

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _P2PSendNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )


@DeveloperAPI
class _P2PRecvNode(_P2PNode):
    """Represents a NCCL P2P recv operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[_P2PSendNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="p2p_recv",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], _P2PSendNode)
        ):
            raise ValueError("Expected a single input node that is a _P2PSendNode")
        self.requires_nccl_read = True

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _P2PRecvNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )
