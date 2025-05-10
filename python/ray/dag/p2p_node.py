from typing import Any, Dict, List, Tuple
from ray.dag import ClassMethodNode, DAGNode
from ray.dag.constants import NCCL_OPERATION_KEY
from ray.dag.nccl_operation import _NcclOperation
from ray.experimental.channel import ChannelInterface
from ray.experimental.util.types import P2POp
from ray.util.annotations import DeveloperAPI


class _P2POperation(_NcclOperation):
    """
    Represent a NCCL P2P operation for scheduling.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args, **kwargs) -> Any:
        """
        Execute the P2P operation.
        """
        raise NotImplementedError(
            "Abstract P2P operations are only used for scheduling "
            "and cannot be executed"
        )


class _P2PSendOperation(_P2POperation):
    """
    Represent an executable NCCL P2P send operation.
    """

    def __init__(self, send_ch: ChannelInterface):
        """
        Args:
            send_ch: A channel that sends data.
        """
        super().__init__()
        self.send_ch = send_ch

    def execute(self, data: Any) -> None:
        """
        Execute the NCCL P2P send operation. Write the data via the NCCL channel.

        Args:
            data: The data to send.
        """
        self.send_ch.write(data)


class _P2PRecvOperation(_P2POperation):
    """
    Represent an executable NCCL P2P recv operation.
    """

    def __init__(self, recv_ch: ChannelInterface):
        """
        Args:
            recv_ch: A channel that receives data.
        """
        super().__init__()
        self.recv_ch = recv_ch

    def execute(self) -> Any:
        """
        Execute the NCCL P2P recv operation. Read the data from the NCCL channel.

        Return:
            Data read from the NCCL channel.
        """
        return self.recv_ch.read()


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
        self._p2p_op: _P2POperation = other_args_to_resolve.get(
            NCCL_OPERATION_KEY, None
        )
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
            raise ValueError("Expected a single input node as ClassMethodNode")
        elif isinstance(method_args[0], _P2PNode):
            raise ValueError("NCCL send node cannot bind to another NCCL P2P node")

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

    @property
    def nccl_op_type(self) -> P2POp:
        return P2POp.SEND


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
            raise ValueError("Expected a single input node as _P2PSendNode")

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

    @property
    def nccl_op_type(self) -> P2POp:
        return P2POp.RECV
