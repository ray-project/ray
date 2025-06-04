from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, List, Optional, Union

from ray.experimental.channel import ChannelContext, ChannelInterface
from ray.experimental.channel.torch_tensor_type import (
    Communicator,
    TorchTensorType,
)
from ray.experimental.util.types import (
    AllGatherOp,
    AllReduceOp,
    ReduceScatterOp,
    _CollectiveOp,
)

import ray

if TYPE_CHECKING:
    import torch


class _NcclOperation(ABC):
    """
    Represent metadata for a NCCL operation.
    """

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """
        Execute the NCCL operation in `ExecutableTask`.
        """
        raise NotImplementedError


class _P2POperation(_NcclOperation):
    """
    Represent an executable NCCL P2P operation.
    """

    def __init__(self):
        self._nccl_ch: Optional[ChannelInterface] = None

    @property
    def nccl_ch(self) -> Optional[ChannelInterface]:
        return self._nccl_ch

    @nccl_ch.setter
    def nccl_ch(self, nccl_ch: ChannelInterface) -> None:
        self._nccl_ch = nccl_ch


class _P2PSendOperation(_P2POperation):
    """
    Represent an executable NCCL P2P send operation.
    """

    def __init__(self):
        super().__init__()

    def execute(self, data: Any) -> None:
        """
        Execute the NCCL P2P send operation. Write the data via the NCCL channel.

        Args:
            data: The data to send.
        """
        self.nccl_ch.write(data)


class _P2PRecvOperation(_P2POperation):
    """
    Represent an executable NCCL P2P recv operation.
    """

    def __init__(self):
        super().__init__()

    def execute(self) -> Any:
        """
        Execute the NCCL P2P recv operation. Read the data from the NCCL channel.

        Return:
            Data read from the NCCL channel.
        """
        return self.nccl_ch.read()


class _CollectiveOperation(_NcclOperation):
    """
    Represent metadata for a NCCL collective operation.

    Args:
        input_nodes: A list of input nodes to the collective operation.
        op: The collective operation to perform.
        transport: The transport to use for the collective operation.

    Requirements:
    1. Input nodes are unique.
    2. Actor handles are unique.
    3. Actor handles match the custom NCCL group if specified.
    """

    def __init__(
        self,
        input_nodes: List["ray.dag.DAGNode"],
        op: _CollectiveOp,
        transport: Optional[Union[str, Communicator]] = None,
    ):
        if len(input_nodes) == 0:
            raise ValueError("Expected input nodes for a collective operation")
        if len(set(input_nodes)) != len(input_nodes):
            raise ValueError("Expected unique input nodes for a collective operation")

        self._actor_handles: List["ray.actor.ActorHandle"] = []
        for input_node in input_nodes:
            actor_handle = input_node._get_actor_handle()
            if actor_handle is None:
                raise ValueError("Expected an actor handle from the input node")
            self._actor_handles.append(actor_handle)
        if len(set(self._actor_handles)) != len(self._actor_handles):
            invalid_input_nodes = [
                input_node
                for input_node in input_nodes
                if self._actor_handles.count(input_node._get_actor_handle()) > 1
            ]
            raise ValueError(
                "Expected unique actor handles for a collective operation, "
                "but found duplicate actor handles from input nodes: "
                f"{invalid_input_nodes}"
            )

        self._op = op
        if transport is None:
            transport = TorchTensorType.NCCL
        self._type_hint = TorchTensorType(transport=transport, _direct_return=True)
        if isinstance(transport, Communicator):
            if set(transport.get_actor_handles()) != set(self._actor_handles):
                raise ValueError(
                    "Expected actor handles to match the custom NCCL group"
                )

    def __str__(self) -> str:
        return (
            f"CollectiveOperation("
            f"_actor_handles={self._actor_handles}, "
            f"_op={self._op}, "
            f"_type_hint={self._type_hint})"
        )

    @property
    def actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    @property
    def type_hint(self) -> TorchTensorType:
        return self._type_hint

    @property
    def nccl_op_type(self) -> _CollectiveOp:
        return self._op

    def get_communicator(self) -> Communicator:
        if self._type_hint.communicator_id is not None:
            ctx = ChannelContext.get_current()
            communicator = ctx.communicators[self._type_hint.communicator_id]
        elif self._type_hint.get_custom_communicator() is not None:
            communicator = self._type_hint.get_custom_communicator()
        else:
            raise ValueError("Expected a NCCL group")
        return communicator

    def execute(self, send_buf: "torch.Tensor") -> "torch.Tensor":
        """
        Call the collective operation on the input tensor. An output tensor is
        allocated and returned.
        """
        import torch

        if not isinstance(send_buf, torch.Tensor):
            raise ValueError("Expected a torch tensor")
        communicator = self.get_communicator()

        if isinstance(self._op, AllGatherOp):
            world_size = len(self._actor_handles)
            recv_buf = torch.empty(
                (send_buf.shape[0] * world_size, *send_buf.shape[1:]),
                dtype=send_buf.dtype,
                device=send_buf.device,
            )
            communicator.allgather(send_buf, recv_buf)
        elif isinstance(self._op, AllReduceOp):
            recv_buf = torch.empty_like(send_buf)
            communicator.allreduce(send_buf, recv_buf, self._op.reduceOp)
        elif isinstance(self._op, ReduceScatterOp):
            world_size = len(self._actor_handles)
            if send_buf.shape[0] % world_size != 0:
                raise ValueError(
                    "Expected the first dimension of the input tensor to be divisible "
                    f"by the world size {world_size}"
                )
            recv_buf = torch.empty(
                (send_buf.shape[0] // world_size, *send_buf.shape[1:]),
                dtype=send_buf.dtype,
                device=send_buf.device,
            )
            communicator.reducescatter(send_buf, recv_buf, self._op.reduceOp)
        else:
            raise ValueError("Expected a collective operation")
        return recv_buf
