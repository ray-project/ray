from typing import Any, Dict, List, Union, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import torch

import ray
from ray.dag import (
    DAGNode,
    ClassMethodNode,
)
from ray.dag.constants import COLLECTIVE_OPERATION_KEY
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.torch_tensor_nccl_channel import _init_nccl_group
from ray.experimental.channel.torch_tensor_type import GPUCommunicator, TorchTensorType
from ray.experimental.util.types import _CollectiveOp, ReduceOp
from ray.util.annotations import DeveloperAPI


class _CollectiveOperation:
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
        input_nodes: List[DAGNode],
        op: _CollectiveOp,
        transport: Optional[Union[str, GPUCommunicator]] = None,
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
        if not isinstance(self._op, ReduceOp):
            raise NotImplementedError("Only ReduceOp is implemented")
        if transport is None:
            transport = TorchTensorType.NCCL
        self._type_hint = TorchTensorType(transport=transport, _direct_return=True)
        if isinstance(transport, GPUCommunicator):
            if set(transport.get_actor_handles()) != set(self._actor_handles):
                raise ValueError(
                    "Expected actor handles to match the custom NCCL group"
                )

    def __str__(self) -> str:
        return (
            f"CollectiveGroup("
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

    def init_nccl_group(self, nccl_group_id: Optional[str] = None) -> str:
        """
        Initialize the NCCL group if it has not been initialized yet. If `nccl_group_id`
        is provided, it means the NCCL group has already been initialized.
        """
        type_hint = self._type_hint
        if type_hint.nccl_group_id is not None:
            return type_hint.nccl_group_id
        if nccl_group_id is None:
            nccl_group_id = _init_nccl_group(
                self._actor_handles, type_hint.get_custom_nccl_group()
            )
        type_hint.set_nccl_group_id(nccl_group_id)
        return nccl_group_id

    def get_nccl_group(self) -> GPUCommunicator:
        if self._type_hint.nccl_group_id is not None:
            ctx = ChannelContext.get_current()
            nccl_group = ctx.nccl_groups[self._type_hint.nccl_group_id]
        elif self._type_hint.get_custom_nccl_group() is not None:
            nccl_group = self._type_hint.get_custom_nccl_group()
        else:
            raise ValueError("Expected a NCCL group")
        return nccl_group

    def execute(self, send_buf: "torch.Tensor") -> "torch.Tensor":
        """
        Call the collective operation on the input tensor. An output tensor is
        allocated and returned.
        """
        import torch

        if not isinstance(send_buf, torch.Tensor):
            raise ValueError("Expected a torch tensor")
        nccl_group = self.get_nccl_group()
        recv_buf = torch.empty_like(send_buf)
        nccl_group.allreduce(send_buf, recv_buf, self._op)
        return recv_buf


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

        super().__init__(
            method_name,
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve,
        )

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
    def collective_op(self) -> _CollectiveOperation:
        return self._collective_op
