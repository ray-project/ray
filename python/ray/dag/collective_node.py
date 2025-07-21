from typing import Any, Dict, List, Union, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import torch

import ray
from ray.dag import (
    DAGNode,
    ClassMethodNode,
)
from ray.dag.constants import COLLECTIVE_OPERATION_KEY, IS_CLASS_METHOD_OUTPUT_KEY
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.torch_tensor_type import Communicator, TorchTensorType
from ray.experimental.util.types import (
    _CollectiveOp,
    AllGatherOp,
    AllReduceOp,
    ReduceScatterOp,
)
from ray.util.annotations import DeveloperAPI


class _CollectiveOperation:
    """
    Represent metadata for a collective communicator collective operation.

    Args:
        inputs: A list of lists of DAGNode. Each nested list inside
            of inputs should contain exactly one object per actor.
            If multiple nested lists are provided, then the order of
            actors should be the same for each nested list.
        op: The collective operation to perform.
        transport: The transport to use for the collective operation.

    Requirements:
    1. Input nodes are unique.
    2. Actor handles are unique.
    3. Actor handles match the custom communicator group if specified.
    """

    def __init__(
        self,
        inputs: List[List[DAGNode]],
        op: _CollectiveOp,
        transport: Optional[Union[str, Communicator]] = None,
    ):
        self._actor_handles: List["ray.actor.ActorHandle"] = []
        for i, input_nodes in enumerate(inputs):
            # Check non-empty input list
            if len(input_nodes) == 0:
                nested_list_error_msg = f" at index {i}" if len(inputs) > 1 else ""
                raise ValueError(
                    f"Expected non-empty input list{nested_list_error_msg}."
                )

            # Check input nodes are DAGNode
            if not all(isinstance(node, DAGNode) for node in input_nodes):
                nested_list_error_msg = (
                    f" at list at index {i}" if len(inputs) > 1 else ""
                )
                raise ValueError(
                    f"Expected all input nodes to be DAGNode{nested_list_error_msg}, "
                    f"but got {input_nodes}."
                )

            # Check unique input nodes
            if len(set(input_nodes)) != len(input_nodes):
                duplicates = [
                    input_node
                    for input_node in input_nodes
                    if input_nodes.count(input_node) > 1
                ]
                nested_list_error_msg = (
                    f" at list at index {i}" if len(inputs) > 1 else ""
                )
                raise ValueError(
                    f"Expected unique input nodes{nested_list_error_msg}, but found duplicates: "
                    f"{duplicates}"
                )

            current_actor_handles = []
            for input_node in input_nodes:
                actor_handle = input_node._get_actor_handle()
                if actor_handle is None:
                    nested_list_error_msg = (
                        f" at list at index {i}" if len(inputs) > 1 else ""
                    )
                    raise ValueError(
                        f"Expected an actor handle from the input node{nested_list_error_msg}"
                    )
                current_actor_handles.append(actor_handle)

            # Check unique actor handles
            if len(set(current_actor_handles)) != len(current_actor_handles):
                invalid_input_nodes = [
                    input_node
                    for input_node in input_nodes
                    if current_actor_handles.count(input_node._get_actor_handle()) > 1
                ]
                nested_list_error_msg = (
                    f" at list at index {i}" if len(inputs) > 1 else ""
                )
                raise ValueError(
                    f"Expected unique actor handles{nested_list_error_msg}, "
                    "but found duplicate actor handles from input nodes: "
                    f"{invalid_input_nodes}"
                )

            if i == 0:
                first_actor_handles = current_actor_handles

            # Check all lists of DAGNode have the same number of nodes
            if len(inputs[0]) != len(inputs[i]):
                raise ValueError(
                    f"Expected all input lists to have the same number of nodes. "
                    f"List at index 0 has length {len(inputs[0])}, but list at "
                    f"index {i} has length {len(inputs[i])}."
                )

            # Check all lists of DAGNode have same set of actor handles
            if set(first_actor_handles) != set(current_actor_handles):
                raise ValueError(
                    f"Expected all input lists to have the same set of actor handles. "
                    f"List at index 0 has actors {set(first_actor_handles)}, but list at "
                    f"index {i} has actors {set(current_actor_handles)}."
                )

            # Check all lists of DAGNode have same order of actor handles
            for j, (first, current) in enumerate(
                zip(first_actor_handles, current_actor_handles)
            ):
                if first != current:
                    raise ValueError(
                        f"Expected all input lists to have the same order of actor handles. "
                        f"List at index 0 has actor {first} at position {j}, but list at "
                        f"index {i} has actor {current} at position {j}."
                    )
        self._actor_handles = current_actor_handles

        self._op = op
        if transport is None:
            transport = TorchTensorType.ACCELERATOR
        self._type_hint = TorchTensorType(transport=transport, _direct_return=True)
        if isinstance(transport, Communicator):
            if set(transport.get_actor_handles()) != set(self._actor_handles):
                raise ValueError(
                    "Expected actor handles to match the custom communicator group"
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

    def get_communicator(self) -> Communicator:
        if self._type_hint.communicator_id is not None:
            ctx = ChannelContext.get_current()
            communicator = ctx.communicators[self._type_hint.communicator_id]
        elif self._type_hint.get_custom_communicator() is not None:
            communicator = self._type_hint.get_custom_communicator()
        else:
            raise ValueError("Expected a communicator group")
        return communicator

    def execute(
        self, *send_buf: "torch.Tensor"
    ) -> Union["torch.Tensor", Tuple["torch.Tensor", ...]]:
        """
        Call the collective operation on the input tensor(s). Output tensor(s) are
        allocated and returned.

        Args:
            *send_buf: A variable number of torch tensors to send to the collective
                operation. The tensors have the same order as the input nodes.

        Returns:
            A torch tensor or a tuple of torch tensors containing the results of the
            collective operation. The output tensors have the same length and order
            as the input node list of the actor of this operation.
        """
        import torch

        if not all(isinstance(t, torch.Tensor) for t in send_buf):
            raise ValueError("Expected a torch tensor for each input node")

        communicator = self.get_communicator()
        if isinstance(self._op, AllGatherOp):
            assert len(send_buf) == 1
            t = send_buf[0]
            world_size = len(self._actor_handles)
            recv_buf = torch.empty(
                (t.shape[0] * world_size, *t.shape[1:]),
                dtype=t.dtype,
                device=t.device,
            )
            communicator.allgather(t, recv_buf)
        elif isinstance(self._op, AllReduceOp):
            if len(send_buf) == 1:
                t = send_buf[0]
                recv_buf = torch.empty_like(t)
                communicator.allreduce(t, recv_buf, self._op.reduceOp)
            else:
                if not all(t.dtype == send_buf[0].dtype for t in send_buf):
                    raise ValueError(
                        "Expected all input tensors to have the same dtype, "
                        f"but got {[t.dtype for t in send_buf]}"
                    )

                def unflatten_from(flat_buf, bufs):
                    views = []
                    offset = 0
                    for t in bufs:
                        numel = t.numel()
                        t = flat_buf[offset : offset + numel].view(t.shape)
                        views.append(t)
                        offset += numel
                    return tuple(views)

                flat_buf = torch.nn.utils.parameters_to_vector(send_buf)
                communicator.allreduce(flat_buf, flat_buf, self._op.reduceOp)
                recv_buf = unflatten_from(flat_buf, send_buf)
        elif isinstance(self._op, ReduceScatterOp):
            assert len(send_buf) == 1
            t = send_buf[0]
            world_size = len(self._actor_handles)
            if t.shape[0] % world_size != 0:
                raise ValueError(
                    "Expected the first dimension of the input tensor to be divisible "
                    f"by the world size {world_size}"
                )
            recv_buf = torch.empty(
                (t.shape[0] // world_size, *t.shape[1:]),
                dtype=t.dtype,
                device=t.device,
            )
            communicator.reducescatter(t, recv_buf, self._op.reduceOp)
        return recv_buf


@DeveloperAPI
class CollectiveOutputNode(ClassMethodNode):
    """Represent an output node from a communicator collective operation in a Ray DAG."""

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
        # Parse the input node(s).
        self._inputs = method_args
        # Parse the collective operation.
        self._collective_op: _CollectiveOperation = other_args_to_resolve.get(
            COLLECTIVE_OPERATION_KEY, None
        )
        self._is_class_method_output: bool = other_args_to_resolve.get(
            IS_CLASS_METHOD_OUTPUT_KEY, False
        )
        if self._collective_op is None and not self._is_class_method_output:
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
