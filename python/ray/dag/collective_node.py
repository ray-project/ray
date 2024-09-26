from weakref import ReferenceType
from typing import Any, Dict, List, Union, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import torch

import ray
from ray.dag import (
    DAGNode,
    ClassNode,
)
from ray.dag.constants import (
    PARENT_CLASS_NODE_KEY,
    BIND_INDEX_KEY,
    COLLECTIVE_GROUP_KEY,
)
from ray.dag.format_utils import get_dag_node_str
from ray.util.annotations import DeveloperAPI
from ray.util.collective.nccl_types import CollectiveOp, ReduceOp
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.torch_tensor_nccl_channel import _init_nccl_group
from ray.experimental.channel.torch_tensor_type import GPUCommunicator, TorchTensorType


class _CollectiveGroup:
    """Represent metadata for a NCCL collective method."""

    def __init__(
        self,
        input_nodes: List[DAGNode],
        op: CollectiveOp,
        transport: Union[str, GPUCommunicator] = TorchTensorType.NCCL,
    ):
        self._input_nodes: List[DAGNode] = input_nodes
        if len(self._input_nodes) == 0:
            raise ValueError("Expected input nodes for a collective group")
        if len(set(self._input_nodes)) != len(self._input_nodes):
            raise ValueError("Expected unique input nodes for a collective group")

        self._actor_handles: List["ray.actor.ActorHandle"] = []
        for input_node in self._input_nodes:
            actor_handle = input_node._get_actor_handle()
            assert actor_handle is not None, "Expected a actor handle"
            self._actor_handles.append(actor_handle)
        if len(set(self._actor_handles)) != len(self._actor_handles):
            raise ValueError("Expected unique actor handles for a collective group")

        self._op = op
        assert isinstance(
            self._op, ReduceOp
        ), "Other collective ops are not implemented"
        self._type_hint = TorchTensorType(transport=transport, _direct_return=True)
        if isinstance(transport, GPUCommunicator):
            assert set(transport.get_actor_handles()) == set(
                self._actor_handles
            ), "Expected actor handles to match the custom NCCL group"

    def __str__(self) -> str:
        return (
            f"CollectiveGroup("
            f"_input_nodes={self._input_nodes}, "
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

    def init_nccl_group(self) -> str:
        type_hint = self._type_hint
        if type_hint.nccl_group_id is not None:
            # The NCCL group has already been initialized.
            return type_hint.nccl_group_id
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

    def method(self, tensor: "torch.Tensor"):
        import torch

        assert isinstance(tensor, torch.Tensor), "Expected a torch tensor"
        nccl_group = self.get_nccl_group()
        assert isinstance(
            self._op, ReduceOp
        ), "Other collective ops are not yet implemented"
        tensor_copy = tensor.clone()
        nccl_group.allreduce(tensor_copy, self._op)
        return tensor_copy


@DeveloperAPI
class CollectiveOutputNode(DAGNode):
    """Represent an output node from a NCCL collective method in a Ray DAG."""

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
        self._bound_args = method_args or []
        self._bound_kwargs = method_kwargs or {}
        self._bound_options = method_options or {}
        self._method_name: str = method_name
        # Parse other_args_to_resolve and assign to variables
        self._parent_class_node: Union[
            ClassNode, ReferenceType["ray._private.actor.ActorHandle"]
        ] = other_args_to_resolve.get(PARENT_CLASS_NODE_KEY)
        # The index/order when bind() is called on this class method
        self._bind_index: Optional[int] = other_args_to_resolve.get(
            BIND_INDEX_KEY, None
        )

        # Parse the input node.
        assert (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], DAGNode)
        ), "Expected a single input node"
        self._input_node = method_args[0]
        # Parse the collective group.
        self._collective_group: _CollectiveGroup = other_args_to_resolve.get(
            COLLECTIVE_GROUP_KEY, None
        )
        assert self._collective_group is not None, "Expected a collective group"

        # The actor creation task dependency is encoded as the first argument,
        # and the ordering dependency as the second, which ensures they are
        # executed prior to this node.
        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve=other_args_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _CollectiveGroup(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        raise NotImplementedError("CollectiveOutputNode is only supported for aDAG")

    def __str__(self) -> str:
        return get_dag_node_str(self, f"{self._method_name}()")

    def get_method_name(self) -> str:
        return self._method_name

    def _get_bind_index(self) -> int:
        return self._bind_index

    def _get_remote_method(self, method_name):
        method_body = getattr(self._parent_class_node, method_name)
        return method_body

    def _get_actor_handle(self) -> Optional["ray.actor.ActorHandle"]:
        if not isinstance(self._parent_class_node, ray.actor.ActorHandle):
            return None
        return self._parent_class_node

    @property
    def collective_group(self) -> _CollectiveGroup:
        return self._collective_group
