import logging
from typing import List, Optional, Union

import ray
from ray.dag.collective_node import CollectiveOutputNode, _CollectiveOperation
from ray.dag.constants import (
    BIND_INDEX_KEY,
    COLLECTIVE_OPERATION_KEY,
    PARENT_CLASS_NODE_KEY,
)
from ray.experimental.channel.torch_tensor_type import Communicator, TorchTensorType
from ray.experimental.util.types import (
    ReduceOp,
    AllGatherOp,
    AllReduceOp,
    ReduceScatterOp,
    BroadcastOp,
    _CollectiveOp,
)
from ray.util.collective.types import ReduceOp as RayReduceOp

logger = logging.getLogger(__name__)


def _bind(
    input_nodes: List["ray.dag.DAGNode"],
    op: _CollectiveOp,
    *,
    root_node: Optional["ray.dag.DAGNode"] = None,
    transport: Optional[Union[str, Communicator]] = None,
):
    """
    Bind input nodes with a collective operation. The collective operation is
    directly applied to the torch tensors from the input nodes. The output nodes
    are the results of the collective operation in the same torch tensors.

    Requirements:
    1. Each input node returns a torch tensor.
    2. Each input node is from a different actor.
    3. If a custom transport is specified, its actor set matches the actor set
        of the input nodes.
    4. If a root node is specified, it must be an input node.
    5. All tensors have the same shape.

    Requirements 1-4 are checked in the `CollectiveGroup` constructor.
    Requirement 5 is not checked yet.

    Args:
        input_nodes: A list of DAG nodes.
        op: The collective operation.
        transport: GPU communicator for the collective operation. If not
            specified, the default NCCL is used.

    Returns:
        A list of collective output nodes.
    """
    if transport is None:
        transport = TorchTensorType.NCCL
    collective_op = _CollectiveOperation(input_nodes, op, root_node, transport)
    collective_output_nodes: List[CollectiveOutputNode] = []

    actor_handle: Optional["ray.actor.ActorHandle"] = input_nodes[0]._get_actor_handle()
    if actor_handle is None:
        raise ValueError("Expected an actor handle from the input node")
    if isinstance(op, AllReduceOp):
        method_name = f"allreduce.{op.reduceOp}"
    elif isinstance(op, ReduceScatterOp):
        method_name = f"reducescatter.{op.reduceOp}"
    elif isinstance(op, AllGatherOp):
        method_name = "allgather"
    elif isinstance(op, BroadcastOp):
        method_name = "broadcast"
    else:
        raise ValueError(f"Expected a collective operation, but found {op}")

    for input_node in input_nodes:
        actor_handle: Optional["ray.actor.ActorHandle"] = input_node._get_actor_handle()
        collective_output_node = CollectiveOutputNode(
            method_name=method_name,
            method_args=(input_node,),
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve={
                PARENT_CLASS_NODE_KEY: actor_handle,
                BIND_INDEX_KEY: actor_handle._ray_dag_bind_index,
                COLLECTIVE_OPERATION_KEY: collective_op,
            },
        )
        actor_handle._ray_dag_bind_index += 1
        collective_output_nodes.append(collective_output_node)
    return collective_output_nodes


class AllGatherWrapper:
    """Wrapper for NCCL all-gather."""

    def bind(
        self,
        input_nodes: List["ray.dag.DAGNode"],
        transport: Optional[Union[str, Communicator]] = None,
    ) -> List[CollectiveOutputNode]:
        return _bind(input_nodes, AllGatherOp(), transport=transport)

    def __call__(
        self,
        tensor_list,
        tensor,
        group_name: str = "default",
    ):
        from ray.util.collective.collective import allgather

        return allgather(tensor_list, tensor, group_name)


class AllReduceWrapper:
    """Wrapper for NCCL all-reduce."""

    def bind(
        self,
        input_nodes: List["ray.dag.DAGNode"],
        op: ReduceOp = ReduceOp.SUM,
        transport: Optional[Union[str, Communicator]] = None,
    ) -> List[CollectiveOutputNode]:
        if not isinstance(op, ReduceOp):
            raise ValueError(f"Unexpected operation: {op}")

        return _bind(input_nodes, AllReduceOp(reduceOp=op), transport=transport)

    def __call__(
        self,
        tensor,
        group_name: str = "default",
        op: RayReduceOp = RayReduceOp.SUM,
    ):
        from ray.util.collective.collective import allreduce

        return allreduce(tensor, group_name, op)


class ReduceScatterWrapper:
    """Wrapper for NCCL reduce-scatter."""

    def bind(
        self,
        input_nodes: List["ray.dag.DAGNode"],
        op: ReduceOp = ReduceOp.SUM,
        transport: Optional[Union[str, Communicator]] = None,
    ) -> List[CollectiveOutputNode]:
        if not isinstance(op, ReduceOp):
            raise ValueError(f"Unexpected operation: {op}")

        return _bind(input_nodes, ReduceScatterOp(reduceOp=op), transport=transport)

    def __call__(
        self,
        tensor,
        group_name: str = "default",
        op: RayReduceOp = RayReduceOp.SUM,
    ):
        from ray.util.collective.collective import reducescatter

        return reducescatter(tensor, group_name, op)


class BroadcastWrapper:
    """Wrapper for NCCL broadcast."""

    def bind(
        self,
        root_node: "ray.dag.DAGNode",
        input_nodes: List["ray.dag.DAGNode"],
        transport: Optional[Union[str, Communicator]] = None,
    ) -> List[CollectiveOutputNode]:
        return _bind(
            input_nodes, BroadcastOp(), root_node=root_node, transport=transport
        )

    def __call__(
        self,
        tensor,
        src_rank: int,
        group_name: str = "default",
    ):
        from ray.util.collective.collective import broadcast

        return broadcast(tensor, src_rank, group_name)


allgather = AllGatherWrapper()
allreduce = AllReduceWrapper()
reducescatter = ReduceScatterWrapper()
broadcast = BroadcastWrapper()
