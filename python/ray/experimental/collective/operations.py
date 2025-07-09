import logging
from typing import List, Optional, Union

import ray
from ray.dag.collective_node import CollectiveOutputNode, _CollectiveOperation
from ray.dag.constants import (
    BIND_INDEX_KEY,
    COLLECTIVE_OPERATION_KEY,
    PARENT_CLASS_NODE_KEY,
    IS_CLASS_METHOD_OUTPUT_KEY,
)
from ray.experimental.channel.torch_tensor_type import Communicator, TorchTensorType
from ray.experimental.util.types import (
    ReduceOp,
    AllGatherOp,
    AllReduceOp,
    ReduceScatterOp,
    _CollectiveOp,
)
from ray.util.collective.types import ReduceOp as RayReduceOp

logger = logging.getLogger(__name__)


def _bind(
    inputs: Union[List["ray.dag.DAGNode"], List[List["ray.dag.DAGNode"]]],
    op: _CollectiveOp,
    transport: Optional[Union[str, Communicator]] = None,
):
    """
    Bind inputs (input nodes or lists of input nodes) with a collective operation.
    The collective operation is applied to each list of input nodes. The output nodes
    will have the same shape as the input nodes.

    Example of binding a list of input node:
    with InputNode() as inp:
        res_comp1 = [actor.comp1.bind(inp) for actor in actors]
        res_comp2 = [actor.comp2.bind(inp) for actor in actors]
        res_ar = allreduce.bind([res_comp1, res_comp2])

    Requirements:
    1. Each input node returns a torch tensor.
    2. Each input node within a list is from a different actor.
    3. If lists of input nodes are provided, the order of actors should
        be the same for each nested list.
    4. If a custom transport is specified, its actor set matches the actor
        set of the input nodes.
    5. If input nodes are provided, then all tensors have the same shape.
        If lists of input nodes are provided, then all tensors in each
        list have the same shape.

    Requirements 1-3 are checked in the `CollectiveGroup` constructor.
    Requirement 4 is not checked yet.

    Args:
        inputs: A list of DAG nodes or a list of lists of DAG nodes. Each leaf list
            should contain one object per actor.
        op: The collective operation.
        transport: GPU communicator for the collective operation. If not
            specified, the default ACCELERATOR is used.

    Returns:
        A list of collective output nodes or a list of lists of collective output nodes,
        with the same shape as the input nodes. Each output node has the same order and
        belongs to the same actor as the corresponding input node.
    """
    if isinstance(inputs[0], list) and not isinstance(op, AllReduceOp):
        raise ValueError(
            "Currently binding a nested list of dag nodes is only supported for allreduce"
        )

    # Convert list of DAGNode into nested list for type checking
    if not isinstance(inputs[0], list):
        inputs = [inputs]

    if transport is None:
        transport = TorchTensorType.ACCELERATOR
    collective_op = _CollectiveOperation(inputs, op, transport)
    collective_output_nodes: List[CollectiveOutputNode] = []

    if isinstance(op, AllGatherOp):
        method_name = "allgather"
    elif isinstance(op, AllReduceOp):
        method_name = f"allreduce.{op.reduceOp}"
    elif isinstance(op, ReduceScatterOp):
        method_name = f"reducescatter.{op.reduceOp}"
    else:
        raise ValueError(f"Expected a collective operation, but got {op}")

    for i in range(len(inputs[0])):
        input_node_list = [l[i] for l in inputs if l]
        actor_handle: Optional["ray.actor.ActorHandle"] = input_node_list[
            0
        ]._get_actor_handle()
        assert actor_handle is not None
        collective_output_node = CollectiveOutputNode(
            method_name=method_name,
            method_args=tuple(input_node_list),
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve={
                PARENT_CLASS_NODE_KEY: actor_handle,
                BIND_INDEX_KEY: actor_handle._ray_dag_bind_index,
                COLLECTIVE_OPERATION_KEY: collective_op,
            },
        )
        actor_handle._ray_dag_bind_index += 1

        if len(input_node_list) > 1:
            output_nodes: List[CollectiveOutputNode] = []
            for i in range(len(input_node_list)):
                output_node = CollectiveOutputNode(
                    f"return_idx_{i}",
                    (collective_output_node, i),
                    dict(),
                    dict(),
                    {
                        BIND_INDEX_KEY: collective_output_node._get_bind_index(),
                        IS_CLASS_METHOD_OUTPUT_KEY: True,
                        PARENT_CLASS_NODE_KEY: actor_handle,
                    },
                )
                output_nodes.append(output_node)
            collective_output_nodes.append(output_nodes)
        else:
            collective_output_nodes.append(collective_output_node)
    return collective_output_nodes


class AllGatherWrapper:
    """Wrapper for NCCL all-gather."""

    def bind(
        self,
        input_nodes: List["ray.dag.DAGNode"],
        transport: Optional[Union[str, Communicator]] = None,
    ) -> List[CollectiveOutputNode]:
        return _bind(input_nodes, AllGatherOp(), transport)

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

        return _bind(input_nodes, AllReduceOp(reduceOp=op), transport)

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

        return _bind(input_nodes, ReduceScatterOp(reduceOp=op), transport)

    def __call__(
        self,
        tensor,
        group_name: str = "default",
        op: RayReduceOp = RayReduceOp.SUM,
    ):
        from ray.util.collective.collective import reducescatter

        return reducescatter(tensor, group_name, op)


allgather = AllGatherWrapper()
allreduce = AllReduceWrapper()
reducescatter = ReduceScatterWrapper()
