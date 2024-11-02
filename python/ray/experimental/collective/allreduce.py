import logging
from typing import List, Optional, Union

import ray
from ray.dag.collective_node import CollectiveOutputNode, _CollectiveOperation
from ray.dag.constants import (
    BIND_INDEX_KEY,
    COLLECTIVE_OPERATION_KEY,
    PARENT_CLASS_NODE_KEY,
)
from ray.experimental.channel.torch_tensor_type import GPUCommunicator, TorchTensorType
from ray.experimental.util.types import ReduceOp
from ray.util.collective.types import ReduceOp as RayReduceOp

# TODO(wxdeng): Unify `ReduceOp` and `RayReduceOp`. Directly importing `RayReduceOp`
# has dependency issues for some tests.

logger = logging.getLogger(__name__)


class AllReduceWrapper:
    """Wrapper for NCCL all-reduce."""

    def bind(
        self,
        input_nodes: List["ray.dag.DAGNode"],
        op: ReduceOp = ReduceOp.SUM,
        transport: Optional[Union[str, GPUCommunicator]] = None,
    ) -> List[CollectiveOutputNode]:
        """
        Bind input nodes with a collective operation. The collective operation is
        directly applied to the torch tensors from the input nodes. The output nodes
        are the results of the collective operation in the same torch tensors.

        Requirements:
        1. Each input node returns a torch tensor.
        2. Each input node is from a different actor.
        3. If a custom transport is specified, its actor set matches the actor set
           of the input nodes.
        4. All tensors have the same shape.

        Requirements 1-3 are checked in the `CollectiveGroup` constructor.
        Requirement 4 is not checked yet.

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
        collective_op = _CollectiveOperation(input_nodes, op, transport)
        collective_output_nodes: List[CollectiveOutputNode] = []

        for input_node in input_nodes:
            actor_handle: Optional[
                "ray.actor.ActorHandle"
            ] = input_node._get_actor_handle()
            if actor_handle is None:
                raise ValueError("Expected an actor handle from the input node")
            collective_output_node = CollectiveOutputNode(
                method_name=f"allreduce.{op}",
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

    def __call__(
        self,
        tensor,
        group_name: str = "default",
        op: RayReduceOp = RayReduceOp.SUM,
    ):
        from ray.util.collective.collective import allreduce

        return allreduce(tensor, group_name, op)


allreduce = AllReduceWrapper()
