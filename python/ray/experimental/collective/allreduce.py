import logging
from typing import List, Optional, Union

import ray
from ray.dag.collective_node import CollectiveGroup, CollectiveOutputNode
from ray.dag.constants import (
    BIND_INDEX_KEY,
    COLLECTIVE_GROUP_KEY,
    PARENT_CLASS_NODE_KEY,
)
from ray.dag.dag_node import DAGNode
from ray.experimental.channel.torch_tensor_type import GPUCommunicator, TorchTensorType
from ray.util.collective import types as ray_types
from ray.util.collective.nccl_types import ReduceOp

logger = logging.getLogger(__name__)


class AllReduceWrapper:
    """Wrapper for NCCL all-reduce."""

    def bind(
        self,
        input_nodes: List["DAGNode"],
        op: ReduceOp = ReduceOp.SUM,
        transport: Union[str, GPUCommunicator] = TorchTensorType.NCCL,
    ) -> List[CollectiveOutputNode]:
        # [TODO] Polish.
        """
        Binds tensors extracted from the given DAGNodes to an all-reduce operation.
        Requirements:
        1. Each given input node must be resolved to a tensor during execution.
        2. Each tensor must be on a different actor.
        3. All tensors must have the same numel (from `torch.Tensor.numel()`).
        4. If a custom `GPUCommunicator` is provided,
           its actors (from `GPUCommunicator.get_actor_handles()`) must match
           the actors of the input nodes (i.e., the actors of the tensors).
        Args:
            input_nodes: A list of DAGNodes.
            op: The reduce operation, which can be SUM (default), PRODUCT, MIN, or MAX.
            comm: GPU communicator to be used during the execution of all-reduce.
                If None (default), a `_NCCLGroup` will be used.
        Returns:
            A list of output nodes of the all-reduce operation.
        """
        collective_group = CollectiveGroup(input_nodes, op, transport)
        collective_output_nodes: List[CollectiveOutputNode] = []

        for input_node in input_nodes:
            actor_handle: Optional[
                "ray.actor.ActorHandle"
            ] = input_node._get_actor_handle()
            assert actor_handle
            collective_output_node = CollectiveOutputNode(
                method_name="allreduce",  # [TODO] From op.
                method_args=(input_node,),
                method_kwargs=dict(),
                method_options=dict(),
                other_args_to_resolve={
                    PARENT_CLASS_NODE_KEY: actor_handle,
                    BIND_INDEX_KEY: actor_handle._ray_dag_bind_index,
                    COLLECTIVE_GROUP_KEY: collective_group,
                },
            )
            actor_handle._ray_dag_bind_index += 1
            collective_output_nodes.append(collective_output_node)

        return collective_output_nodes

    def __call__(
        self,
        tensor,
        group_name: str = "default",
        op: ray_types.ReduceOp = ray_types.ReduceOp.SUM,
    ):
        from ray.util.collective.collective import allreduce

        return allreduce(tensor, group_name, op)


allreduce = AllReduceWrapper()
