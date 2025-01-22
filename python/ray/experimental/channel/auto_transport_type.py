from typing import Dict, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelOutputType
from ray.experimental.channel.torch_tensor_type import TorchTensorType


class TypeHintResolver:
    """
    This class is used to resolve `AutoChannelType` into an actual channel type
    (e.g., `TorchTensorType` with proper transport) based on node locations and
    GPU IDs of the readers and writers.
    """

    def __init__(self, actor_to_gpu_ids: Dict["ray.actor.ActorHandle", List[str]]):
        """
        Args:
            actor_to_gpu_ids: Mapping from actor handle to its GPU IDs.
        """
        self.actor_to_gpu_ids = actor_to_gpu_ids

    def _use_same_gpu(
        self,
        writer_and_node: Tuple["ray.actor.ActorHandle", str],
        reader_and_node: Union[
            Tuple["ray.actor.ActorHandle", str],
            List[Tuple["ray.actor.ActorHandle", str]],
        ],
    ) -> bool:
        """
        Check if the writer and readers use the same GPU.

        Args:
            writer_and_node: A tuple of writer actor handle and its node ID.
            reader_and_node: A tuple of reader actor handle and its node ID, or
                a list of such tuples.

        Returns:
            True if the writer and all the readers use the same GPU, False otherwise.
        """
        if isinstance(reader_and_node, list):
            return all(
                self._use_same_gpu(writer_and_node, entry) for entry in reader_and_node
            )
        if writer_and_node[1] != reader_and_node[1]:
            return False
        writer_gpu_ids = self._check_single_gpu(
            self.actor_to_gpu_ids.get(writer_and_node[0], [])
        )
        reader_gpu_ids = self._check_single_gpu(
            self.actor_to_gpu_ids.get(reader_and_node[0], [])
        )
        return writer_gpu_ids == reader_gpu_ids

    def _use_gpu(
        self, actors: Union["ray.actor.ActorHandle", List["ray.actor.ActorHandle"]]
    ) -> bool:
        """
        Check if the actors use GPUs.

        Args:
            actors: An actor handle or a list of actor handles.

        Returns:
            True if the actors use GPUs, False otherwise.
        """
        if isinstance(actors, list):
            return all(self._use_gpu(actor) for actor in actors)
        gpu_ids = self.actor_to_gpu_ids.get(actors, [])
        return len(self._check_single_gpu(gpu_ids)) > 0

    def _check_single_gpu(self, gpu_ids: List[str]) -> List[str]:
        """
        Check and assert gpu_ids has only one GPU ID.

        Returns:
            The same list of GPU IDs as passed in.
        """
        assert len(gpu_ids) <= 1, (
            "Compiled Graphs currently don't support allocating multiple GPUs "
            "to a single actor"
        )
        return gpu_ids

    def resolve(
        self,
        writer_and_node: Tuple["ray.actor.ActorHandle", str],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
    ) -> "ChannelOutputType":
        """
        Resolve to the actual channel type based on the node locations and GPU IDs.

        Args:
            writer_and_node: A tuple of writer actor handle and its node ID.
            reader_and_node_list: A list of tuples of reader actor handle and its
                node ID.

        Returns:
            The actual channel type.
        """
        writer = writer_and_node[0]
        readers = [reader for reader, _ in reader_and_node_list]

        if any(reader is None for reader in readers):
            # None means reader is the driver, currently driver on GPU
            # is not supported, so we always use shared memory to transfer
            # tensors.
            return TorchTensorType()

        # Case 1: writer and readers don't both use GPU, use shared memory
        # to transport the tensors
        if not (self._use_gpu(writer) and self._use_gpu(readers)):
            return TorchTensorType()

        # Case 2: writer and readers use the same GPU are are on the same node,
        # use shared memory to transport the tensors
        if self._use_same_gpu(writer_and_node, reader_and_node_list):
            return TorchTensorType()

        # Case 3: writer and readers use different GPUs, use NCCL to transport
        # the tensors
        return TorchTensorType(transport="nccl")


class AutoTransportType(ChannelOutputType):
    """
    Type hint for automatic transport selection for tensors.

    With this type hint Compiled Graphs automatically decide the best transport
    to use (e.g., NCCL or shared memory) based on the node locations and GPU IDs
    of the readers and writers.
    """

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        driver_actor_id: Optional[str] = None,
    ) -> "ChannelOutputType":
        """
        Directly calling create_channel() on AutoTransportType should not happen,
        just raise an exception with informative message.
        """
        raise ValueError(
            "This should not happen: AutoTransportType should "
            "have been resolved before creating a channel. "
            "Please file a Ray GitHub issue for bug report."
        )
