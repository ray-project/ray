from typing import Dict, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelOutputType
from ray.experimental.channel.torch_tensor_type import TorchTensorType


class TypeHintResolver:
    def __init__(self, actor_to_gpu_ids: Dict["ray.actor.ActorHandle", List[str]]):
        self.actor_to_gpu_ids = actor_to_gpu_ids

    def _use_same_gpu(
        self,
        writer_and_node: Tuple["ray.actor.ActorHandle", str],
        reader_and_node: Union[
            Tuple["ray.actor.ActorHandle", str],
            List[Tuple["ray.actor.ActorHandle", str]],
        ],
    ) -> bool:
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
        if isinstance(actors, list):
            return all(self._use_gpu(actor) for actor in actors)
        gpu_ids = self.actor_to_gpu_ids.get(actors, [])
        return len(self._check_single_gpu(gpu_ids)) > 0

    def _check_single_gpu(self, gpu_ids: List[str]) -> bool:
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
        writer = writer_and_node[0]
        readers = [reader for reader, _ in reader_and_node_list]
        if not (self._use_gpu(writer) and self._use_gpu(readers)):
            print("Creating shared memory channel")
            # TODO: technically it's using SharedMemoryType, but we
            # still need to register the custom serializer for it.
            return TorchTensorType()

        if self._use_same_gpu(writer_and_node, reader_and_node_list):
            # TODO: technically it's using SharedMemoryType, but we
            # still need to register the custom serializer for it.
            return TorchTensorType()

        return TorchTensorType(transport="nccl")


class AutoChannelType(ChannelOutputType):
    def create_channel(
        self,
        writer: "ray.actor.ActorHandle",
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        driver_actor_id: Optional[str] = None,
    ):
        raise ValueError(
            "This should not happen: AutoChannelType should "
            "have been resolved before creating a channel."
            "Please file a Ray GitHub issue for bug report."
        )
