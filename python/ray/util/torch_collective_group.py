import ray
import torch.distributed as dist
from enum import Enum
from typing import List
from ray import ObjectRef


class Backend(Enum):
    NCCL = "nccl"
    GLOO = "gloo"


class CollectiveGroup:
    def __init__(self, object_refs: List[ObjectRef]):
        self.object_refs = object_refs

    def ready(self):
        return ray.get(self.object_refs)


def init_collective_group(
    actors: List[ray.actor.ActorHandle], backend: str
) -> CollectiveGroup:
    assert backend in [b.value for b in Backend]
    world_size = len(actors)
    # TODO: init_method
    init_method = "tcp://localhost:8889"

    refs = []
    for rank, actor in enumerate(actors):
        refs.append(
            actor.__ray_call__.remote(
                lambda self: dist.init_process_group(
                    backend=backend,
                    world_size=world_size,
                    rank=rank,
                    init_method=init_method,
                )
            )
        )

    # Register actor id to rank mapping to the core worker.
    ray._private.worker.global_worker.core_worker.register_actor_nccl_group(
        [actor._actor_id for actor in actors]
    )

    # TODO: Register actor nccl group in core driver process
    return CollectiveGroup(refs)
