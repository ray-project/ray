import ray
import torch
import torch.distributed as dist
import time
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.experimental.channel import ChannelContext


# TODOs:
# [x] Decorator for GPU tensor returns
# [x] Custom serializer / deserializer to get value from actor store
# [ ] Return torch tensor metadata to driver when task finishes
# [x] register collectives in python
# [ ] On task submission, driver sends send/recv tasks to actors A and B


WORLD_SIZE = 2


@ray.remote
class Actor:
    def register_custom_serializer(self):
        TorchTensorType().register_custom_serializer()

    def setup(self, world_size, rank, init_method, group_name="default"):
        dist.init_process_group(
            backend="gloo", world_size=world_size, rank=rank, init_method=init_method
        )

    @ray.method(tensor_transport="nccl")
    def randn(self, shape):
        return torch.randn(shape)

    def sum(self, tensor):
        print("SUM", tensor)
        return tensor.sum().item()


if __name__ == "__main__":
    actors = [Actor.remote() for _ in range(WORLD_SIZE)]
    ray.get([a.__ray_ready__.remote() for a in actors])

    # TODO: Replace with an API call that takes in a list of actors and
    # returns a handle to the group.
    init_method = "tcp://localhost:8889"
    ray.get(
        [
            actor.setup.remote(WORLD_SIZE, rank, init_method)
            for rank, actor in enumerate(actors)
        ]
    )
    actor_ids = [actor._ray_actor_id for actor in actors]

    # TODO(swang): Wrap actors in a Communicator interface.
    ctx = ChannelContext.get_current()
    ctx.communicators[0] = actors
    print("Collective group setup done")

    ray.get([actor.register_custom_serializer.remote() for actor in actors])
    print("Registered custom serializer")

    shape = (1,)

    start = time.time()
    for _ in range(10):
        ref = actors[0].randn.remote(shape)
        ref = actors[1].sum.remote(ref)
        print(ray.get(ref))
    end = time.time()
    print((end - start) / 10)
