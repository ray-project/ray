import ray
import torch
import socket
import os
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

    def ping(self):
        return

    def setup(self, world_size, rank, init_method, group_name="default"):
        dist.init_process_group(backend="gloo", world_size=world_size, rank=rank, init_method=init_method)

    @ray.method(tensor_transport="nccl")
    def randn(self, shape):
        return torch.randn(shape)

    def sum(self, tensor):
        print("SUM")
        return tensor.sum().item()

    def send(self, meta, dst_rank):
        worker = ray._private.worker.global_worker
        tensor = worker.in_actor_object_store[meta.obj_id]
        dist.send(tensor, dst_rank)

    def recv(self, meta, src_rank):
        worker = ray._private.worker.global_worker
        tensor = torch.zeros(meta.shape, dtype=meta.dtype)
        dist.recv(tensor, src_rank)
        worker.in_actor_object_store[meta.obj_id] = tensor

if __name__ == "__main__":
    actors = [Actor.remote() for _ in range(WORLD_SIZE)]
    ray.get([a.ping.remote() for a in actors])
    print("actors started")

    # TODO: Replace with an API call that takes in a list of actors and
    # returns a handle to the group.
    init_method = "tcp://localhost:8889"
    ray.get([actor.setup.remote(WORLD_SIZE, rank, init_method) for rank, actor in enumerate(actors)])
    actor_ids = [actor._ray_actor_id for actor in actors]

    # TODO(swang): Wrap actors in a Communicator interface.
    ctx = ChannelContext.get_current()
    ctx.communicators[0] = actors
    print("Collective group setup done")

    ray.get([actor.register_custom_serializer.remote() for actor in actors])
    print("Serialization done")

    shape = (1, )

    ref = actors[0].randn.remote(shape)
    ref = actors[1].sum.remote(ref)
    print(ray.get(ref))

    start = time.time()
    for _ in range(10):
        ref = actors[0].randn.remote(shape)
        ref = actors[1].sum.remote(ref)
        print(ray.get(ref))
    end = time.time()
    print((end - start) / 10)

    ## After getting response from actor A, driver will now have in its local
    ## heap object store:
    ## ObjRef(xxx) -> OBJECT_IN_ACTOR, A.address

    ## TODO: On task submission to actor B, driver looks up arguments to the task. 
    ## driver:
    ## - Driver sees that `ref` argument is on actor A.
    ## - Driver submits A.send, B.recv tasks. Include ObjRef.
    ## - Then, driver submits actual B.sum task.
    ## B:
    ## - Execute recv. B will have the tensor in its local actor store.
    ## - Execute sum. When looking up arguments, it sees OBJECT_IN_ACTOR, so it
    ## gets the actual value from its local actor store (which we know is
    ## already there).
    #s = ray.get(actors[1].sum.remote(ref))
    #t = ray.get(ref)
    #assert t.sum() == s

    ## Instead of calling send/recv manually, we would like to do it
    ## automatically, using the above API.
    #actors[0].send.remote(1, ref)
    #recved = actors[1].recv.remote(0, shape)
    #s = ray.get(actors[1].sum.remote(recved))
    #assert t.sum() == s
