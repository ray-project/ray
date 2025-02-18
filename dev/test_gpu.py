import ray
import torch
import socket
import os
import torch.distributed as dist

# TODO(amjad/kai-hsun):
# - Read torch.distributed docs
# - Read NCCL docs
# - Walk through code for a single .remote() task submission.

# TODO(swang):
# On workers:
# 1. Check for CUDA tensor. If is CUDA tensor, use lambda
# to store in python actor state. Skip allocating in object store.
# 2. Return OBJECT_IN_ACTOR_STORE instead of OBJECT_IN_PLASMA.
# 3. On actor deserialize args, return OBJECT_IN_ACTOR_STORE errors directly.
# 4. In pyx execute_task handler, replace OBJECT_IN_ACTOR_STORE args
# with object from python actor state. Error if not found.

# On driver:
# 1. On initial task submission with the decorator call, put
# OBJECT_IN_ACTOR_STORE in local object store.
# (If actor task returns OBJECT_IN_ACTOR_STORE again, okay to ignore).
# 2. If task is submitted that depends on OBJECT_IN_ACTOR_STORE, submit
# send/recv tasks.
# 3. Add GC callback. Need actor address.


# TODO(later):
# - GPUObjectRef?


WORLD_SIZE = 2

@ray.remote
class Actor:

    def ping(self):
        return

    def setup(self, world_size, rank, init_method, group_name="default"):
        dist.init_process_group(backend="gloo", world_size=world_size, rank=rank, init_method=init_method)

    def randn(self, shape):
        return torch.randn(shape)

    def sum(self, tensor):
        return tensor.sum()

    def send(self, dst_rank, t):
        dist.send(t, dst_rank)

    def recv(self, src_rank, shape):
        t = torch.zeros(shape)
        dist.recv(t, src_rank)
        return t

if __name__ == "__main__":
    actors = [Actor.remote() for _ in range(WORLD_SIZE)]
    ray.get([a.ping.remote() for a in actors])
    print("actors started")

    # TODO: Replace with an API call that takes in a list of actors and
    # returns a handle to the group.
    init_method = "tcp://localhost:8889"
    ray.get([actor.setup.remote(WORLD_SIZE, rank, init_method) for rank, actor in enumerate(actors)])
    print("Collective group setup done")

    shape = (100, )

    ref = actors[0].randn.remote(shape)

    # After getting response from actor A, driver will now have in its local
    # heap object store:
    # ObjRef(xxx) -> OBJECT_IN_ACTOR, A.address

    # TODO: On task submission to actor B, driver looks up arguments to the task. 
    # driver:
    # - Driver sees that `ref` argument is on actor A.
    # - Driver submits A.send, B.recv tasks. Include ObjRef.
    # - Then, driver submits actual B.sum task.
    # B:
    # - Execute recv. B will have the tensor in its local actor store.
    # - Execute sum. When looking up arguments, it sees OBJECT_IN_ACTOR, so it
    # gets the actual value from its local actor store (which we know is
    # already there).
    s = ray.get(actors[1].sum.remote(ref))
    t = ray.get(ref)
    assert t.sum() == s

    # Instead of calling send/recv manually, we would like to do it
    # automatically, using the above API.
    actors[0].send.remote(1, ref)
    recved = actors[1].recv.remote(0, shape)
    s = ray.get(actors[1].sum.remote(recved))
    assert t.sum() == s
