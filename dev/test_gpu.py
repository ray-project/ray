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
# Worker return path:
# 1. Check for CUDA tensor. If is CUDA tensor, use lambda
# to store in python actor state. Skip allocating in object store.
# 2. Return OBJECT_IN_ACTOR_STORE instead of OBJECT_IN_PLASMA.

# Worker resolve arguments for task path:
# 1. In DependencyWaiter::Wait, find all OBJECT_IN_ACTOR args, and start the
# recv (in Python) for these args.
# 2. In pyx execute_task handler, during deserialization, replace
# OBJECT_IN_ACTOR_STORE args with object from python actor store. Error if not
# found.
# 3. After task is done, delete the object.

# Driver needs to store:
# - which actor holds the object
# - the shape and dtype for the object
# On driver:
# 1. On initial task submission with the decorator call, put
# OBJECT_IN_ACTOR_STORE in local object store.
# (If actor task returns OBJECT_IN_ACTOR_STORE again, okay to ignore).
# 2. If task is submitted that depends on OBJECT_IN_ACTOR_STORE, submit
# send/recv tasks.
# 3. Add GC callback. Need actor address.


# Submit an actor task:
# Python: .remote
# Driver C++: CoreWorker::SubmitActorTask. Create task spec:
# - assigns an execution index
# - put task arguments into the spec
# Driver C++: TaskManager::AddPendingTask
# - resolve arguments - wait for arguments to finish executing, then inline if needed into the taskspec
# <-- wait until actor A has finished randn and sent back OBJECT_IN_ACTOR error
# - send PushTaskRequest RPC to the actor to actually execute the task
# ---> if argument has OBJECT_IN_ACTOR error, submit a Send task or RPC to actor A

# Actor PushTaskRequest handler:
# - Queue the task.
# - Call DependencyWaiter.wait() on the task's OBJECT_IN_PLASMA arguments.
# - TODO: Add call to P2pDependencyWaiter.wait() on the task's OBJECT_IN_ACTOR arguments.
# --> call ncclRecv
# <-- wait until any plasma args are local
# - execute the task.



# 1. Submit A.randn
# 2. Submit B.sum
# <-- A.randn to finish, receive OBJECT_IN_ACTOR error
# --> submit A.send
# --> submit B.recv


# TODO(later):
# - GPUObjectRef? / decorator so that we know ahead of time whether the data contains a GPU tensor.
# - set up the transfer between actors
# - if driver calls ray.get, use the object store to transfer the data
# - Define the send/recv methods on the base Ray actor class
# - [Kai-Hsun] setting up and tearing down the collective group. torch.distributed or maybe ray.util.collective?
# - handle cases where same object gets used by multiple tasks. Then we don't want to erase it until all of the tasks are done.
# - Order the communication operations (RPCs don't guarantee ordering)


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
    print("Collective group setup done")

    shape = (100, )

    ref = actors[0].randn.remote(shape)
    print("ObjectRef:", ref)
    ref = actors[0].sum.remote(ref)
    print(ray.get(ref))

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
