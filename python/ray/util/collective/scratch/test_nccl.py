import ray
import cupy.cuda.nccl as nccl
from collections import defaultdict
import cupy as cp

@ray.remote(num_gpus=0.5)
class Actor:
    def __init(self):
        cp.cuda.Stream.null.synchronize()
        self.send = cp.ones((10,), dtype=cp.float32)
        self.recv = cp.zeros((10,), dtype=cp.float32)
        cp.cuda.Stream.null.synchronize()
   
   def send_getter(self):
        return self.send

    def send_setter(self, val):
        self.send = val

    def recv_getter(self):
        return self.recv

    def recv_setter(self, val):
        self.recv = val

    def compute(self):
        pass

class GroupManager(Object):
    def __init__(self):
        self.group = defaultdict([])

    def create_colletive_group(self,
                               backend,
                               group_name,
                               world_size,
                               rank,
                               actor,
                               uid=None):

        self.group[group_name].append({actor: nccl.NcclCommunicator(world_size, uid, rank)})



def declare_collective_group(actors, group_options):
    # sort actors by rank
    ranks = group_options["rank"]
    if len(actors) != len(ranks) or len(actors) != group_options["world_size"]:
        raise ValueError()

    #sorted_actors = [x for _,x in sorted(zip(ranks, actors)), key=lambda pair: pair[0]]
    uid = nccl.get_unique_id()
    for i in range(0, len(ranks):
        _group_mgr.create_collective_group(group_options["backend"],
                                           group_options["name"],
                                           group_options["world_size"],
                                           ranks,
                                           actors[i],
                                           uid)

def allreduce(group_name):
    for (actor, comm) in _group_mgr.group[group_name]:
        dummy = self.recv = cp.zeros((10,), dtype=cp.float32).ptr
        comm.allReduce(ray.get(actor.send_getter()).ptr, dummy, 10, cp.cuda.nccl.NCCL_FLOAT32, 1, cp.cuda.Stream.null.ptr)
        actor.set_recv(dummy)
        cp.cuda.Stream.null.synchronize()

_group_mgr = GroupManager()

group_options = {"name" : "haha", 
                 "backend" : "nccl",
                 "world_size" : 4, 
                 "rank" : [0,1,2,3]}

actors = [Actor().remote() for i in range(4)]
declare_collective_group(actors, group_options})

allreduce("haha")

for i in range(4):
    print(ray.get(actors[i].recv_getter()))
