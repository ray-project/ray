import pygloo
import numpy as np
import os
import ray
import time
import shutil

@ray.remote(num_cpus=1)
def test1(rank, world_size, fileStore_path):
    '''
    rank = 0;  # Rank of this process within list of participating processes
    world_size = 2;  # Number of participating processes
    '''
    sendbuf = np.ones((1,2))
    recvbuf = np.zeros((1,2))

    if rank==0:
        if os.path.exists(fileStore_path):
            shutil.rmtree(fileStore_path)
        os.makedirs(fileStore_path)
    else: time.sleep(0.5)
    context = pygloo.rendezvous.Context(rank, world_size);

    # help(pygloo.transport.tcp.attr)

    attr = pygloo.transport.tcp.attr("localhost")
    # Perform rendezvous for TCP pairs
    dev = pygloo.transport.tcp.CreateDevice(attr)

    fileStore_path
    fileStore = pygloo.rendezvous.FileStore(fileStore_path)
    # redis = pygloo.RedisStore("redishost")
    # prefix = os.getenv("PREFIX")
    store = pygloo.rendezvous.PrefixStore(str(world_size), fileStore)

    context.connectFullMesh(store, dev)

    pygloo.allreduce(context, sendbuf, recvbuf, sendbuf.size, 0)

    print(f"rank {rank} sends {sendbuf}, receives {recvbuf}")
    ## example output
    # (pid=3643) rank 0 sends [[1. 1.]], receives [[4. 4.]]
    # (pid=3648) rank 1 sends [[1. 1.]], receives [[4. 4.]]

if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test1.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)

