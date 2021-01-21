import pygloo
import numpy as np
import os
import ray
import time
import shutil

@ray.remote(num_cpus=1)
def test_allreduce(rank, world_size, fileStore_path):
    '''
    rank = 0;  # Rank of this process within list of participating processes
    world_size = 2;  # Number of participating processes
    '''
    if rank==0:
        if os.path.exists(fileStore_path):
            shutil.rmtree(fileStore_path)
        os.makedirs(fileStore_path)
    else: time.sleep(0.5)
    context = pygloo.rendezvous.Context(rank, world_size);

    attr = pygloo.transport.tcp.attr("localhost")
    # Perform rendezvous for TCP pairs
    dev = pygloo.transport.tcp.CreateDevice(attr)

    fileStore_path
    fileStore = pygloo.rendezvous.FileStore(fileStore_path)
    store = pygloo.rendezvous.PrefixStore(str(world_size), fileStore)

    context.connectFullMesh(store, dev)

    sendbuf = np.array([[1,2,3],[1,2,3]], dtype=np.float32)
    recvbuf = np.zeros_like(sendbuf, dtype=np.float32)

    sendptr = sendbuf.__array_interface__["data"][0]
    recvptr = recvbuf.__array_interface__["data"][0]
    data_size = sendbuf.size

    pygloo.allreduce(context, sendptr, recvptr, data_size, pygloo.glooDataType_t.glooFloat32)

    print(f"rank {rank} sends {sendbuf}, \nreceives {recvbuf}")
    ## example output
    # (pid=30445) rank 0 sends [[1. 2. 3.]
    # (pid=30445)             [1. 2. 3.]],
    # (pid=30445) receives [[2. 4. 6.]
    # (pid=30445)          [2. 4. 6.]]
    # (pid=30446) rank 1 sends [[1. 2. 3.]
    # (pid=30446)              [1. 2. 3.]],
    # (pid=30446) receives [[2. 4. 6.]
    # (pid=30446)          [2. 4. 6.]]

if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_allreduce.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)

