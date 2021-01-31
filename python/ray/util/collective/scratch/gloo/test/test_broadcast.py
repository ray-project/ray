import pygloo
import numpy as np
import os
import ray
import time
import shutil
import torch

@ray.remote(num_cpus=1)
def test_broadcast(rank, world_size, fileStore_path):
    '''
    rank  # Rank of this process within list of participating processes
    world_size  # Number of participating processes
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

    fileStore = pygloo.rendezvous.FileStore(fileStore_path)
    store = pygloo.rendezvous.PrefixStore(str(world_size), fileStore)

    context.connectFullMesh(store, dev)

    if rank == 0:
        sendbuf = np.array([[1,2,3],[1,2,3]], dtype=np.float32)
        sendptr = sendbuf.ctypes.data
    else:
        sendbuf = np.zeros((2,3), dtype=np.float32)
        sendptr = -1
    recvbuf = np.zeros_like(sendbuf, dtype=np.float32)
    recvptr = recvbuf.ctypes.data

    # if rank == 0:
    #     sendbuf = torch.Tensor([[1,2,3],[1,2,3]]).float()
    #     sendptr = sendbuf.data_ptr()
    # else:
    #     sendbuf = torch.zeros(2,3)
    #     sendptr = 0
    # recvbuf = torch.zeros_like(sendbuf)
    # recvptr = recvbuf.data_ptr()

    data_size = sendbuf.size if isinstance(sendbuf, np.ndarray) else sendbuf.numpy().size
    datatype = pygloo.glooDataType_t.glooFloat32
    root = 0

    pygloo.broadcast(context, sendptr, recvptr, data_size, datatype, root)

    print(f"rank {rank} sends {sendbuf}, receives {recvbuf}")
    ## example output
    # (pid=36435) rank 1 sends [[0. 0. 0.]
    # (pid=36435)  [0. 0. 0.]], receives [[1. 2. 3.]
    # (pid=36435)  [1. 2. 3.]]
    # (pid=36432) rank 0 sends [[1. 2. 3.]
    # (pid=36432)  [1. 2. 3.]], receives [[1. 2. 3.]
    # (pid=36432)  [1. 2. 3.]]


if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_broadcast.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)
