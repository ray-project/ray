import pygloo
import numpy as np
import os
import ray
import time
import shutil
import torch

@ray.remote(num_cpus=1)
def test_gather(rank, world_size, fileStore_path):
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

    sendbuf = np.array([rank, rank+1], dtype=np.float32)
    sendptr = sendbuf.ctypes.data

    recvbuf = np.zeros((1, world_size*2), dtype=np.float32)
    recvptr = recvbuf.ctypes.data

    # sendbuf = torch.Tensor([i+1 for i in range(sum([j+1 for j in range(world_size)]))]).float()
    # sendptr = sendbuf.data_ptr()
    # recvbuf = torch.zeros(rank+1).float()
    # recvptr = recvbuf.data_ptr()

    data_size = sendbuf.size if isinstance(sendbuf, np.ndarray) else sendbuf.numpy().size
    datatype = pygloo.glooDataType_t.glooFloat32

    pygloo.gather(context, sendptr, recvptr, data_size, datatype, root = 0)

    print(f"rank {rank} sends {sendbuf}, receives {recvbuf}")

    ## example output
    # (pid=23172) rank 2 sends [2. 3.], receives [[0. 0. 0. 0. 0. 0.]]
    # (pid=23171) rank 1 sends [1. 2.], receives [[0. 0. 0. 0. 0. 0.]]
    # (pid=23173) rank 0 sends [0. 1.], receives [[0. 1. 1. 2. 2. 3.]]

if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 3
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_gather.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)
