import pygloo
import numpy as np
import os
import ray
import time
import shutil
import torch

@ray.remote(num_cpus=1)
def test_allgather(rank, world_size, fileStore_path):
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

    sendbuf = np.array([[1,2,3],[1,2,3]], dtype=np.float32)
    recvbuf = np.zeros([world_size] + list(sendbuf.shape), dtype=np.float32)
    sendptr = sendbuf.ctypes.data
    recvptr = recvbuf.ctypes.data

    # sendbuf = torch.Tensor([[1,2,3],[1,2,3]]).float()
    # recvbuf = torch.zeros([world_size] + list(sendbuf.shape)).float()
    # sendptr = sendbuf.data_ptr()
    # recvptr = recvbuf.data_ptr()

    assert sendbuf.size() * world_size == recvbuf.size()

    data_size = sendbuf.size if isinstance(sendbuf, np.ndarray) else sendbuf.numpy().size
    datatype = pygloo.glooDataType_t.glooFloat32

    pygloo.allgather(context, sendptr, recvptr, data_size, datatype)

    print(f"rank {rank} sends {sendbuf},\nreceives {recvbuf}")

    ## example output
    # (pid=29044) rank 0 sends [[1. 2. 3.]
    # (pid=29044)              [1. 2. 3.]],
    # (pid=29044) receives [[[1. 2. 3.]
    # (pid=29044)          [1. 2. 3.]]
    # (pid=29044)          [[1. 2. 3.]
    # (pid=29044)          [1. 2. 3.]]]
    # (pid=29046) rank 1 sends [[1. 2. 3.]
    # (pid=29046)              [1. 2. 3.]],
    # (pid=29046) receives [[[1. 2. 3.]
    # (pid=29046)          [1. 2. 3.]]
    # (pid=29046)          [[1. 2. 3.]
    # (pid=29046)          [1. 2. 3.]]]

if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_allgather.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)
