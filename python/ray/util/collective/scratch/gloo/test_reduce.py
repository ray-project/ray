import pygloo
import numpy as np
import os
import ray
import time
import shutil
import torch

@ray.remote(num_cpus=1)
def test_reduce(rank, world_size, fileStore_path):
    '''
    rank  # Rank of this process within list of participating processes
    world_size  # Number of participating processes

    BUG:
        The non-root recvbuf will recieves some data.
        Example: root is 0. But recvbuf of rank 1 shouldn't change.
        (pid=5479) rank 0 sends [[1. 2. 3.]
        (pid=5479)              [1. 2. 3.]],
        (pid=5479)  receives [[2. 4. 6.]
        (pid=5479)           [2. 4. 6.]]
        (pid=5478) rank 1 sends [[1. 2. 3.]
        (pid=5478)              [1. 2. 3.]],
        (pid=5478)  receives [[0. 0. 0.]
        (pid=5478)           [0. 4. 6.]]
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
    recvbuf = np.zeros_like(sendbuf, dtype=np.float32)
    sendptr = sendbuf.ctypes.data
    recvptr = recvbuf.ctypes.data

    # sendbuf = torch.Tensor([[1,2,3],[1,2,3]]).float()
    # recvbuf = torch.zeros_like(sendbuf)
    # sendptr = sendbuf.data_ptr()
    # recvptr = recvbuf.data_ptr()

    data_size = sendbuf.size if isinstance(sendbuf, np.ndarray) else sendbuf.numpy().size
    datatype = pygloo.glooDataType_t.glooFloat32
    op = pygloo.ReduceOp.SUM
    root = 0

    pygloo.reduce(context, sendptr, recvptr, data_size, datatype, op, root)

    print(f"rank {rank} sends {sendbuf}, receives {recvbuf}")


if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_reduce.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)
