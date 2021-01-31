import pygloo
import numpy as np
import os
import ray
import time
import shutil
import torch

@ray.remote(num_cpus=1)
def test_scatter(rank, world_size, fileStore_path):
    '''
    rank  # Rank of this process within list of participating processes
    world_size  # Number of participating processes

    BUG:
        The non-root process recvbuf will recieves un-completedd data.
        Example:

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

    sendbuf = [np.array([[1,2,3],[1,2,3]], dtype=np.float32)]*world_size
    recvbuf = np.zeros((2, 3), dtype=np.float32)
    sendptr = []
    for i in sendbuf:
        sendptr.append(i.ctypes.data)
    recvptr = recvbuf.ctypes.data

    # sendbuf = [torch.Tensor([[1,2,3],[1,2,3]]).float()]*world_size
    # recvbuf = torch.zeros_like(sendbuf)
    # sendptr = []
    # for i in sendbuf:
    #     sendptr.append(i.data_ptr())
    # recvptr = recvbuf.data_ptr()

    data_size = sendbuf[0].size if isinstance(sendbuf[0], np.ndarray) else sendbuf[0].numpy().size
    datatype = pygloo.glooDataType_t.glooFloat32
    root = 0

    pygloo.scatter(context, sendptr, recvptr, data_size, datatype,  root)

    print(f"rank {rank} sends {sendbuf}, receives {recvbuf}")
    ## example output, root is 0.
    # (pid=18951) rank 1 sends [array([[1., 2., 3.],
    # (pid=18951)        [1., 2., 3.]], dtype=float32), array([[1., 2., 3.],
    # (pid=18951)        [1., 2., 3.]], dtype=float32)], receives [[1. 2. 3.]
    # (pid=18951)  [1. 2. 3.]]
    # (pid=18952) rank 0 sends [array([[1., 2., 3.],
    # (pid=18952)        [1., 2., 3.]], dtype=float32), array([[1., 2., 3.],
    # (pid=18952)        [1., 2., 3.]], dtype=float32)], receives [[1. 2. 3.]
    # (pid=18952)  [1. 2. 3.]]

if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_scatter.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)
