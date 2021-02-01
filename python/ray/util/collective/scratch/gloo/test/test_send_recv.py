import pygloo
import numpy as np
import os
import ray
import time
import shutil
import torch

@ray.remote(num_cpus=1)
def test_send_recv(rank, world_size, fileStore_path):
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

        # sendbuf = torch.Tensor([[1,2,3],[1,2,3]]).float()
        # sendptr = sendbuf.data_ptr()

        data_size = sendbuf.size if isinstance(sendbuf, np.ndarray) else sendbuf.numpy().size
        datatype = pygloo.glooDataType_t.glooFloat32
        peer = 1
        pygloo.send(context, sendptr, data_size, datatype, peer)
        print(f"rank {rank} sends {sendbuf}")

    elif rank == 1:
        recvbuf = np.zeros((2,3), dtype=np.float32)
        recvptr = recvbuf.ctypes.data

        # recvbuf = torch.zeros(2,3).float()
        # recvptr = recvbuf.data_ptr()

        data_size = recvbuf.size if isinstance(recvbuf, np.ndarray) else recvbuf.numpy().size
        datatype = pygloo.glooDataType_t.glooFloat32
        peer = 0

        pygloo.recv(context, recvptr, data_size, datatype, peer)
        print(f"rank {rank} receives {recvbuf}")
    else:
        raise Exception("Only support 2 process to test send function and recv function")
    ## example output


if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fileStore_path = f"{ray.worker._global_node.get_session_dir_path()}" + "/collective/gloo/rendezvous"

    fns = [test_send_recv.remote(i, world_size, fileStore_path) for i in range(world_size)]
    ray.get(fns)
