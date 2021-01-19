import pygloo
import numpy as np
import os
import ray

@ray.remote(num_cpus=1)
def test1(rank, world_size):
    '''
    myRank = 0;  # Rank of this process within list of participating processes
    contextSize = 2;  # Number of participating processes
    '''
    data = np.ones((1,2))

    context = pygloo.rendezvous.Context(rank, world_size);

    # help(pygloo.transport.tcp.attr)

    attr = pygloo.transport.tcp.attr("localhost")
    # Perform rendezvous for TCP pairs
    dev = pygloo.transport.tcp.CreateDevice(attr)
    fileStore = pygloo.rendezvous.FileStore("/data3/huangrunhui/proj2/gloo_pybind/pygloo/tmp")
    # redis = pygloo.RedisStore("redishost")
    # prefix = os.getenv("PREFIX")
    store = pygloo.rendezvous.PrefixStore(str(world_size), fileStore)
    print(store)
    print(dev)
    context.connectFullMesh(store, dev)
    print(rank)

    pygloo.allreduce(data)


if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2
    fns = [test1.remote(i, world_size) for i in range(world_size)]

    ray.get(fns)

