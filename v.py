import os
import ray
import numpy as np
import time

# ray.init(num_cpus=1)

# @ray.remote
# def f():
#     for _ in range(5):
#         import time
#         yield np.random.rand(5 * 1024 * 1024)
#         time.sleep(1)

# @ray.remote
# class A:
#     def f(self):
#         for _ in range(5):
#             import time
#             time.sleep(10)
#             print("Executed..")
#             # yield np.random.rand(5 * 1024 * 1024)
#             yield np.random.rand(5 * 1024 * 1024)
#             print("Done...")

# # g = f.options(num_returns="dynamic").remote()
# def _check_refcounts():
#     actual = ray._private.worker.global_worker.core_worker.get_all_reference_counts()
#     print(actual)
# # for i in g:
# #     print("1")
# #     print(ray.get(i))
# #     del i

# # print("Task succeeded!")
# a = A.remote()

# g = a.f.options(num_returns="dynamic").remote()
# print(g)
# for i in g:
#     print(ray.get(i))
#     del i
# print("Actor succeeded!")
from ray.cluster_utils import Cluster
def test_generator_dist_chain():
    cluster = Cluster()
    cluster.add_node(num_cpus=4, object_store_memory=1 * 1024 * 1024 * 1024)
    ray.init()
    # cluster.add_node(num_cpus=1)
    # cluster.add_node(num_cpus=1)
    # cluster.add_node(num_cpus=1)
    # cluster.add_node(num_cpus=1)

    @ray.remote
    class ChainActor:
        def __init__(self, i, child=None):
            self.child = child
            print("starto, ", i)

        def get_data(self):
            print("executed, ", os.getpid())
            if not self.child:
                while True:
                    print("Children haha.")
                    print("parent haha", os.getpid())
                    time.sleep(2)
                    yield np.ones(5 * 1024 * 1024)
            else:
                print("send data")
                for i in self.child.get_data.options(
                    num_returns="dynamic"
                ).remote():
                    print("parent haha", os.getpid())
                    r, _ = ray.wait([i], timeout=0)
                    assert len(r) > 0
                    yield ray.get(i)
                    del i

    chain_actor = ChainActor.remote(0)
    chain_actor_2 = ChainActor.remote(1, chain_actor)
    chain_actor_3 = ChainActor.remote(2, chain_actor_2)
    chain_actor_4 = ChainActor.remote(3, chain_actor_3)
    ray.get([chain_actor_4.__ray_ready__.remote(), chain_actor_3.__ray_ready__.remote(),chain_actor_2.__ray_ready__.remote(),chain_actor.__ray_ready__.remote()])

    s = time.time()
    for i in chain_actor_4.get_data.options(num_returns="dynamic").remote():
        print("top level")
        print(i)
        print(ray.get(i))
        # assert np.array_equal(np.ones(5 * 1024 * 1024), ray.get(i))
        del i
        print("Takes ", time.time() -  s)
    summary = ray._private.internal_api.memory_summary(stats_only=True)
    assert "Spilled" not in summary, summary


def test_generator_dist_all_gather():
    cluster = Cluster()
    cluster.add_node(num_cpus=0, object_store_memory=1 * 1024 * 1024 * 1024)
    ray.init()
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self, child=None):
            self.child = child

        def get_data(self):
            for _ in range(10):
                time.sleep(0.1)
                yield np.ones(5 * 1024 * 1024)

    async def all_gather():
        actor = Actor.remote()
        async for i in actor.get_data.options(num_returns="dynamic").remote():
            assert np.array_equal(np.ones(5 * 1024 * 1024), i)

    async def main():
        await asyncio.gather(all_gather(), all_gather(), all_gather(), all_gather())

    asyncio.run(main())
    summary = ray._private.internal_api.memory_summary(stats_only=True)
    assert "Spilled" not in summary, summary

test_generator_dist_chain()


# @ray.remote
# def f():
#     while True:
#         print("f streamed")
#         yield np.random.rand(5 * 1024 * 1024)
#         time.sleep(1)

# @ray.remote
# def g():
#     for i in f.options(num_returns="dynamic").remote():
#         print("g streamed")
#         yield ray.get(i)
#         del i

# s = time.time()
# for i in g.options(num_returns="dynamic").remote():
#     print("top level streamed")
#     print(ray.get(i))
#     print("took, ", time.time() - s)
#     s = time.time()
