import ray
import numpy as np

ray.init(num_cpus=1)

@ray.remote
def f():
    for _ in range(5):
        import time
        yield np.random.rand(5 * 1024 * 1024)
        time.sleep(1)

@ray.remote
class A:
    def f(self):
        for _ in range(5):
            import time
            time.sleep(1)
            print("Executed..")
            # yield np.random.rand(5 * 1024 * 1024)
            yield 1
            print("Done...")

# g = f.options(num_returns="dynamic").remote()
def _check_refcounts():
    actual = ray._private.worker.global_worker.core_worker.get_all_reference_counts()
    print(actual)
# for i in g:
#     print("1")
#     print(ray.get(i))
#     del i

# print("Task succeeded!")
a = A.remote()

g = a.f.options(num_returns="dynamic").remote()
print(g)
for i in g:
    print(ray.get(i))
    del i
print("Actor succeeded!")
