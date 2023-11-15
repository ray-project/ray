import ray
import numpy as np
import time

ray.init()
arr = np.random.rand(1)
ref = ray.put(arr)
#assert np.array_equal(ray.get(ref), arr)
print("starting...")


@ray.remote
class Reader:
    def __init__(self, refs):
        self.ref = refs[0]

    def read(self):
        while True:
            arr = ray.get(self.ref)
            # do something.
            print(arr[0])

            # Signal to writer that they can write again.
            ray.release(self.ref)


print("OBJECT REF IS", ref)

pinned = ray.get(ref)

#reader = Reader.remote([ref])

for _ in range(10):
    start = time.time()
    for i in range(10_000):
        arr[0] = i
        ray.worker.global_worker.put_object(arr, object_ref=ref)
        #assert ray.get(ref)[0] == i
    end = time.time()
    print(f"done, tput: {10_000 / (end - start)} puts/s")
