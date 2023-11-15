import ray
import numpy as np
import time

ray.init()

@ray.remote
def f():
    arr = b"binary"
    ref = ray.put(arr)
    print("starting...")

    # Keep the plasma object pinned.
    # TODO(swang): Pin the object properly in plasma store.
    pinned = ray.get(ref)

    while True:
        start = time.time()
        for i in range(100_000):
            ray.worker.global_worker.put_object(arr, object_ref=ref)
            #assert ray.get(ref)[0] == i
        end = time.time()
        print(f"done, tput: {100_000 / (end - start)} puts/s")

ray.get(f.remote())
