import ray
import numpy as np
import time

ray.init()


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
            #ray.release(self.ref)


@ray.remote
def run(num_trials=3, use_bytes=True, reuse_object_ref=False, read_local=False, read_remote=False):
    if use_bytes:
        arr = b"binary"
    else:
        arr = np.random.rand(1)

    ref = ray.put(arr)

    if use_bytes:
        assert ray.get(ref) == arr
    else:
        assert np.array_equal(ray.get(ref), arr)
    print("starting...")

    if reuse_object_ref:
        # Keep the plasma object pinned.
        # TODO(swang): Pin the object properly in plasma store.
        pinned = ray.get(ref)
        print("Object ref:", ref)

        if read_remote:
            reader = Reader.remote([ref])
            reader.read.remote()
    else:
        assert not read_remote

    for _ in range(num_trials):
        start = time.time()
        for i in range(10_000):
            if use_bytes:
                arr = i.to_bytes(8, "little")
            else:
                arr[0] = i

            if reuse_object_ref:
                ray.worker.global_worker.put_object(arr, object_ref=ref)
            else:
                ref = ray.put(arr)
            if read_local:
                assert ray.get(ref)[0] == i
        end = time.time()
        print(f"done, tput: {10_000 / (end - start)} puts/s")


if __name__ == "__main__":
    print("Dynamic ray.put")
    ray.get(run.remote())

    print("Reuse ray.put buffer")
    ray.get(run.remote(reuse_object_ref=True))
