import ray
import numpy as np
import time

ray.init()


@ray.remote
class Reader:
    def __init__(self, refs):
        self.ref = refs[0]

    def read(self, use_bytes):
        for i in range(10_000):
            arr = ray.get(self.ref)
            #arr = worker.core_worker.get_if_local(object_refs)
            if use_bytes:
                assert int.from_bytes(arr, "little") == i
            else:
                print("remote", arr[0])
                assert arr[0] == i

            # Signal to writer that they can write again.
            ray.release(self.ref)


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

    remote_read_done = None
    if reuse_object_ref:
        # Keep the plasma object pinned.
        # TODO(swang): Pin the object properly in plasma store.
        pinned = ray.get(ref)
        print("Object ref:", ref)

        if read_remote:
            reader = Reader.remote([ref])
            remote_read_done = reader.read.remote(use_bytes)
    else:
        assert not read_remote

    ray.release(ref)
    print("starting...")

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
                if use_bytes:
                    assert int.from_bytes(ray.get(ref), "little") == i
                else:
                    assert ray.get(ref)[0] == i
                ray.release(ref)
        end = time.time()
        print(f"done, tput: {10_000 / (end - start)} puts/s")

    if remote_read_done is not None:
        ray.get(remote_read_done)


if __name__ == "__main__":
    run_local = False

    if not run_local:
        remote_run = ray.remote(run)
        def run_fn(*args, **kwargs):
            return ray.get(remote_run.remote(*args, **kwargs))
        run = run_fn

    #print("Dynamic ray.put")
    #ray.get(run.remote())

    #print("Reuse ray.put buffer")
    #ray.get(run.remote(reuse_object_ref=True))

    run(use_bytes=False, reuse_object_ref=True, read_remote=False, read_local=True)
