import ray
import ctypes
import numpy as np
import time


def get_and_reset_ref(ref):
    # Wait for the DAG to finish.
    output_buf = ray.get(ref)
    # Reset the output ref.
    ray.worker.global_worker.core_worker.unseal_object(ref)
    return output_buf


@ray.remote
class Worker:
    def __init__(self):
        pass

    def echo(self, x):
        return x

    def run_custom_method(self, fn, *args):
        fn(self, *args)


def _method_runner(self, method_name, refs):
    method = getattr(self, method_name)
    recv_arr = ray.get(refs[0])
    send_arr = ray.get(refs[1])
    send_ptr = ctypes.c_char_p(send_arr.ctypes.data)
    i = 0
    while True:
        while i == recv_arr[0]:
            pass
        i = method(recv_arr[0])
        ctypes.memset(send_ptr, i, 1)  # pong


def _plasma_method_runner(self, method_name, refs):
    #method = getattr(self, method_name)
    while True:
        buf = get_and_reset_ref(refs[0])
        ray.worker.global_worker.put_object(buf, object_ref=refs[1])
        # Delete ref because we cannot call ray.get on an unsealed ref.
        del buf


class ScatterGather:
    def __init__(self, actors, method_name):
        self.actors = actors
        self.method_name = method_name
        self.setup()

    def setup(self):
        raise NotImplementedError

    def invoke(self, arg):
        raise NotImplementedError

    def teardown(self):
        raise NotImplementedError

    def __del__(self):
        self.teardown()


class ScatterGatherTasksImpl(ScatterGather):
    def setup(self):
        pass

    def teardown(self):
        pass

    def invoke(self, arg):
        return ray.get([
            getattr(actor, self.method_name).remote(arg)
            for actor
            in self.actors
        ])


class ScatterGatherShmImpl(ScatterGather):
    def setup(self):
        self.runner_refs = []
        self.send_ptrs = []
        self.recv_arrs = []

        for actor in self.actors:
            send_ref = ray.put(np.zeros(10, dtype=np.uint8))
            recv_ref = ray.put(np.zeros(10, dtype=np.uint8))
            send_arr = ray.get(send_ref)
            recv_arr = ray.get(recv_ref)
            self.runner_refs.append(
                actor.run_custom_method.remote(
                    _method_runner, self.method_name, [send_ref, recv_ref]))
            self.send_ptrs.append(ctypes.c_char_p(send_arr.ctypes.data))
            self.recv_arrs.append(recv_arr)

    def invoke(self, arg):
        for send_ptr in self.send_ptrs:
            ctypes.memset(send_ptr, arg, 1)
        for recv_arr in self.recv_arrs:
            while recv_arr[0] != arg:  # TODO handle non-identity function return
                pass
        return [arg] * len(self.actors)

    def teardown(self):
        # TODO cancelling actor tasks is not supported? kill them
        for a in self.actors:
            ray.kill(a)


class ScatterGatherPlasmaImpl(ScatterGather):
    def setup(self):
        self.runner_refs = []
        self.send_refs = []
        self.recv_refs = []
        self.send_bufs = []

        for actor in self.actors:
            send_ref = ray.put(np.zeros(10, dtype=np.uint8))
            recv_ref = ray.put(np.zeros(10, dtype=np.uint8))
            print(send_ref, recv_ref)
            self.send_bufs.append(get_and_reset_ref(send_ref))
            get_and_reset_ref(recv_ref)

            self.runner_refs.append(
                actor.run_custom_method.remote(
                    _plasma_method_runner, self.method_name, [send_ref, recv_ref]))

            self.send_refs.append(send_ref)
            self.recv_refs.append(recv_ref)

    def invoke(self, arg):
        for ref in self.send_refs:
            ray.worker.global_worker.put_object(
                    np.ones(10, dtype=np.uint8) * arg,
                    object_ref=ref)
        outputs = []
        for ref in self.recv_refs:
            output = get_and_reset_ref(ref)[0]
            outputs.append(output)
        return outputs

    def teardown(self):
        # TODO cancelling actor tasks is not supported? kill them
        for a in self.actors:
            ray.kill(a)

#for n in [1, 4]:
#    for cls in [ScatterGatherTasksImpl, ScatterGatherShmImpl, ScatterGatherPlasmaImpl]:
for n in [1]:
    for cls in [ScatterGatherPlasmaImpl]:
        actors = [Worker.remote() for _ in range(n)]
        impl = cls(actors, method_name="echo")
        time.sleep(2)
        i = 1

        # Warmup.
        res = impl.invoke(i)

        start = time.time()
        while time.time() - start < 5:
            z = i % 255
            res = impl.invoke(z)
            assert res == [z] * len(actors)
            i += 1
        print(n, "actors", cls.__name__, "avg latency", (time.time() - start) / i * 1e6, "us")
        del impl

