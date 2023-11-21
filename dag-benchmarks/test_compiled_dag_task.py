import os
import time

import ray

verbose = os.environ.get("VERBOSE", "1") == "1"
fused_worker_tasks = os.environ.get("FUSED_WORKER_TASKS", "0") == "1"

# Do the work in a remote task so we can use flame-graphs in the dashboard on the main.
@ray.remote
def work():

    @ray.remote
    class A:
        def __init__(self, input_ref):
            self.input_ref = input_ref[0]
            time.sleep(1)
            # TODO: For reusable objects, it's easiest right now if we always make
            # sure to write from the same process.
            self.output_ref = ray.put(b"111111111", max_readers=1)
            ray.release(self.output_ref)

            self.i = 0

        def get_output_ref(self):
            return self.output_ref

        def f(self, input_obj):
            if verbose:
                print("worker iteration", self.i, input_obj)
            self.i += 1
            ray.worker.global_worker.put_object(
                b"world", object_ref=self.output_ref, max_readers=1
            )
            ray.release(self.input_ref)

        def f_fused(self, input_obj):
            while True:
                self.f(input_obj)
                ray.get(self.input_ref)

        def foo(self):
            print("FOO")


    in_ref = ray.put(b"000000000", max_readers=1)
# TODO(swang): Sleep to make sure that the object store sees the Seal. Should
# replace this with a better call to put reusable objects, and have the object
# store ReadRelease.
    time.sleep(1)
    ray.release(in_ref)
    print("in ref:", in_ref)

    a = A.remote([in_ref])
    out_ref = ray.get(a.get_output_ref.remote())
    print("out ref:", out_ref)

    if fused_worker_tasks:
        a.f_fused.options(_is_compiled_dag_task=True).remote(in_ref)
    else:
        a.f.options(_is_compiled_dag_task=True).remote(in_ref)

    n = 1000
    for _ in range(500):
        start = time.time()
        for i in range(n):
            if verbose:
                print("driver iteration", i, "start")
            ray.worker.global_worker.put_object(b"hello", object_ref=in_ref, max_readers=1)

            x = ray.get(out_ref)
            if verbose:
                print("driver iteration", i, "output", x)
            ray.release(out_ref)  # todo crashes
        print(n / (time.time() - start), "iterations per second")

## TODO: Test actor can also execute other tasks.
# a.foo.remote()
    time.sleep(5)

ray.get(work.remote())
