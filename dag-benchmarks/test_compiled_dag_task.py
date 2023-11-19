import time

import ray


@ray.remote
class A:
    def __init__(self):
        self.i = 0

    def f(self, input_obj, input_ref, output_ref):
        print("worker iteration", self.i, input_obj, input_ref, output_ref)
        self.i += 1
        ray.worker.global_worker.put_object(
            b"world", object_ref=output_ref[0], max_readers=1
        )
        ray.release(input_ref[0])


a = A.remote()

in_ref = ray.put(b"hello", max_readers=1)
out_ref = ray.put(b"world", max_readers=1)
pins = ray.get([in_ref, out_ref])

a.f.options(_is_compiled_dag_task=True).remote(in_ref, [in_ref], [out_ref])

for i in range(10):
    print("driver iteration", i, "start")
    ray.worker.global_worker.put_object(b"hello", object_ref=in_ref, max_readers=1)
    print("driver iteration", i, "output", ray.get(out_ref))
    #ray.release(out_ref)  # todo crashes

time.sleep(1)
