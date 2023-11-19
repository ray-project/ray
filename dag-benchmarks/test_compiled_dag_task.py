import time

import ray


@ray.remote
class A:
    def __init__(self, input_ref, output_ref):
        self.input_ref = input_ref[0]
        self.output_ref = output_ref[0]
        self.pins = ray.get([self.input_ref, self.output_ref])
        self.i = 0

    def f(self, input_obj):
        print("worker iteration", self.i, input_obj)
        self.i += 1
#        ray.worker.global_worker.put_object(
#            b"world", object_ref=self.output_ref, max_readers=1
#        )
        ray.release(self.input_ref)


in_ref = ray.put(b"000000000", max_readers=1)
out_ref = ray.put(b"111111111", max_readers=1)
pins = ray.get([in_ref, out_ref])

a = A.remote([in_ref], [out_ref])
a.f.options(_is_compiled_dag_task=True).remote(in_ref)

for i in range(10):
    print("driver iteration", i, "start")
    ray.worker.global_worker.put_object(b"hello", object_ref=in_ref, max_readers=1)

#    print("driver iteration", i, "output", ray.get(out_ref))
    #ray.release(out_ref)  # todo crashes

time.sleep(5)
