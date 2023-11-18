import ray


@ray.remote
class A:
    def f(self, input_ref, output_ref):
        print("execution invoked")
        ray.worker.global_worker.put_object(
            b"world", object_ref=output_ref[0], max_readers=1
        )
        ray.release(input_ref)


a = A.remote()

in_ref = ray.put(b"hello")
out_ref = ray.put(b"world")
pins = ray.get([in_ref, out_ref])

a.f.options(_is_compiled_dag_task=True).remote(ref, [out_ref])

for _ in range(10):
    ray.worker.global_worker.put_object(b"hello", object_ref=ref, max_readers=1)
    print("Output", ray.get(out_ref))
    ray.release(out_ref)
