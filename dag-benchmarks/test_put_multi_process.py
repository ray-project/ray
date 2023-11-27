import ray
import time

"""
Verify put in 2 different processes work.
"""
ray.init(num_cpus=1)

print("Test Write input from driver -> Read & Write from worker -> Read output from driver")
expected_input = b"000000000000"
ref = ray.put(expected_input, max_readers=1)
print(ref)

@ray.remote
class A:
    def f(self, refs, expected_input, output_val):
        ref = refs[0]
        val = ray.get(refs[0])
        assert val == expected_input, val
        ray.release(ref)
        ray.worker.global_worker.put_object(output_val, object_ref=ref, max_readers=1)


a = A.remote()
time.sleep(1)
output_val = b"0"
b = a.f.remote([ref], expected_input, output_val)
ray.get(b)
val = ray.get(ref)
assert output_val == val
ray.release(ref)

print("Test Write input from driver twice -> Read & Write from worker -> Read output from driver")
# Test write twice.
ref = ray.put(b"000000000000", max_readers=1)
assert b"000000000000" == ray.get(ref)
ray.release(ref)
print(ref)
expected_input = b"1"
ray.worker.global_worker.put_object(b"1", object_ref=ref, max_readers=1)

a = A.remote()
time.sleep(1)
expected_output = b"23"
b = a.f.remote([ref], expected_input, expected_output)
ray.get(b)
val = ray.get(ref)
assert expected_output == val
ray.release(ref)