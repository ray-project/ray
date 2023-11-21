import ray
ray.init()
ref = ray.put(b"000000000", max_readers=1)
val = ray.get(ref)
print(val)
assert val == b"000000000"
ray.release(ref)
ray.worker.global_worker.put_object(
    b"world", object_ref=ref, max_readers=1
)
val = ray.get(ref)
assert val == b"world", val
print(val)
