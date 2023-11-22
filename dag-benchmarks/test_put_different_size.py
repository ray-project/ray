import ray

# Basic case
ray.init()
ref = ray.put(b"000000000", max_readers=1)
val = ray.get(ref)
print(val)
assert val == b"000000000"
ray.release(ref)

ray.worker.global_worker.put_object(b"world", object_ref=ref, max_readers=1)
val = ray.get(ref)
assert val == b"world", val
print(val)
ray.release(ref)

# Same size
ray.worker.global_worker.put_object(b"world", object_ref=ref, max_readers=1)
val = ray.get(ref)
assert val == b"world", val
print(val)
ray.release(ref)

# Bigger size
ray.worker.global_worker.put_object(b"world1", object_ref=ref, max_readers=1)
val = ray.get(ref)
assert val == b"world1", val
print(val)
ray.release(ref)

# Repeat
for i in range(100):
    result = b"0" * (i % 8)
    ray.worker.global_worker.put_object(result, object_ref=ref, max_readers=1)
    val = ray.get(ref)
    assert val == result, val
    print(val)
    ray.release(ref)
