import time

import ray

"""
max_readers sound like it can have up to 2 readers, but it
actually means if you don't read it twice, it cannot upgrade the version and
write a new value.

We should either change the name (if it is expected) or fix it if it is a bug.
"""
# Basic case
ray.init(num_cpus=1)
ref = ray.put(b"000000000000", max_readers=2)
val = ray.get(ref)
print(val)
assert val == b"000000000000"
ray.release(ref)
ray.worker.global_worker.put_object(b"1", object_ref=ref, max_readers=2)
# hangs
val = ray.get(ref)
print(val)
