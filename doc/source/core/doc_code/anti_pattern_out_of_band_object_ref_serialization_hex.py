# __anti_pattern_start__
import gc

import ray

ray.init()


@ray.remote
def consumer_broken(obj_ref_hex: str, timeout: float):
    # Anti-pattern: rebuild an ObjectRef from a hex string.
    # The hex was produced out of band (e.g. via .hex()) and Ray's distributed
    # reference counting never saw it. The underlying object may have already
    # been garbage-collected, so this ray.wait can hang or return only the
    # "not ready" list forever.
    obj_ref = ray.ObjectRef(bytes.fromhex(obj_ref_hex))
    ready, not_ready = ray.wait([obj_ref], timeout=timeout, fetch_local=False)
    return len(ready), len(not_ready)


@ray.remote
def consumer_correct(obj_ref_hex: str, keep_alive, timeout: float):
    # Recommended pattern: still reconstruct from hex on the consumer side, but
    # have the caller pass the live ObjectRef alongside (here inside a list, so
    # Ray ref-counts it without auto-dereferencing the top-level argument).
    # That keeps the underlying object pinned for the duration of the task.
    obj_ref = ray.ObjectRef(bytes.fromhex(obj_ref_hex))
    ready, not_ready = ray.wait([obj_ref], timeout=timeout, fetch_local=False)
    return len(ready), len(not_ready)


# Anti-pattern in action: the driver puts the value, serializes the ObjectRef
# to a hex string, then drops the only live reference before invoking the
# consumer. Ray's distributed reference counter sees zero references, so the
# object is eligible for collection by the time the consumer rebuilds it.
inner_ref = ray.put(42)
inner_ref_hex = inner_ref.hex()
del inner_ref
gc.collect()
broken_result = ray.get(consumer_broken.remote(inner_ref_hex, 5.0))
# Likely (0, 1) -- the object was unreachable from the consumer's perspective.
print(f"broken: ready={broken_result[0]}, not_ready={broken_result[1]}")

# Correct usage: pass the hex string (so the task can reconstruct the ref) and
# also pass the live ObjectRef inside a container so Ray's ref counter keeps
# the underlying object alive for the duration of the task. Wrapping in a list
# prevents Ray from auto-dereferencing the top-level argument.
inner_ref = ray.put(42)
inner_ref_hex = inner_ref.hex()
correct_result = ray.get(
    consumer_correct.remote(inner_ref_hex, [inner_ref], 5.0)
)
# Expect (1, 0) -- the object is ready.
print(f"correct: ready={correct_result[0]}, not_ready={correct_result[1]}")
# __anti_pattern_end__
