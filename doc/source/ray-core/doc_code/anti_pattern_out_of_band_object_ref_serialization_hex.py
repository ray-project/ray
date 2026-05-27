# __anti_pattern_start__
import ray

ray.init()


@ray.remote
class Producer:
    def make(self):
        # Returns an ObjectRef. The producer task's result IS the ObjectRef
        # the caller will eventually consume.
        return ray.put(42)


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
def consumer_correct(obj_ref, timeout: float):
    # Recommended pattern: pass the ObjectRef directly as a task argument.
    # Ray sees the reference, keeps the underlying object pinned, and ray.wait
    # behaves as documented.
    ready, not_ready = ray.wait([obj_ref], timeout=timeout, fetch_local=False)
    return len(ready), len(not_ready)


# Anti-pattern in action: the producer's ObjectRef is serialized to a hex
# string and passed by value, so Ray loses track of it.
producer = Producer.remote()
inner_ref = ray.get(producer.make.remote())
broken_result = ray.get(consumer_broken.remote(inner_ref.hex(), 5.0))
# Likely (0, 1) -- the object was unreachable from the consumer's perspective.
print(f"broken: ready={broken_result[0]}, not_ready={broken_result[1]}")

# Correct usage: pass the ObjectRef itself. Ray tracks the reference end to end.
correct_result = ray.get(consumer_correct.remote(inner_ref, 5.0))
# Expect (1, 0) -- the object is ready.
print(f"correct: ready={correct_result[0]}, not_ready={correct_result[1]}")
# __anti_pattern_end__
