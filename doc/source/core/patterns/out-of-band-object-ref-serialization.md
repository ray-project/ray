---
myst:
  html_meta:
    description: "Anti-pattern: serializing an ObjectRef out of band escapes reference counting, so Ray pins its value and can leak it."
---

(ray-out-of-band-object-ref-serialization)=

# Anti-pattern: Serializing ray.ObjectRef out of band

Avoid serializing `ray.ObjectRef` because Ray can't know when to garbage collect the underlying object.

Ray uses distributed reference counting for `ray.ObjectRef`. Ray pins the underlying object until the system no longer uses the reference. When no references to the pinned object remain, Ray garbage collects the object and cleans it up from the system. However, if your code serializes a `ray.ObjectRef`, Ray can't keep track of the reference.

Out-of-band serialization can happen through any channel that strips the `ObjectRef` of its in-process semantics: `pickle` and `ray.cloudpickle`, or string forms that `ObjectRef.hex()` produces and `ray.ObjectRef(bytes.fromhex(...))` reconstructs. Both routes have the same root problem and the same recommended fix: pass the `ObjectRef` itself as a remote task argument or return value, so Ray sees the reference and keeps the distributed reference count correct end to end.

To avoid incorrect behavior, if `ray.cloudpickle` serializes a `ray.ObjectRef`, Ray pins the object for the lifetime of the worker that serialized it. A pinned object can't be evicted from the object store until that worker dies or the object's owner dies. This approach is prone to Ray object leaks, which can lead to disk spilling.

To detect this pattern in your code, set the environment variable `RAY_allow_out_of_band_object_ref_serialization=0`. If Ray detects that `ray.cloudpickle` serialized a `ray.ObjectRef`, it raises an exception with helpful messages.

## Code example: pickle and cloudpickle

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_out_of_band_object_ref_serialization.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

## Code example: hex string round-trip

A common variant is to call `ObjectRef.hex()`, send the string somewhere out of band, such as a remote task argument, a database row, a Redis key, or an HTTP request, and reconstruct the reference on the other side with `ray.ObjectRef(bytes.fromhex(...))`.

The hex form carries no reference count. From Ray's perspective, the reference disappears the moment your code converts it to a string, so the underlying object becomes eligible for garbage collection. By the time the consumer rebuilds the `ObjectRef` and calls `ray.wait` or `ray.get`, the object might be gone. `ray.wait` then keeps the ref in the *not ready* list until the timeout fires, which looks like `ray.wait` never completing.

**Recommended pattern:** pass the `ObjectRef` as a task argument or return value so Ray's distributed reference counter tracks it and keeps the underlying object pinned for the duration of the task. Ray auto-dereferences a *top-level* `ObjectRef` argument, so the task receives the underlying value rather than the reference. When the task still needs the reference itself, for example to reconstruct it from hex on the consumer side, pass the live `ObjectRef` nested inside a container, as in `f.remote([obj_ref])`. Ray then registers the task as a borrower and keeps the object resolvable even after the caller drops its own copy of the reference.

```{literalinclude} ../doc_code/anti_pattern_out_of_band_object_ref_serialization_hex.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```
