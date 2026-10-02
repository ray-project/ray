---
myst:
  html_meta:
    description: "Anti-pattern: serializing an ObjectRef out of band escapes reference counting and can let its value be reclaimed early."
---

(ray-out-of-band-object-ref-serialization)=

# Anti-pattern: Serializing ray.ObjectRef out of band

Avoid serializing `ray.ObjectRef` because Ray can't know when to garbage collect the underlying object.

Ray uses distributed reference counting for `ray.ObjectRef`. Ray pins the underlying object until the system no longer uses the reference. When no references to the pinned object remain, Ray garbage collects the object and cleans it up from the system. However, if your code serializes a `ray.ObjectRef`, Ray can't keep track of the reference.

To avoid incorrect behavior, if `ray.cloudpickle` serializes a `ray.ObjectRef`, Ray pins the object for the lifetime of a worker. A pinned object can't be evicted from the object store until its owner worker dies. This approach is prone to Ray object leaks, which can lead to disk spilling.

To detect this pattern in your code, set the environment variable `RAY_allow_out_of_band_object_ref_serialization=0`. If Ray detects that `ray.cloudpickle` serialized a `ray.ObjectRef`, it raises an exception with helpful messages.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_out_of_band_object_ref_serialization.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```
