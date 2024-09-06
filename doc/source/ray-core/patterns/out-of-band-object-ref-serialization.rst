.. _ray-out-of-band-object-ref-serialization:

Anti-pattern: Serialize ray.ObjectRef out of band
=================================================

**TLDR:** Avoid serialize ``ray.ObjectRef`` because Ray cannot know when the GC the underlying object.

Ray's ``ray.ObjectRef`` is distributed reference counted. Ray pins the underlying object until the reference is not used by the system anymore.
When all references are gone, the pinned object is garbage collected and cleaned up from the system.
However, if user code serializes ``ray.objectRef``, Ray cannot keep track of the reference.

If you have to serialize ``ray.ObjectRef``, use ``ray.cloudpickle``. Since it is not useful for most of use cases,
Ray by default raises an exception (from Ray 2.36) when an object ref is serialized with ``ray.cloudpickle``.
However, you can set an environment variable RAY_allow_out_of_band_object_ref_serialization=1 to avoid raising an exception.
In this case, the object is pinned for the lifetime of a worker instead (which is prone to Ray object leaks, which can lead disk spilling).

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_pass_large_arg_by_value.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:**

.. literalinclude:: ../doc_code/anti_pattern_pass_large_arg_by_value.py
    :language: python
    :start-after: __better_approach_start__
    :end-before: __better_approach_end__
