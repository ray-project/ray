.. _ray-out-of-band-object-ref-serialization:

Anti-pattern: Serialize ray.ObjectRef out of band
=================================================

**TLDR:** Avoid serialize ``ray.ObjectRef`` because Ray cannot know when the GC the underlying object.

Ray's ``ray.ObjectRef`` is distributed reference counted. Ray pins the underlying object until the reference is not used by the system anymore.
When all references are gone, the pinned object is garbage collected and cleaned up from the system.
However, if user code serializes ``ray.objectRef``, Ray cannot keep track of the reference.

To avoid incorrect behavior, if ``ray.ObjectRef`` is serialized by ``ray.cloudpickle``, Ray pins the object for the lifetime of a worker. "pin" means that object cannot be evicted from the object store
until the corresponding worker dies. It is prone to Ray object leaks, which can lead disk spilling.

To detect if this pattern exists in your code, you can set an environment variable ``RAY_allow_out_of_band_object_ref_serialization=0``. If Ray detects
``ray.ObjectRef`` is serialized by ``ray.cloudpickle``, it raises an exception with helpful messages.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_out_of_band_object_ref_serialization.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__
