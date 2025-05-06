.. _ray-out-of-band-object-ref-serialization:

Anti-pattern: Serialize ray.ObjectRef out of band
=================================================

**TLDR:** Avoid serializing ``ray.ObjectRef`` because Ray can't know when to garbage collect the underlying object.

Ray's ``ray.ObjectRef`` is distributed reference counted. Ray pins the underlying object until the reference isn't used by the system anymore.
When all references are the pinned object gone,  Ray garbage collects the pinned object and cleans it up from the system.
However, if user code serializes ``ray.objectRef``, Ray can't keep track of the reference.

To avoid incorrect behavior, if ``ray.cloudpickle`` serializes``ray.ObjectRef``, Ray pins the object for the lifetime of a worker. "Pin" means that object can't be evicted from the object store
until the corresponding owner worker dies. It's prone to Ray object leaks, which can lead disk spilling. See :ref:`thjs page <serialize-object-ref>` for more details.

To detect if this pattern exists in your code, you can set an environment variable ``RAY_allow_out_of_band_object_ref_serialization=0``. If Ray detects
that ``ray.cloudpickle`` serialized``ray.ObjectRef``, it raises an exception with helpful messages.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_out_of_band_object_ref_serialization.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__
