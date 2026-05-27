.. _ray-out-of-band-object-ref-serialization:

Anti-pattern: Serialize ray.ObjectRef out of band
=================================================

**TLDR:** Avoid serializing ``ray.ObjectRef`` because Ray can't know when to garbage collect the underlying object.

Ray's ``ray.ObjectRef`` is distributed reference counted. Ray pins the underlying object until the reference isn't used by the system anymore.
When all references to the pinned object are gone, Ray garbage collects the pinned object and cleans it up from the system.
However, if user code serializes ``ray.ObjectRef``, Ray can't keep track of the reference.

To avoid incorrect behavior, if ``ray.cloudpickle`` serializes ``ray.ObjectRef``, Ray pins the object for the lifetime of a worker. "Pin" means that object can't be evicted from the object store
until the corresponding owner worker dies. It's prone to Ray object leaks, which can lead to disk spilling. See :ref:`this page <serialize-object-ref>` for more details.

To detect if this pattern exists in your code, you can set an environment variable ``RAY_allow_out_of_band_object_ref_serialization=0``. If Ray detects
that ``ray.cloudpickle`` serialized ``ray.ObjectRef``, it raises an exception with helpful messages.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_out_of_band_object_ref_serialization.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

Anti-pattern: round-trip an ``ObjectRef`` through its hex string
----------------------------------------------------------------

A common variant of this anti-pattern is to call ``ObjectRef.hex()``, send the
string somewhere out of band (a remote task argument, a database row, a Redis
key, an HTTP request), and reconstruct the reference on the other side with
``ray.ObjectRef(bytes.fromhex(...))``.

The hex form is just bytes; it carries no reference count. From Ray's
perspective the reference disappeared the moment it was converted to a string,
so the underlying object becomes eligible for garbage collection. By the time
the consumer rebuilds the ``ObjectRef`` and calls ``ray.wait`` or ``ray.get``
on it, the object is gone. ``ray.wait`` then returns the ref in the *not
ready* list forever (until the timeout fires), which surfaces as "``ray.wait``
is broken".

**Recommended pattern:** pass the ``ObjectRef`` itself as a task argument or
return value. Ray serializes ``ObjectRef`` arguments specially and keeps the
distributed reference count correct end to end. Wrap a single ref in a list
(``f.remote([obj_ref])``) if your task signature expects a collection.

.. literalinclude:: ../doc_code/anti_pattern_out_of_band_object_ref_serialization_hex.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__
