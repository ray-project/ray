Anti-pattern: Closure capturing large objects harms performance
===============================================================

**TLDR:** Avoid closure capturing large objects in remote functions or classes, use object store instead.

When you define a :ref:`ray.remote <ray-remote-ref>` function or class,
it is easy to accidentally capture large (more than a few MB) objects implicitly in the definition.
This can lead to slow performance or even OOM since Ray is not designed to handle serialized functions or classes that are very large.

For such large objects, there are two options to resolve this problem:

- Use :ref:`ray.put() <ray-put-ref>` to put the large objects in the Ray object store, and then pass object references as arguments to the remote functions or classes (*"better approach #1"* below)
- Create the large objects inside the remote functions or classes by passing a lambda method (*"better approach #2"*). This is also the only option for using unserializable objects.


Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_closure_capture_large_objects.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach #1:**

.. literalinclude:: ../doc_code/anti_pattern_closure_capture_large_objects.py
    :language: python
    :start-after: __better_approach_1_start__
    :end-before: __better_approach_1_end__

**Better approach #2:**

.. literalinclude:: ../doc_code/anti_pattern_closure_capture_large_objects.py
    :language: python
    :start-after: __better_approach_2_start__
    :end-before: __better_approach_2_end__
