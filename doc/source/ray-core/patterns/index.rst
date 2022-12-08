.. _core-patterns:

Design Patterns & Anti-patterns
===============================

This section is a collection of common design patterns and anti-patterns for writing Ray applications.

ray.get() related
-----------------

.. toctree::
    :maxdepth: 1

    ray-get-loop
    unnecessary-ray-get
    ray-get-submission-order
    ray-get-too-many-objects

others
------

.. toctree::
    :maxdepth: 1

    nested-tasks
    generators
    limit-pending-tasks
    limit-running-tasks
    actor-sync
    tree-of-actors
    pipelining
    return-ray-put
    too-fine-grained-tasks
    redefine-task-actor-loop
    pass-large-arg-by-value
    closure-capture-large-objects
    global-variables
