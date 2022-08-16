.. _task-patterns:

Task Design Patterns
====================

This section is a collection of common design patterns (and anti-patterns) for Ray tasks. It is meant as a handbook for both:

- New users trying to understand how to get started with Ray, and
- Advanced users trying to optimize their use of Ray tasks

You may also be interested in visiting the design patterns section for :ref:`actors <actor-patterns>`.

.. toctree::
    :maxdepth: -1

    tree-of-tasks
    map-reduce
    limit-tasks
    closure-capture
    fine-grained-tasks
    global-variables
    ray-get-loop
    submission-order
    too-many-results
    redefine-task-actor-loop
    unnecessary-ray-get
