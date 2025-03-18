.. _workflows:

Ray Workflows: Durable Ray Task Graphs
======================================

.. toctree::
    :hidden:

    key-concepts
    basics
    management
    metadata
    events
    comparison
    advanced
    api/api

.. warning::

  The experimental Ray Workflows library has been deprecated and will be removed in a
  future version of Ray.

Ray Workflows implements high-performance, *durable* application workflows using
Ray tasks as the underlying execution engine. It enables task-based Ray jobs
to seamlessly resume execution even in the case of entire-cluster failure.

Why Ray Workflows?
------------------

**Flexibility:** Combine the flexibility of Ray's dynamic task graphs with
strong durability guarantees. Branch or loop conditionally based on runtime
data. Use Ray distributed libraries seamlessly within workflow tasks.

**Performance:** Ray Workflows offers sub-second overheads for task launch and
supports workflows with hundreds of thousands of tasks. Take advantage of the
Ray object store to pass distributed datasets between tasks with zero-copy
overhead.

You might find that Ray Workflows is *lower level* compared to engines such as
`AirFlow <https://www.astronomer.io/blog/airflow-ray-data-science-story>`__
(which can also run on Ray). This is because Ray Workflows focuses more on core
durability primitives as opposed to tools and integrations.
