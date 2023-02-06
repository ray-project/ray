Ray Core API
============

Python API
----------

.. _ray-get_gpu_ids-ref:

ray.get_gpu_ids
~~~~~~~~~~~~~~~

.. autofunction:: ray.get_gpu_ids

.. _ray-actor-pool-ref:

ray.util.ActorPool
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.ActorPool
   :members:

ray.util.queue.Queue
~~~~~~~~~~~~~~~~~~~~

.. _ray-queue-ref:

.. autoclass:: ray.util.queue.Queue
   :members:

.. _ray-nodes-ref:

ray.nodes
~~~~~~~~~

.. autofunction:: ray.nodes

.. _ray-timeline-ref:

ray.timeline
~~~~~~~~~~~~

.. autofunction:: ray.timeline

.. _ray-cluster_resources-ref:

ray.cluster_resources
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.cluster_resources

.. _ray-available_resources-ref:

ray.available_resources
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.available_resources

ray.cross_language
~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.cross_language.java_function

.. autofunction:: ray.cross_language.java_actor_class

.. _custom-metric-api-ref:

Custom Metrics APIs
-------------------

Counter
~~~~~~~

.. autoclass:: ray.util.metrics.Counter
   :members:

Gauge
~~~~~

.. autoclass:: ray.util.metrics.Gauge
   :members:

Histogram
~~~~~~~~~

.. autoclass:: ray.util.metrics.Histogram
   :members:

.. _runtime-context-apis:

Runtime Context APIs
--------------------

.. autofunction:: ray.runtime_context.get_runtime_context

.. autoclass:: ray.runtime_context.RuntimeContext
    :members:

.. _package-ref-debugging-apis:

Debugging APIs
--------------

.. autofunction:: ray.util.pdb.set_trace

.. autofunction:: ray.util.inspect_serializability

