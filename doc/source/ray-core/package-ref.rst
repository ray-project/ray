Ray Core API
============

Python API
----------

.. _ray-is_initialized-ref:

ray.is_initialized
~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.is_initialized

.. _ray-get_actor-ref:


ray.get_actor
~~~~~~~~~~~~~~~

.. autofunction:: ray.get_actor

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

.. _ray-core-exceptions:

Exceptions
----------

.. autoclass:: ray.exceptions.TaskCancelledError
   :members:
.. autoclass:: ray.exceptions.GetTimeoutError
   :members:
.. _ray-core-exceptions-runtime-env-setup-error:
.. autoclass:: ray.exceptions.RuntimeEnvSetupError
   :members:
.. autoclass:: ray.exceptions.TaskUnschedulableError
   :members:
.. autoclass:: ray.exceptions.ActorUnschedulableError
   :members:
.. autoclass:: ray.exceptions.TaskPlacementGroupRemoved
   :members:
.. autoclass:: ray.exceptions.ActorPlacementGroupRemoved
   :members:
.. autoclass:: ray.exceptions.LocalRayletDiedError
   :members:
.. autoclass:: ray.exceptions.WorkerCrashedError
   :members:
.. autoclass:: ray.exceptions.RaySystemError
   :members:
.. autoclass:: ray.exceptions.ObjectStoreFullError
   :members:
.. autoclass:: ray.exceptions.OutOfDiskError
   :members:
.. _ray-core-exceptions-object-lost-error:
.. autoclass:: ray.exceptions.ObjectLostError
   :members:
.. autoclass:: ray.exceptions.ObjectFetchTimedOutError
   :members:
.. autoclass:: ray.exceptions.OwnerDiedError
   :members:
.. autoclass:: ray.exceptions.ObjectReconstructionFailedError
   :members:
.. autoclass:: ray.exceptions.ObjectReconstructionFailedMaxAttemptsExceededError
   :members:
.. autoclass:: ray.exceptions.ObjectReconstructionFailedLineageEvictedError
   :members:
.. autoclass:: ray.exceptions.PlasmaObjectNotAvailable
   :members:
.. autoclass:: ray.exceptions.AsyncioActorExit
   :members:
.. autoclass:: ray.exceptions.CrossLanguageError
   :members:
