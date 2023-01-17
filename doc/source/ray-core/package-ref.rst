Ray Core API
============

Python API
----------

.. _ray-init-ref:

ray.init
~~~~~~~~

.. autofunction:: ray.init

.. _ray-is_initialized-ref:

ray.is_initialized
~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.is_initialized

.. _ray-remote-ref:

ray.remote
~~~~~~~~~~

.. autofunction:: ray.remote

.. _ray-options-ref:

.. autofunction:: ray.remote_function.RemoteFunction.options

.. autofunction:: ray.actor.ActorClass.options

.. _scheduling-strategy-ref:

.. autofunction:: ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy

.. autofunction:: ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy

.. _ray-get-ref:

ray.get
~~~~~~~

.. autofunction:: ray.get

.. _ray-wait-ref:

ray.wait
~~~~~~~~

.. autofunction:: ray.wait

.. _ray-put-ref:

ray.put
~~~~~~~

.. autofunction:: ray.put

.. _ray-kill-ref:

ray.kill
~~~~~~~~

.. autofunction:: ray.kill

.. _ray-cancel-ref:

ray.cancel
~~~~~~~~~~

.. autofunction:: ray.cancel

.. _ray-get_actor-ref:


ray.get_actor
~~~~~~~~~~~~~~~

.. autofunction:: ray.get_actor

.. _ray-get_gpu_ids-ref:

ray.get_gpu_ids
~~~~~~~~~~~~~~~

.. autofunction:: ray.get_gpu_ids

.. _ray-shutdown-ref:

ray.shutdown
~~~~~~~~~~~~

.. autofunction:: ray.shutdown

.. _ray-method-ref:

ray.method
~~~~~~~~~~

.. autofunction:: ray.method

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

.. _ray-placement-group-ref:

Placement Group APIs
--------------------

placement_group
~~~~~~~~~~~~~~~

.. autofunction:: ray.util.placement_group.placement_group


PlacementGroup (class)
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.placement_group.PlacementGroup
   :members:

placement_group_table
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.util.placement_group.placement_group_table


remove_placement_group
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.util.placement_group.remove_placement_group

get_current_placement_group
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.util.placement_group.get_current_placement_group

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

.. _runtime-env-apis:

Runtime Env APIs
----------------

.. autoclass:: ray.runtime_env.RuntimeEnvConfig
    :members:

.. autoclass:: ray.runtime_env.RuntimeEnv
    :members:

.. _package-ref-debugging-apis:

Debugging APIs
--------------

.. autofunction:: ray.util.pdb.set_trace

.. autofunction:: ray.util.inspect_serializability

.. _ray-core-exceptions:

Exceptions
----------

.. autoclass:: ray.exceptions.RayError
   :members:

.. _ray-core-exceptions-ray-task-error:
.. autoclass:: ray.exceptions.RayTaskError
   :members:
.. autoclass:: ray.exceptions.TaskCancelledError
   :members:
.. autoclass:: ray.exceptions.GetTimeoutError
   :members:
.. _ray-core-exceptions-ray-actor-error:
.. autoclass:: ray.exceptions.RayActorError
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

RayExecutor
-----------

.. autoclass:: ray.util.RayExecutor
   :members:
