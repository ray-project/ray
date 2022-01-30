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

.. autofunction:: ray.remote_function.RemoteFunction.options

.. autofunction:: ray.actor.ActorClass.options

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

.. autofunction:: ray.java_function

.. autofunction:: ray.java_actor_class

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

.. _package-ref-debugging-apis:

Debugging APIs
--------------

.. autofunction:: ray.util.pdb.set_trace

.. autofunction:: ray.util.inspect_serializability

.. _ray-cli:

The Ray Command Line API
------------------------

.. _ray-start-doc:

.. click:: ray.scripts.scripts:start
   :prog: ray start
   :show-nested:

.. _ray-stop-doc:

.. click:: ray.scripts.scripts:stop
   :prog: ray stop
   :show-nested:

.. _ray-up-doc:

.. click:: ray.scripts.scripts:up
   :prog: ray up
   :show-nested:

.. _ray-down-doc:

.. click:: ray.scripts.scripts:down
   :prog: ray down
   :show-nested:

.. _ray-exec-doc:

.. click:: ray.scripts.scripts:exec
   :prog: ray exec
   :show-nested:

.. _ray-submit-doc:

.. click:: ray.scripts.scripts:submit
   :prog: ray submit
   :show-nested:

.. _ray-attach-doc:

.. click:: ray.scripts.scripts:attach
   :prog: ray attach
   :show-nested:

.. _ray-get_head_ip-doc:

.. click:: ray.scripts.scripts:get_head_ip
   :prog: ray get_head_ip
   :show-nested:

.. _ray-stack-doc:

.. click:: ray.scripts.scripts:stack
   :prog: ray stack
   :show-nested:

.. _ray-memory-doc:

.. click:: ray.scripts.scripts:memory
   :prog: ray memory
   :show-nested:

.. _ray-timeline-doc:

.. click:: ray.scripts.scripts:timeline
   :prog: ray timeline
   :show-nested:

.. click:: ray.scripts.scripts:debug
   :prog: ray debug
   :show-nested:
