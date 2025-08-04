Utility
=======

.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.ActorPool
   ray.util.queue.Queue
   ray.util.list_named_actors

   ray.util.serialization.register_serializer
   ray.util.serialization.deregister_serializer

   ray.util.accelerators.tpu.get_current_pod_worker_count
   ray.util.accelerators.tpu.get_current_pod_name
   ray.util.accelerators.tpu.get_num_tpu_chips_on_node

   ray.nodes
   ray.cluster_resources
   ray.available_resources

   .. Other docs have references to these
   ray.util.queue.Empty
   ray.util.queue.Full

.. _custom-metric-api-ref:

Custom Metrics
--------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.metrics.Counter
   ray.util.metrics.Gauge
   ray.util.metrics.Histogram

.. _package-ref-debugging-apis:

Debugging
---------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   ray.util.rpdb.set_trace
   ray.util.inspect_serializability
   ray.timeline
