======================================
Advanced Topics, Configurations, & FAQ
======================================

Ray Serve has a number of knobs and tools for you to tune for your particular workload.
All Ray Serve advanced options and topics are covered on this page aside from the
fundamentals of :doc:`deployment`. For a more hands on take, please check out the :ref:`serve-tutorials`.

There are a number of things you'll likely want to do with your serving application including
scaling out, splitting traffic, or batching input for better performance. To do all of this,
you will create a ``BackendConfig``, a configuration object that you'll use to set
the properties of a particular backend.

.. contents::

Scaling Out
===========

To scale out a backend to multiple workers, simplify configure the number of replicas.

.. code-block:: python

  config = {"num_replicas": 10}
  serve.create_backend("my_scaled_endpoint_backend", handle_request, config=config)

  # scale it back down...
  config = {"num_replicas": 2}
  serve.update_backend_config("my_scaled_endpoint_backend", config)

This will scale up or down the number of workers that can accept requests.

Using Resources (CPUs, GPUs)
============================

To assign hardware resource per worker, you can pass resource requirements to
``ray_actor_options``. To learn about options to pass in, take a look at
:ref:`Resources with Actor<actor-resource-guide>` guide.

For example, to create a backend where each replica uses a single GPU, you can do the
following:

.. code-block:: python

  config = {"num_gpus": 1}
  serve.create_backend("my_gpu_backend", handle_request, ray_actor_options=config)

Configuring Parallelism with OMP_NUM_THREADS
--------------------------------------------

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the OMP_NUM_THREADS environment variable.
To :ref:`avoid contention<omp-num-thread-note>`, Ray sets ``OMP_NUM_THREADS=1`` by default because Ray workers and actors use a single CPU by default.
If you *do* want to enable this parallelism in your Serve backend, just set OMP_NUM_THREADS to the desired value either when starting Ray or in your function/class definition:

.. code-block:: bash

  OMP_NUM_THREADS=12 ray start --head
  OMP_NUM_THREADS=12 ray start --address=$HEAD_NODE_ADDRESS

.. code-block:: python

  class MyBackend:
      def __init__(self, parallelism):
          os.environ["OMP_NUM_THREADS"] = parallelism
          # Download model weights, initialize model, etc.

  serve.create_backend("parallel_backend", MyBackend, 12)

.. _serve-batching:

Batching to improve performance
===============================

You can also have Ray Serve batch requests for performance. In order to do use this feature, you need to:
1. Set the ``max_batch_size`` in the ``config`` dictionary.
2. Modify your backend implementation to accept a list of requests and return a list of responses instead of handling a single request.


.. code-block:: python

  class BatchingExample:
      def __init__(self):
          self.count = 0

      @serve.accept_batch
      def __call__(self, requests):
          responses = []
              for request in requests:
                  responses.append(request.json())
          return responses

  config = {"max_batch_size": 5}
  serve.create_backend("counter1", BatchingExample, config=config)
  serve.create_endpoint("counter1", backend="counter1", route="/increment")

Please take a look at :ref:`Batching Tutorial<serve-batch-tutorial>` for a deep
dive.

.. _`serve-split-traffic`:

Splitting Traffic Between Backends
==================================

At times it may be useful to expose a single endpoint that is served by multiple backends.
You can do this by splitting the traffic for an endpoint between backends using :mod:`set_traffic <ray.serve.set_traffic>`.
When calling :mod:`set_traffic <ray.serve.set_traffic>`, you provide a dictionary of backend name to a float value that will be used to randomly route that portion of traffic (out of a total of 1.0) to the given backend.
For example, here we split traffic 50/50 between two backends:

.. code-block:: python

  serve.create_backend("backend1", MyClass1)
  serve.create_backend("backend2", MyClass2)

  serve.create_endpoint("fifty-fifty", backend="backend1", route="/fifty")
  serve.set_traffic("fifty-fifty", {"backend1": 0.5, "backend2": 0.5})

Each request is routed randomly between the backends in the traffic dictionary according to the provided weights.
Please see :ref:`session-affinity` for details on how to ensure that clients or users are consistently mapped to the same backend.

Canary Deployments
------------------

:mod:`set_traffic <ray.serve.set_traffic>` can be used to implement canary deployments, where one backend serves the majority of traffic, while a small fraction is routed to a second backend. This is especially useful for "canary testing" a new model on a small percentage of users, while the tried and true old model serves the majority. Once you are satisfied with the new model, you can reroute all traffic to it and remove the old model:

.. code-block:: python

  serve.create_backend("default_backend", MyClass)

  # Initially, set all traffic to be served by the "default" backend.
  serve.create_endpoint("canary_endpoint", backend="default_backend", route="/canary-test")

  # Add a second backend and route 1% of the traffic to it.
  serve.create_backend("new_backend", MyNewClass)
  serve.set_traffic("canary_endpoint", {"default_backend": 0.99, "new_backend": 0.01})

  # Add a third backend that serves another 1% of the traffic.
  serve.create_backend("new_backend2", MyNewClass2)
  serve.set_traffic("canary_endpoint", {"default_backend": 0.98, "new_backend": 0.01, "new_backend2": 0.01})

  # Route all traffic to the new, better backend.
  serve.set_traffic("canary_endpoint", {"new_backend": 1.0})

  # Or, if not so succesful, revert to the "default" backend for all traffic.
  serve.set_traffic("canary_endpoint", {"default_backend": 1.0})

Incremental Rollout
-------------------

:mod:`set_traffic <ray.serve.set_traffic>` can also be used to implement incremental rollout.
Here, we want to replace an existing backend with a new implementation by gradually increasing the proportion of traffic that it serves.
In the example below, we do this repeatedly in one script, but in practice this would likely happen over time across multiple scripts.

.. code-block:: python

  serve.create_backend("existing_backend", MyClass)

  # Initially, all traffic is served by the existing backend.
  serve.create_endpoint("incremental_endpoint", backend="existing_backend", route="/incremental")

  # Then we can slowly increase the proportion of traffic served by the new backend.
  serve.create_backend("new_backend", MyNewClass)
  serve.set_traffic("incremental_endpoint", {"existing_backend": 0.9, "new_backend": 0.1})
  serve.set_traffic("incremental_endpoint", {"existing_backend": 0.8, "new_backend": 0.2})
  serve.set_traffic("incremental_endpoint", {"existing_backend": 0.5, "new_backend": 0.5})
  serve.set_traffic("incremental_endpoint", {"new_backend": 1.0})

  # At any time, we can roll back to the existing backend.
  serve.set_traffic("incremental_endpoint", {"existing_backend": 1.0})

.. _session-affinity:

Session Affinity
----------------

Splitting traffic randomly among backends for each request is is general and simple, but it can be an issue when you want to ensure that a given user or client is served by the same backend repeatedly.
To address this, Serve offers a "shard key" can be specified for each request that will deterministically map to a backend.
In practice, this should be something that uniquely identifies the entity that you want to consistently map, like a client ID or session ID.
The shard key can either be specified via the X-SERVE-SHARD-KEY HTTP header or ``handle.options(shard_key="key")``.

.. note:: The mapping from shard key to backend may change when you update the traffic policy for an endpoint.

.. code-block:: python

  # Specifying the shard key via an HTTP header.
  requests.get("127.0.0.1:8000/api", headers={"X-SERVE-SHARD-KEY": session_id})

  # Specifying the shard key in a call made via serve handle.
  handle = serve.get_handle("api_endpoint")
  handler.options(shard_key=session_id).remote(args)

Shadow Testing
--------------

Sometimes when deploying a new backend, you may want to test it out without affecting the results seen by users.
You can do this with :mod:`shadow_traffic <ray.serve.shadow_traffic>`, which allows you to duplicate requests to multiple backends for testing while still having them served by the set of backends specified via :mod:`set_traffic <ray.serve.set_traffic>`.
Metrics about these requests are recorded as usual so you can use them to validate model performance.
This is demonstrated in the example below, where we create an endpoint serviced by a single backend but shadow traffic to two other backends for testing.

.. code-block:: python

  serve.create_backend("existing_backend", MyClass)

  # All traffic is served by the existing backend.
  serve.create_endpoint("shadowed_endpoint", backend="existing_backend", route="/shadow")

  # Create two new backends that we want to test.
  serve.create_backend("new_backend_1", MyNewClass)
  serve.create_backend("new_backend_2", MyNewClass)

  # Shadow traffic to the two new backends. This does not influence the result
  # of requests to the endpoint, but a proportion of requests are
  # *additionally* sent to these backends.

  # Send 50% of all queries to the endpoint new_backend_1.
  serve.shadow_traffic("shadowed_endpoint", "new_backend_1", 0.5)
  # Send 10% of all queries to the endpoint new_backend_2.
  serve.shadow_traffic("shadowed_endpoint", "new_backend_2", 0.1)

  # Stop shadowing traffic to the backends.
  serve.shadow_traffic("shadowed_endpoint", "new_backend_1", 0)
  serve.shadow_traffic("shadowed_endpoint", "new_backend_2", 0)

Composing Multiple Models
=========================
Ray Serve supports composing individually scalable models into a single model
out of the box. For instance, you can combine multiple models to perform
stacking or ensembles.

To define a higher-level composed model you need to do three things:

1. Define your underlying models (the ones that you will compose together) as
   Ray Serve backends
2. Define your composed model, using the handles of the underlying models
   (see the example below).
3. Define an endpoint representing this composed model and query it!

In order to avoid synchronous execution in the composed model (e.g., it's very
slow to make calls to the composed model), you'll need to make the function
asynchronous by using an ``async def``. You'll see this in the example below.

That's it. Let's take a look at an example:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_model_composition.py

Monitoring
==========

Ray Serve exposes system metrics like number of requests through Python API
``serve.stat`` and HTTP ``/-/metrics`` API. By default, it uses a custom
structured format for easy parsing and debugging.

Via python:

.. code-block:: python

  serve.stat()
  """
    [..., {
          "info": {
              "name": "num_http_requests",
              "route": "/-/routes",
              "type": "MetricType.COUNTER"
          },
          "value": 1
      },
      {
          "info": {
              "name": "num_http_requests",
              "route": "/echo",
              "type": "MetricType.COUNTER"
          },
          "value": 10
      }, ...]
  """

Via HTTP:

.. code-block::

  curl http://localhost:8000/-/metrics
  # Returns the same output as above in JSON format.

You can also access the result in `Prometheus <https://prometheus.io/>`_ format,
by setting the ``metric_exporter`` option in :mod:`serve.init <ray.serve.init>`.

.. code-block:: python

  from ray.serve.metric import PrometheusExporter
  serve.init(metric_exporter=PrometheusExporter)

.. code-block::

  curl http://localhost:8000/-/metrics

  # HELP backend_request_counter_total Number of queries that have been processed in this replica
  # TYPE backend_request_counter_total counter
  backend_request_counter_total{backend="echo:v1"} 5.0
  backend_request_counter_total{backend="echo:v2"} 5.0
  ...

The metric exporter is extensible and you can customize it for your own metric
infrastructure. We are gathering feedback and welcome contribution! Feel free
to submit a github issue to chat with us in #serve channel in `community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_.

Here's an simple example of a dummy exporter that writes metrics to file:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_metric_export.py

.. _serve-faq:

Ray Serve FAQ
=============

How do I deploy serve?
----------------------

See :doc:`deployment` for information about how to deploy serve.

How do I delete backends and endpoints?
---------------------------------------

To delete a backend, you can use :mod:`serve.delete_backend <ray.serve.delete_backend>`.
Note that the backend must not be use by any endpoints in order to be delete.
Once a backend is deleted, its tag can be reused.

.. code-block:: python

  serve.delete_backend("simple_backend")


To delete a endpoint, you can use :mod:`serve.delete_endpoint <ray.serve.delete_endpoint>`.
Note that the endpoint will no longer work and return a 404 when queried.
Once a endpoint is deleted, its tag can be reused.

.. code-block:: python

  serve.delete_endpoint("simple_endpoint")

How do I call an endpoint from Python code?
-------------------------------------------

use the following  to get a "handle" to that endpoint.

.. code-block:: python
    
    handle = serve.get_handle("api_endpoint")


How do I call a method on my backend class besides __call__?
-------------------------------------------------------------

To call a method via HTTP use the header field ``X-SERVE-CALL-METHOD``.

To call a method via Python, do the following:

.. code-block:: python

    class StatefulProcessor:
        def __init__(self):
            self.count = 1

        def __call__(self, request):
            return {"current": self.count}

        def other_method(self, inc):
            self.count += inc
            return True

    handle = serve.get_handle("backend_name")
    handle.options(method_name="other_method").remote(5)
