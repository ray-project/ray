======================================
Advanced Topics and Configurations
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

To scale out a backend to many instances, simply configure the number of replicas.

.. code-block:: python

  config = {"num_replicas": 10}
  client.create_backend("my_scaled_endpoint_backend", handle_request, config=config)

  # scale it back down...
  config = {"num_replicas": 2}
  client.update_backend_config("my_scaled_endpoint_backend", config)

This will scale up or down the number of replicas that can accept requests.

Using Resources (CPUs, GPUs)
============================

To assign hardware resources per replica, you can pass resource requirements to
``ray_actor_options``.
By default, each replica requires one CPU.
To learn about options to pass in, take a look at :ref:`Resources with Actor<actor-resource-guide>` guide.

For example, to create a backend where each replica uses a single GPU, you can do the
following:

.. code-block:: python

  config = {"num_gpus": 1}
  client.create_backend("my_gpu_backend", handle_request, ray_actor_options=config)

Fractional Resources
--------------------

The resources specified in ``ray_actor_options`` can also be *fractional*.
This allows you to flexibly share resources between replicas.
For example, if you have two models and each doesn't fully saturate a GPU, you might want to have them share a GPU by allocating 0.5 GPUs each.
The same could be done to multiplex over CPUs.

.. code-block:: python

  half_gpu_config = {"num_gpus": 0.5}
  client.create_backend("my_gpu_backend_1", handle_request, ray_actor_options=half_gpu_config)
  client.create_backend("my_gpu_backend_2", handle_request, ray_actor_options=half_gpu_config)

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

  client.create_backend("parallel_backend", MyBackend, 12)


.. note::
  Some other libraries may not respect ``OMP_NUM_THREADS`` and have their own way to configure parallelism.
  For example, if you're using OpenCV, you'll need to manually set the number of threads using ``cv2.setNumThreads(num_threads)`` (set to 0 to disable multi-threading).
  You can check the configuration using ``cv2.getNumThreads()`` and ``cv2.getNumberOfCPUs()``.

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
  client.create_backend("counter1", BatchingExample, config=config)
  client.create_endpoint("counter1", backend="counter1", route="/increment")

Please take a look at :ref:`Batching Tutorial<serve-batch-tutorial>` for a deep
dive.

.. _`serve-split-traffic`:

Splitting Traffic Between Backends
==================================

At times it may be useful to expose a single endpoint that is served by multiple backends.
You can do this by splitting the traffic for an endpoint between backends using :mod:`client.set_traffic <ray.serve.api.Client.set_traffic>`.
When calling :mod:`client.set_traffic <ray.serve.api.Client.set_traffic>`, you provide a dictionary of backend name to a float value that will be used to randomly route that portion of traffic (out of a total of 1.0) to the given backend.
For example, here we split traffic 50/50 between two backends:

.. code-block:: python

  client.create_backend("backend1", MyClass1)
  client.create_backend("backend2", MyClass2)

  client.create_endpoint("fifty-fifty", backend="backend1", route="/fifty")
  client.set_traffic("fifty-fifty", {"backend1": 0.5, "backend2": 0.5})

Each request is routed randomly between the backends in the traffic dictionary according to the provided weights.
Please see :ref:`session-affinity` for details on how to ensure that clients or users are consistently mapped to the same backend.

Canary Deployments
------------------

:mod:`client.set_traffic <ray.serve.api.Client.set_traffic>` can be used to implement canary deployments, where one backend serves the majority of traffic, while a small fraction is routed to a second backend. This is especially useful for "canary testing" a new model on a small percentage of users, while the tried and true old model serves the majority. Once you are satisfied with the new model, you can reroute all traffic to it and remove the old model:

.. code-block:: python

  client.create_backend("default_backend", MyClass)

  # Initially, set all traffic to be served by the "default" backend.
  client.create_endpoint("canary_endpoint", backend="default_backend", route="/canary-test")

  # Add a second backend and route 1% of the traffic to it.
  client.create_backend("new_backend", MyNewClass)
  client.set_traffic("canary_endpoint", {"default_backend": 0.99, "new_backend": 0.01})

  # Add a third backend that serves another 1% of the traffic.
  client.create_backend("new_backend2", MyNewClass2)
  client.set_traffic("canary_endpoint", {"default_backend": 0.98, "new_backend": 0.01, "new_backend2": 0.01})

  # Route all traffic to the new, better backend.
  client.set_traffic("canary_endpoint", {"new_backend": 1.0})

  # Or, if not so succesful, revert to the "default" backend for all traffic.
  client.set_traffic("canary_endpoint", {"default_backend": 1.0})

Incremental Rollout
-------------------

:mod:`client.set_traffic <ray.serve.api.Client.set_traffic>` can also be used to implement incremental rollout.
Here, we want to replace an existing backend with a new implementation by gradually increasing the proportion of traffic that it serves.
In the example below, we do this repeatedly in one script, but in practice this would likely happen over time across multiple scripts.

.. code-block:: python

  client.create_backend("existing_backend", MyClass)

  # Initially, all traffic is served by the existing backend.
  client.create_endpoint("incremental_endpoint", backend="existing_backend", route="/incremental")

  # Then we can slowly increase the proportion of traffic served by the new backend.
  client.create_backend("new_backend", MyNewClass)
  client.set_traffic("incremental_endpoint", {"existing_backend": 0.9, "new_backend": 0.1})
  client.set_traffic("incremental_endpoint", {"existing_backend": 0.8, "new_backend": 0.2})
  client.set_traffic("incremental_endpoint", {"existing_backend": 0.5, "new_backend": 0.5})
  client.set_traffic("incremental_endpoint", {"new_backend": 1.0})

  # At any time, we can roll back to the existing backend.
  client.set_traffic("incremental_endpoint", {"existing_backend": 1.0})

.. _session-affinity:

Session Affinity
----------------

Splitting traffic randomly among backends for each request is is general and simple, but it can be an issue when you want to ensure that a given user or client is served by the same backend repeatedly.
To address this, a "shard key" can be specified for each request that will deterministically map to a backend.
In practice, this should be something that uniquely identifies the entity that you want to consistently map, like a client ID or session ID.
The shard key can either be specified via the X-SERVE-SHARD-KEY HTTP header or :mod:`handle.options(shard_key="key") <ray.serve.handle.RayServeHandle.options>`.

.. note:: The mapping from shard key to backend may change when you update the traffic policy for an endpoint.

.. code-block:: python

  # Specifying the shard key via an HTTP header.
  requests.get("127.0.0.1:8000/api", headers={"X-SERVE-SHARD-KEY": session_id})

  # Specifying the shard key in a call made via serve handle.
  handle = client.get_handle("api_endpoint")
  handler.options(shard_key=session_id).remote(args)

.. _serve-shadow-testing:

Shadow Testing
--------------

Sometimes when deploying a new backend, you may want to test it out without affecting the results seen by users.
You can do this with :mod:`client.shadow_traffic <ray.serve.api.Client.shadow_traffic>`, which allows you to duplicate requests to multiple backends for testing while still having them served by the set of backends specified via :mod:`client.set_traffic <ray.serve.api.Client.set_traffic>`.
Metrics about these requests are recorded as usual so you can use them to validate model performance.
This is demonstrated in the example below, where we create an endpoint serviced by a single backend but shadow traffic to two other backends for testing.

.. code-block:: python

  client.create_backend("existing_backend", MyClass)

  # All traffic is served by the existing backend.
  client.create_endpoint("shadowed_endpoint", backend="existing_backend", route="/shadow")

  # Create two new backends that we want to test.
  client.create_backend("new_backend_1", MyNewClass)
  client.create_backend("new_backend_2", MyNewClass)

  # Shadow traffic to the two new backends. This does not influence the result
  # of requests to the endpoint, but a proportion of requests are
  # *additionally* sent to these backends.

  # Send 50% of all queries to the endpoint new_backend_1.
  client.shadow_traffic("shadowed_endpoint", "new_backend_1", 0.5)
  # Send 10% of all queries to the endpoint new_backend_2.
  client.shadow_traffic("shadowed_endpoint", "new_backend_2", 0.1)

  # Stop shadowing traffic to the backends.
  client.shadow_traffic("shadowed_endpoint", "new_backend_1", 0)
  client.shadow_traffic("shadowed_endpoint", "new_backend_2", 0)

.. _serve-model-composition:

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

Ray Serve exposes important system metrics like the number of successful and
errored requests through the Ray metrics monitoring infrastructure. By default,
the metrics are exposed in Prometheus format on each node. See the
`Ray Monitoring documentation <../ray-metrics.html>`__ for more information.


Reconfiguring Backends (Experimental)
=====================================

Suppose you want to update a parameter in your model without creating a whole
new backend.  You can do this by writing a `reconfigure` method for the class
underlying your backend.  At runtime, you can then pass in your new parameters
by setting the `user_config` field of :mod:`BackendConfig <ray.serve.BackendConfig>`.

The following simple example will make the usage clear:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_reconfigure.py

The `reconfigure` method is called when the class is created if `user_config`
is set.  In particular, it's also called when new replicas are created in the
future, in case you decide to scale up your backend later.  The
`reconfigure` method is also called each time `user_config` is updated via 
:mod:`client.update_backend_config <ray.serve.api.Client.update_backend_config>`.

Dependency Management
=====================

Ray Serve supports serving backends with different (possibly conflicting)
python dependencies.  For example, you can simultaneously serve one backend
that uses legacy Tensorflow 1 and another backend that uses Tensorflow 2.

Currently this is supported using `conda <https://docs.conda.io/en/latest/>`_.
You must have a conda environment set up for each set of
dependencies you want to isolate.  If using a multi-node cluster, the
conda configuration must be identical across all nodes.

Here's an example script.  For it to work, first create a conda
environment named ``ray-tf1`` with Ray Serve and Tensorflow 1 installed,
and another named ``ray-tf2`` with Ray Serve and Tensorflow 2.  The Ray and
python versions must be the same in both environments.  To specify
an environment for a backend to use, simply pass the environment name in to
:mod:`client.create_backend <ray.serve.api.Client.create_backend>`
as shown below.

.. literalinclude:: ../../../python/ray/serve/examples/doc/conda_env.py

.. warning::
  The script must be run in an activated conda environment (not required to be
  ``ray-tf1`` or ``ray-tf2``).  We hope to remove this restriction in the
  future.

.. note::
  If the argument ``env`` is omitted, backends will be started in the same
  conda environment as the caller of
  :mod:`client.create_backend <ray.serve.api.Client.create_backend>` by
  default.

