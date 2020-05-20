=====================================
Advanced Topics, Configurations & FAQ
=====================================

Ray Serve has a number of knobs and tools for you to tune for your particular workload. 
All Ray Serve advanced options and topics are covered on this page aside from the 
fundamentals of :doc:`deployment`. For a more hands on take, please check out the :ref:`serve-tutorials`.

There are a number of things you'll likely want to do with your serving application including
scaling out, splitting traffic, or batching input for better response performance. To do all of this,
you will create a ``BackendConfig``, a configuration object that you'll use to set 
the properties of a particular backend.

.. contents::

Scaling Out
===========

To scale out a backend to multiple workers, simplify configure the number of replicas.

.. code-block:: python

  config = {"num_replicas": 2}
  serve.create_backend("my_scaled_endpoint_backend", handle_request, config=config)

This will scale out the number of workers that can accept requests.

Using Resources (CPUs, GPUs)
============================

To assign hardware resource per worker, you can pass resource requirements to
``ray_actor_options``. To learn about options to pass in, take a look at
:ref:`Resources with Actor<actor-resource-guide>` guide.

For example, to create a backend where each replica uses a single GPU, you can do the
following:

.. code-block:: python

  options = {"num_gpus": 1}
  serve.create_backend("my_gpu_backend", handle_request, ray_actor_options=options)

.. note::

  Deep learning models like PyTorch and Tensorflow often use all the CPUs when
  performing inference. Ray sets the environment variable ``OMP_NUM_THREADS=1`` to
  :ref:`avoid contention<omp-num-thread-note>`. This means each worker will only
  use one CPU instead of all of them.

.. _serve-batching:

Batching to improve performance
===============================

You can also have Ray Serve batch requests for performance. You'll configure this in the backend config.

.. code-block:: python

  class BatchingExample:
      def __init__(self):
          self.count = 0

      @serve.accept_batch
      def __call__(self, flask_request):
          self.count += 1
          batch_size = serve.context.batch_size
          return [self.count] * batch_size

  serve.create_endpoint("counter1", "/increment")

  config = {"max_batch_size": 5}
  serve.create_backend("counter1", BatchingExample, config=config)
  serve.set_traffic("counter1", {"counter1": 1.0})

.. _`serve-split-traffic`:

Splitting Traffic and A/B Testing
==================================

It's trivial to also split traffic, simply specify the endpoint and the backends that you want to split.

.. code-block:: python
  
  serve.create_endpoint("endpoint_identifier_split", "/split", methods=["GET", "POST"])

  # splitting traffic 70/30
  serve.set_traffic("endpoint_identifier_split", {"my_endpoint_backend": 0.7, "my_endpoint_backend_class": 0.3})

While splitting traffic is general simple, at times you'll want to consider :ref:`session-affinity`, making it easy to
control what users see which version of the model. See the docs on :ref:`session-affinity` for more information.

.. _session-affinity:

Session Affinity
================

In some cases, you may want to ensure that requests from the same client, user, etc. get mapped to the same backend.
To do this, you can specify a "shard key" that will deterministically map requests to a backend.
The shard key can either be specified via the X-SERVE-SHARD-KEY HTTP header or ``handle.options(shard_key="key")``.

.. note:: The mapping from shard key to backend may change when you update the traffic policy for an endpoint.

.. code-block:: python

  # Specifying the shard key via an HTTP header.
  requests.get("127.0.0.1:8000/api", headers={"X-SERVE-SHARD-KEY": session_id})

  # Specifying the shard key in a call made via serve handle.
  handle = serve.get_handle("api_endpoint")
  handler.options(shard_key=session_id).remote(args)


.. _serve-faq:

Ray Serve FAQ
=============

How do I deploy serve?
----------------------

See :doc:`deployment` for information about how to deploy serve.

How do I delete a backend?
--------------------------

To delete a backend, we can use `serve.delete_backend`.
Note that the backend must not be use by any endpoints in order to be delete.
Once a backend is deleted, its tag can be reused.

.. code-block:: python

  serve.delete_backend("simple_backend")