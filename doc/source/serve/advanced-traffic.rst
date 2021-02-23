=========================
Advanced Traffic Patterns
=========================

.. contents::

.. _`serve-split-traffic`:

Splitting Traffic
=================

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
==================

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
===================

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
================

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
==============

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
