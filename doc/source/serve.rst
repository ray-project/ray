Ray Serve (Experimental)
========================

Ray Serve is a serving library that exposes python function/classes to HTTP.
It has built-in support for flexible traffic policy. This means you can easy
split incoming traffic to multiple implementations. 

With Ray Serve, you can deploy your services at any scale.

.. warning::
  Ray Serve is Python 3 only.

Quickstart
----------
.. literalinclude:: ../../python/ray/experimental/serve/examples/echo_full.py

API
---
.. automodule:: ray.experimental.serve
    :members:

Policies For Backend Selection
------------------------------
.. autoclass:: ray.experimental.serve.queues.RandomPolicyQueue
.. autoclass:: ray.experimental.serve.queues.RoundRobinPolicyQueue
.. autoclass:: ray.experimental.serve.queues.PowerOfTwoPolicyQueue
.. autoclass:: ray.experimental.serve.queues.FixedPackingPolicyQueue

Serve Request Classes
---------------------
.. autoclass:: ray.experimental.serve.request_params.RequestParams
.. autoclass:: ray.experimental.serve.request_params.RequestInfo

Serve Router
------------
.. autoclass:: ray.experimental.serve.queues.CentralizedQueues
    :members: