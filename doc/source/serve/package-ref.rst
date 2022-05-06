Ray Serve API
=============

Core APIs
---------
.. autofunction:: ray.serve.start
.. autofunction:: ray.serve.deployment
.. autofunction:: ray.serve.list_deployments
.. autofunction:: ray.serve.get_deployment
.. autofunction:: ray.serve.shutdown

.. _`deployment-api`:

Deployment API
--------------

.. autoclass:: ray.serve.deployment.Deployment
    :members: deploy, delete, options, get_handle

.. _`servehandle-api`:

ServeHandle API
---------------
.. autoclass:: ray.serve.handle.RayServeHandle
    :members: remote, options

Batching Requests
-----------------
.. autofunction:: ray.serve.batch(max_batch_size=10, batch_wait_timeout_s=0.0)
