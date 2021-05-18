Serve API Reference
===================

Core APIs
---------
.. autofunction:: ray.serve.start
.. autofunction:: ray.serve.deployment
.. autofunction:: ray.serve.list_deployments
.. autofunction:: ray.serve.get_deployment
.. autofunction:: ray.serve.shutdown
.. autofunction:: ray.serve.connect
.. autofunction:: ray.serve.create_backend
.. autofunction:: ray.serve.list_backends
.. autofunction:: ray.serve.delete_backend
.. autofunction:: ray.serve.get_backend_config
.. autofunction:: ray.serve.update_backend_config
.. autofunction:: ray.serve.create_endpoint
.. autofunction:: ray.serve.list_endpoints
.. autofunction:: ray.serve.delete_endpoint
.. autofunction:: ray.serve.set_traffic
.. autofunction:: ray.serve.shadow_traffic
.. autofunction:: ray.serve.get_handle

.. _`deployment-api`:

Deployment API
--------------

.. autoclass:: ray.serve.api.Deployment
    :members: deploy, delete, options, get_handle

.. _`servehandle-api`:

ServeHandle API
---------------
.. autoclass:: ray.serve.handle.RayServeHandle
    :members: remote, options

Batching Requests
-----------------
.. autofunction:: ray.serve.batch(max_batch_size=10, batch_wait_timeout_s=0.0)
