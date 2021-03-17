Serve API Reference
===================

Core APIs
---------
.. autofunction:: ray.serve.start
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
.. autofunction:: ray.serve.shutdown

Backend Configuration
---------------------
.. autoclass:: ray.serve.BackendConfig

.. _`servehandle-api`:

ServeHandle API
---------------
.. autoclass:: ray.serve.handle.RayServeHandle
    :members: remote, options

When calling from Python, the backend implementation will receive ``ServeRequest``
objects instead of Starlette requests.

.. autoclass:: ray.serve.utils.ServeRequest
    :members:

Batching Requests
-----------------
.. autofunction:: ray.serve.accept_batch
