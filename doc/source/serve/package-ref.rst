Serve API Reference
===================

Core APIs
---------
.. autofunction:: ray.serve.start
.. autofunction:: ray.serve.connect
.. autofunction:: create_backend
.. autofunction:: list_backends
.. autofunction:: delete_backend
.. autofunction:: get_backend_config
.. autofunction:: update_backend_config
.. autofunction:: create_endpoint
.. autofunction:: list_endpoints
.. autofunction:: delete_endpoint
.. autofunction:: set_traffic
.. autofunction:: shadow_traffic
.. autofunction:: get_handle
.. autofunction:: shutdown

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
