Serve API Reference
===================

Start or Connect to a Cluster
-----------------------------
.. autofunction:: ray.serve.start
.. autofunction:: ray.serve.connect

Client API
----------
.. autoclass:: ray.serve.api.Client
    :members: create_backend, list_backends, delete_backend, get_backend_config, update_backend_config, create_endpoint, list_endpoints, delete_endpoint, set_traffic, shadow_traffic, get_handle, shutdown

Backend Configuration
---------------------
.. autoclass:: ray.serve.BackendConfig

.. autoclass:: ray.serve.CondaEnv

.. autofunction:: ray.serve.get_current_backend_tag

.. autofunction:: ray.serve.get_current_replica_tag

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

Built-in Backends
-----------------
.. autoclass:: ray.serve.backends.ImportedBackend
