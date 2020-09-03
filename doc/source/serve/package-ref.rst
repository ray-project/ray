Serve API Reference
===================

Basic APIs
----------
.. autofunction:: ray.serve.start
.. autofunction:: ray.serve.connect

Management APIs
---------------
.. autoclass:: ray.serve.Client
    :members: create_backend
    :members: list_backends
    :members: delete_backend
    :members: get_backend_config
    :members: update_backend_config
    :members: create_endpoint
    :members: list_endpoints
    :members: delete_endpoint
    :members: set_traffic
    :members: shadow_traffic
    :members: get_handle

Serve Handle API
----------------
.. autoclass:: ray.serve.RayServeHandle
    :members: remote
    :members: options


``serve.accept_batch`` is a decorator to mark that your backend implementation
does accepts a list of requests as input instead of just a request.
.. autofunction:: ray.serve.accept_batch
