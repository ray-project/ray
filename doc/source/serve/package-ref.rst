Serve API Reference
===================

``serve.accept_batch`` is a decorator to mark that your backend implementation
does accepts a list of requests as input instead of just a request.
.. autofunction:: ray.serve.accept_batch

Basic APIs
----------
.. autofunction:: ray.serve.start
.. autofunction:: ray.serve.connect

Client API
----------
.. autoclass:: ray.serve.api.Client
    :members: create_backend, list_backends, delete_backend, get_backend_config, update_backend_config, create_endpoint, list_endpoints, delete_endpoint, set_traffic, shadow_traffic, get_handle

Handle API
----------
.. autoclass:: ray.serve.handle.RayServeHandle
    :members: remote, options
