Package Reference
=================

Basic APIs
----------
.. autofunction:: ray.serve.init
.. autofunction:: ray.serve.shutdown
.. autofunction:: ray.serve.create_backend
.. autofunction:: ray.serve.create_endpoint


APIs for Managing Endpoints
---------------------------
.. autofunction:: ray.serve.list_endpoints
.. autofunction:: ray.serve.delete_endpoint
.. autofunction:: ray.serve.set_traffic
.. autofunction:: ray.serve.shadow_traffic


APIs for Managing Backends
--------------------------
.. autofunction:: ray.serve.list_backends
.. autofunction:: ray.serve.delete_backend
.. autofunction:: ray.serve.get_backend_config
.. autofunction:: ray.serve.update_backend_config

Advanced APIs
-------------

``serve.get_handle`` enables calling endpoints from Python.

.. autofunction:: ray.serve.get_handle
.. autoclass:: ray.serve.handle.RayServeHandle

``serve.stat`` queries Ray Serve's built-in metric monitor.
.. autofunction:: ray.serve.stat


``serve.accept_batch`` marks your backend API does accept list of input instead
of just single input.
.. autofunction:: ray.serve.accept_batch
