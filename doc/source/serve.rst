Ray Serve
=========

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

Ray Serve is a serving library that exposes python function/classes to HTTP.
It has built-in support for flexible traffic policy. This means you can easy
split incoming traffic to multiple implementations. 

.. note::

  This API is new and may be revised in future Ray releases. If you encounter
  any bugs, please file an `issue on GitHub`_.

With Ray Serve, you can deploy your services at any scale.

Quickstart
----------
.. literalinclude:: ../../python/ray/experimental/serve/examples/echo_full.py

API
---
.. automodule:: ray.experimental.serve
    :members:
