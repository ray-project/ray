Ray Serve (Experimental)
========================

Ray Serve is a serving library that exposes python function/classes to HTTP.
It has built-in support for flexible traffic policy. This means you can easy
split incoming traffic to multiple implementations. 

With Ray Serve, you can deploy your services at any scale.

Quickstart
----------
.. literalinclude:: ../../python/ray/experimental/serve/examples/echo_full.py

API
---
.. automodule:: ray.experimental.serve
    :members:
