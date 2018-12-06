Async API (Experimental)
========================

Since Python 3.5, it is possible to write concurrent code using the ``async/await`` `syntax <https://docs.python.org/3/library/asyncio.html>`__.

This document describes Ray's support for asyncio, enabling integration with popular async frameworks (e.g., aiohttp, aioredis, etc.) for high performance web and prediction serving.

Starting Ray
------------

You must initialize Ray first.

Please refer to `Starting Ray`_ for instructions.

.. _`Starting Ray`: http://ray.readthedocs.io/en/latest/tutorial.html#starting-ray


Converting Ray objects into asyncio futures
-------------------------------------------

Ray object IDs can be converted into asyncio futures with ``ray.experimental.async_api``.

.. code-block:: python

  import asyncio
  import time
  import ray
  from ray.experimental import async_api

  @ray.remote
  def f():
      time.sleep(1)
      return {'key1': ['value']}

  ray.init()
  future = async_api.as_future(f.remote())
  asyncio.get_event_loop().run_until_complete(future)  # {'key1': ['value']}


.. autofunction:: ray.experimental.async_api.as_future


Example Usage
-------------

+----------------------------------------+-----------------------------------------------------+
| **Basic Python**                       | **Distributed with Ray**                            |
+----------------------------------------+-----------------------------------------------------+
| .. code-block:: python                 | .. code-block:: python                              |
|                                        |                                                     |
|   # Execute f serially.                |   # Execute f in parallel.                          |
|                                        |                                                     |
|                                        |                                                     |
|   def f():                             |   @ray.remote                                       |
|     time.sleep(1)                      |   def f():                                          |
|     return 1                           |       time.sleep(1)                                 |
|                                        |       return 1                                      |
|                                        |                                                     |
|                                        |   ray.init()                                        |
|   results = [f() for i in range(4)]    |   results = ray.get([f.remote() for i in range(4)]) |
+----------------------------------------+-----------------------------------------------------+
| **Async Python**                       | **Async Ray**                                       |
+----------------------------------------+-----------------------------------------------------+
| .. code-block:: python                 | .. code-block:: python                              |
|                                        |                                                     |
|   # Execute f asynchronously.          |   # Execute f asynchronously with Ray/asyncio.      |
|                                        |                                                     |
|                                        |   from ray.experimental import async_api            |
|                                        |                                                     |
|                                        |   @ray.remote                                       |
|   async def f():                       |   def f():                                          |
|       await asyncio.sleep(1)           |       time.sleep(1)                                 |
|       return 1                         |       return 1                                      |
|                                        |                                                     |
|                                        |   ray.init()                                        |
|   loop = asyncio.get_event_loop()      |   loop = asyncio.get_event_loop()                   |
|   tasks = [f() for i in range(4)]      |   tasks = [async_api.as_future(f.remote())          |
|                                        |            for i in range(4)]                       |
|   results = loop.run_until_complete(   |   results = loop.run_until_complete(                |
|       asyncio.gather(tasks))           |       asyncio.gather(tasks))                        |
+----------------------------------------+-----------------------------------------------------+


Known Issues
------------

Async API support is experimental, and we are working to improve its performance. Please `let us know <https://github.com/ray-project/ray/issues>`__ any issues you encounter.
