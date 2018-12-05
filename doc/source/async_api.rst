Async API (experimental)
========================

Many web applications & prediction serving service today heavily rely on asynchronous execution to gain performance.
Since version 3.5, a set of basic async interfaces has been introduced to Python,
making it more feasible to program asynchronously,
and popular async frameworks (aiohttp, aioredis, etc.) are emerging,
indicating the power of asynchronous APIs.

This document talks about asynchronous APIs in Ray.

Starting Ray
------------

Please refer to `Starting Ray`_ for instructions.

.. _`Starting Ray`: http://ray.readthedocs.io/en/latest/tutorial.html#starting-ray


Convert an ObjectID into a asyncio Future
-----------------------------------------

Object IDs can be converted into asyncio Futures.

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


Example Use
-----------

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
|   # Execute f asynchronously.          |   # Execute f asynchronously with asyncio.          |
|                                        |                                                     |
|                                        |   from ray.exprimental import async_api             |
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


Initialize & shutdown Async Ray APIs
------------------------------------

Async Ray APIs can be initialized automatically by calling any related functions.
But users could call `ray.experimental.async_api.init` ahead of time.

Users can manually shutdown the APIs by calling `ray.experimental.async_api.shutdown`.
Shutdown the API will cancel all related pending tasks.


