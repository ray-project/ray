Async API (Experimental)
========================

Many web applications & prediction serving service today heavily rely on asynchronous execution to gain performance.
Since version 3.5, a set of basic async interfaces has been introduced to Python,
making it more feasible to program asynchronously,
and popular async frameworks (aiohttp, aioredis, etc.) are emerging,
indicating the power of asynchronous APIs.

This document describes asynchronous APIs in Ray.

Starting Ray
------------

You must initialize Ray first.

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


Known issues
------------

Currently, Plasma Object Store will try to send all notification about all ObjectIDs to all workers.
It is possible that the Plasma Object Store becomes slower or even runs out of memory.

A workaround is manually shutdown the APIs by calling `ray.experimental.async_api.shutdown`.
Shutdown the API will cancel all related pending tasks.
