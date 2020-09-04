Advanced AsyncIO on Ray
=======================

It is an in-depth tutorial about how to use AsyncIO on Ray. To learn basic APIs, please take a look at `Ray AsyncIO APIs <https://docs.ray.io/en/latest/async_api.html>`_

Motivation
----------

Ray excels in CPU bounded workloads such as ML training or data processing, but what if workloads are IO bounded? The best solution for Python is to use `AsyncIO API <https://docs.python.org/3/library/asyncio.html>`_.  
Ray is nicely integrated to Python's AsyncIO APIs to support IO bounded workload.

In this tutorial, we'll learn about how AsyncIO works in Ray. After that, we'll build distributed web crawler together using AsyncIO with Ray.

AsyncIO
-------
AsyncIO is Python's standard library to enable async programming in Python. This tutorial will assume you already know about the basic of AsyncIO.
Please take a look at this tutorial before reading this tutorial `tutorial <https://realpython.com/async-io-python/>`_ if you are not familiar with AsyncIO APIs.

Ray's APIs are compatible to AsyncIO. There are 2 different ways you can use AsyncIO with Ray.

AsyncIO with Ray's APIs
-----------------------
Ray is innately asynchronous. Whenever users use `.remote()`, it generates Object reference that is obtainable by `ray.get` API.
This enables asynchronous programming since programs can run bunch of `.remote()` calls and wait for the result all at once.
Here, the issue is that `ray.get` is a blocking call. It means when `ray.get` is called, programs need to wait until object references are ready (`.remote()` calls returned).

To get around this "blocking call" problem, Ray supports "non-blocking" `ray.get`. This is achieved by making Object references compatible to AsyncIO's coroutines. Programs can `await` or run other AsyncIO APIs instead of `ray.get` on object references.
When is this useful? Let's say programs need to `ray.get` on many objects, but we don't want them to wait and waste CPUs. 

Let's see how this can be useful with an example. Note the example requires Python 3.7+

.. code-block:: python

  import ray
  ray.init()

  @ray.remote()
  def f():
      import time
      # This simulate the IO bounded task.
      time.sleep(10)
      return True

  # .remote call is already asynchronous.
  # The program returns right away.
  # Note that a is an object reference.
  # But it is also compatible to AsyncIO coroutine.
  a = f.remote()

  # This needs to wait for 10 seconds.
  ray.get(a)


  #Now, let's try using asyncio.
  import asyncio
  
  async def async_task(ray_object_ref):
      print("task start")
      # Here, this coroutine will pause and switch context to other coroutines.
      await ray_object_ref
      print("task done")

  async def main():
      a = f.remote()
      b = f.remote()
      await asyncio.gather(async_task(a), async_task(b))

  asyncio.run(main())
