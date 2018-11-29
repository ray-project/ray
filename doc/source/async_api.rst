The Async Ray API
=================

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


Initialize & shutdown Async Ray APIs
------------------------------------

Async Ray APIs can be initialized automatically by calling any related functions.
But users could call `ray.experimental.async_api.init` ahead of time.

Users can manually shutdown the APIs by calling `ray.experimental.async_api.shutdown`.
Shutdown the API will cancel all related pending tasks.

Getting values from object IDs
------------------------------

Object IDs can be converted into a coroutine that return objects by calling ``ray.experimental.async_api.get`` on the object
ID.

``ray.experimental.async_api.get`` accepts more complex forms of parameters than `ray.get`:

* A single object ID

.. code-block:: python

  import asyncio
  import time
  import ray.experimental.async_api as async_api

  @ray.remote
  def f():
      time.sleep(1)
      return {'key1': ['value']}

  # Get one object ID. The async get will return immediately.
  coroutine_object = async_api.get(f.remote())
  # Wait the coroutine until it completes.
  asyncio.get_event_loop().run_until_complete(coroutine_object)  # {'key1': ['value']}

* A single coroutine or future that contains an ObjectID

.. code-block:: python

  import asyncio
  import time
  import ray
  import ray.experimental.async_api as async_api

  async def f():
      return ray.put({'key1': ['value']})

  # Get one object ID. The async get will return immediately.
  coroutine_object = async_api.get(f())
  # Wait the coroutine until it completes.
  asyncio.get_event_loop().run_until_complete(coroutine_object)  # {'key1': ['value']}


* A chain composed of coroutines and futures that will eventually return an ObjectID:

.. code-block:: python

    import asyncio
    import ray
    import ray.experimental.async_api as async_api

    # get from coroutine/future chains
    obj_id = {'key1': ['value']}
    for _ in range(7):
        obj_id = ray.put([obj_id])

    async def recurrent_get(obj_id):
        if isinstance(obj_id, str):
            return obj_id
        obj_id = await async_api.get(obj_id)
        if isinstance(obj_id, list):
            return await (recurrent_get(obj_id[0]))
        return obj_id

    results = asyncio.get_event_loop().run_until_complete(recurrent_get(obj_id)) # {'key1': ['value']}

* A list composed of objects we talked above.

.. autofunction:: ray.experimental.async_api.get


Waiting for a subset of tasks to finish
---------------------------------------

`ray.experimental.async_api.wait` has the same purpose with `ray.wait` _ but it supports
async operations. You could read docs of `ray.wait`_ to understand its behaviors.

.. _`ray.wait`: http://ray.readthedocs.io/en/latest/api.html#ray.wait

`ray.experimental.async_api.wait` can accept a list composed of ObjectIDs,
futures and coroutines.


.. code-block:: python

  import asyncio
  import time
  import ray.experimental.async_api as async_api

  @ray.remote
  def f(n):
      time.sleep(n)
      return n

  # Start 3 tasks with different durations.
  results = [f.remote(i) for i in range(3)]
  # Block until 2 of them have finished.
  ready_ids, remaining_ids = asyncio.get_event_loop().run_until_complete(async_api.wait(results, num_returns=2))

  # Start 5 tasks with different durations.
  results = [f.remote(i) for i in range(5)]
  # Block until 4 of them have finished or 2.5 seconds pass.
  ready_ids, remaining_ids = asyncio.get_event_loop().run_until_complete(async_api.wait(results, num_returns=4, timeout=2500))

Because `ray.experimental.async_api.wait` supports futures and coroutines as its input,
it could happen that a passing in future/coroutine fails to return an ObjectID
before timeout. In this case, we will return the pending inputs:

.. code-block:: python

  import asyncio
  import time
  import ray.experimental.async_api as async_api

  def delayed_gen_tasks(delay=5, time_scale=0.1):
      async def _gen(n):
          await asyncio.sleep(delay, loop=async_api.eventloop)

          @ray.remote
          def f(n):
              time.sleep(n * time_scale)
              return n

          return f.remote(n)
      return [_gen(i) for i in range(5)]

      tasks = delayed_gen_tasks(100, 5)
      fut = async_api.wait(tasks, timeout=5, num_returns=len(tasks))
      results, pendings = asyncio.get_event_loop().run_until_complete(fut) # [], tasks


.. autofunction:: ray.experimental.async_api.wait

Async group operations
----------------------
You could use `ray.experimental.async_api.create_group` to accomplish complex
async controls. This function returns a `PlasmaFutureGroup` object which works
like a list and stores info about different tasks.

For example, this could help you implement something like `asyncio.wait` (which may have inspired `ray.wait`):

.. code-block:: python

  import functools
  import time
  import ray.experimental.async_api as async_api

  async def wait(*coroutines_or_futures,
                 timeout: float,
                 num_returns: int,
                 loop=None,
                 return_exceptions=False):
      """This method resembles `asyncio.wait`.

      Args:
          *coroutines_or_futures:  A list of coroutines or futures.
          timeout (float): The timeout in seconds.
          num_returns (int): The minimal number of ready object returns.
          loop (PlasmaSelectorEventLoop): An eventloop.
          return_exceptions: If true, return exceptions as results
              without raising them.

      Returns:
          Tuple[List, List]: Ready futures & unready ones.
      """

      fut = async_api.create_group(return_exceptions=return_exceptions)
      fut.set_halt_condition(
          functools.partial(
              fut.halt_on_some_finished,
              n=num_returns,
          ))
      fut.extend(coroutines_or_futures)
      results = await fut.wait(timeout)  # set timeout
      # Ignore `CancelledError` caused by pending tasks.
      fut.return_exceptions = True
      return results



The key is about a method called `set_halt_condition` which could register a condition function.
Change the condition function to decide when `PlasmaFutureGroup` should be marked as finished.

.. autofunction:: ray.experimental.async_api.create_group
