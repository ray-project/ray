The Async Ray API
=================

Starting Ray
------------

Please refer to `The Ray API` _ for instructions.

.. _`The Ray API`: http://ray.readthedocs.io/en/latest/api.html#starting-ray


Ray's async mechanism
---------------------

Ray's async mechanism is built on eventloops.


Getting values from object IDs
------------------------------

Object IDs can be converted into a coroutine that return objects by calling ``ray.experimental.async_api.get`` on the object
ID.

``ray.experimental.async_api.get`` accepts more complex forms of parameters than `ray.get`:

* A single object ID

.. code-block:: python
  import time
  import ray.experimental.async_api as async_api

  @ray.remote
  def f():
      time.sleep(1000)
      return {'key1': ['value']}

  # Get one object ID. The async get will return immediately.
  coroutine_object = async_api.get(f.remote())
  # Wait the coroutine until it completes.
  async_api.run_until_complete(coroutine_object)  # {'key1': ['value']}

* A single coroutine or future that contains an ObjectID

.. code-block:: python
  import time
  import ray.experimental.async_api as async_api

  async def f():
      return ray.put({'key1': ['value']})

  # Get one object ID. The async get will return immediately.
  coroutine_object = async_api.get(f())
  # Wait the coroutine until it completes.
  async_api.run_until_complete(coroutine_object)  # {'key1': ['value']}


* A chain composed of coroutines and futures that will eventually return an ObjectID:

.. code-block:: python
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

    results = async_api.run_until_complete(recurrent_get(obj_id)) # {'key1': ['value']}

* A list composed of objects we talked above.

.. autofunction:: ray.experimental.async_api.get


Waiting for a subset of tasks to finish
---------------------------------------

`ray.experimental.async_api.wait` has the same purpose with `ray.wait` _ but it supports
async operations. You could read docs of `ray.wait` _ to understand its behaviors.

.. _`ray.wait`: http://ray.readthedocs.io/en/latest/api.html#ray.wait

`ray.experimental.async_api.wait` can accept a list composed of ObjectIDs,
futures and coroutines.


.. code-block:: python

  import time
  import numpy as np
  import ray.experimental.async_api as async_api

  @ray.remote
  def f(n):
      time.sleep(n)
      return n

  # Start 3 tasks with different durations.
  results = [f.remote(i) for i in range(3)]
  # Block until 2 of them have finished.
  ready_ids, remaining_ids = async_api.run_until_complete(async_api.wait(results, num_returns=2))

  # Start 5 tasks with different durations.
  results = [f.remote(i) for i in range(5)]
  # Block until 4 of them have finished or 2.5 seconds pass.
  ready_ids, remaining_ids = async_api.run_until_complete(async_api.wait(results, num_returns=4, timeout=2500))

Because `ray.experimental.async_api.wait` supports futures and coroutines as its input,
it could happen that a passing in future/coroutine fails to return an ObjectID
before timeout. In this case, we will return `None`:

.. code-block:: python

  import time
  import numpy as np
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
      results, pendings = async_api.run_until_complete(fut) # [], [None]*5

