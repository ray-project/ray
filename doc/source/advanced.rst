Advanced Usage
==============

This page will cover some more advanced examples of using Ray's flexible programming model.

Nested Remote Functions
-----------------------

Remote functions can call other remote functions, resulting in nested tasks.
For example, consider the following.

.. code:: python

    @ray.remote
    def f():
        return 1

    @ray.remote
    def g():
        # Call f 4 times and return the resulting object IDs.
        return [f.remote() for _ in range(4)]

    @ray.remote
    def h():
        # Call f 4 times, block until those 4 tasks finish,
        # retrieve the results, and return the values.
        return ray.get([f.remote() for _ in range(4)])

Then calling ``g`` and ``h`` produces the following behavior.

.. code:: python

    >>> ray.get(g.remote())
    [ObjectID(b1457ba0911ae84989aae86f89409e953dd9a80e),
     ObjectID(7c14a1d13a56d8dc01e800761a66f09201104275),
     ObjectID(99763728ffc1a2c0766a2000ebabded52514e9a6),
     ObjectID(9c2f372e1933b04b2936bb6f58161285829b9914)]

    >>> ray.get(h.remote())
    [1, 1, 1, 1]

**One limitation** is that the definition of ``f`` must come before the
definitions of ``g`` and ``h`` because as soon as ``g`` is defined, it
will be pickled and shipped to the workers, and so if ``f`` hasn't been
defined yet, the definition will be incomplete.


Passing Around Actor Handles (Experimental)
-------------------------------------------

Actor handles can be passed into other tasks. To see an example of this, take a
look at the `asynchronous parameter server example`_. To illustrate this with
a simple example, consider a simple actor definition. This functionality is
currently **experimental** and subject to the limitations described below.

.. code-block:: python

  @ray.remote
  class Counter(object):
      def __init__(self):
          self.counter = 0

      def inc(self):
          self.counter += 1

      def get_counter(self):
          return self.counter

We can define remote functions (or actor methods) that use actor handles.

.. code-block:: python

  @ray.remote
  def f(counter):
      while True:
          counter.inc.remote()

If we instantiate an actor, we can pass the handle around to various tasks.

.. code-block:: python

  counter = Counter.remote()

  # Start some tasks that use the actor.
  [f.remote(counter) for _ in range(4)]

  # Print the counter value.
  for _ in range(10):
      print(ray.get(counter.get_counter.remote()))

.. _`asynchronous parameter server example`: http://ray.readthedocs.io/en/latest/example-parameter-server.html
