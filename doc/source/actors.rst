Actors
======

Remote functions in Ray should be thought of as functional and side-effect free.
Restricting ourselves only to remote functions gives us distributed functional
programming, which is great for many use cases, but in practice is a bit
limited.


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

Current Actor Limitations
-------------------------

We are working to address the following issues.

1. **Actor lifetime management:** Currently, when the original actor handle for
   an actor goes out of scope, a task is scheduled on that actor that kills the
   actor process (this new task will run once all previous tasks have finished
   running). This could be an issue if the original actor handle goes out of
   scope, but the actor is still being used by tasks that have been passed the
   actor handle.
2. **Returning actor handles:** Actor handles currently cannot be returned from
   a remote function or actor method. Similarly, ``ray.put`` cannot be called on
   an actor handle.
3. **Reconstruction of evicted actor objects:** If ``ray.get`` is called on an
   evicted object that was created by an actor method, Ray currently will not
   reconstruct the object. For more information, see the documentation on
   `fault tolerance`_.
4. **Deterministic reconstruction of lost actors:** If an actor is lost due to
   node failure, the actor is reconstructed on a new node, following the order
   of initial execution. However, new tasks that are scheduled onto the actor
   in the meantime may execute in between re-executed tasks. This could be an
   issue if your application has strict requirements for state consistency.

.. _`asynchronous parameter server example`: http://ray.readthedocs.io/en/latest/example-parameter-server.html
.. _`fault tolerance`: http://ray.readthedocs.io/en/latest/fault-tolerance.html
