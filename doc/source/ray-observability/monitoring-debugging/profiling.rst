.. _ray-core-profiling:

Profiling
=========

Visualizing Tasks in the Ray Timeline
-------------------------------------

The most important tool is the timeline visualization tool. To visualize tasks
in the Ray timeline, you can dump the timeline as a JSON file by running ``ray
timeline`` from the command line or ``ray.timeline`` from the Python API.

To use the timeline, Ray profiling must be enabled by setting the
``RAY_PROFILING=1`` environment variable prior to starting Ray on every machine.

.. code-block:: python

  ray.timeline(filename="/tmp/timeline.json")

Then open `chrome://tracing`_ in the Chrome web browser, and load
``timeline.json``.

.. _`chrome://tracing`: chrome://tracing

Profiling Using Python's CProfile
---------------------------------

You can use Python's native cProfile `profiling module`_ to profile the performance of your Ray application. Rather than tracking
line-by-line of your application code, cProfile can give the total runtime
of each loop function, as well as list the number of calls made and
execution time of all function calls made within the profiled code.

.. _`profiling module`: https://docs.python.org/3/library/profile.html#module-cProfile

Unlike ``line_profiler`` above, this detailed list of profiled function calls
**includes** internal function calls and function calls made within Ray.

However, similar to ``line_profiler``, cProfile can be enabled with minimal
changes to your application code (given that each section of the code you want
to profile is defined as its own function). To use cProfile, add an import
statement, then replace calls to the loop functions as follows:

.. code-block:: python

  import cProfile  # Added import statement

  def ex1():
      list1 = []
      for i in range(5):
          list1.append(ray.get(func.remote()))

  def main():
      ray.init()
      cProfile.run('ex1()')  # Modified call to ex1
      cProfile.run('ex2()')
      cProfile.run('ex3()')

  if __name__ == "__main__":
      main()

Now, when you execute your Python script, a cProfile list of profiled function
calls are printed on the terminal for each call made to ``cProfile.run()``.
At the very top of cProfile's output gives the total execution time for
``'ex1()'``:

.. code-block:: bash

  601 function calls (595 primitive calls) in 2.509 seconds

Following is a snippet of profiled function calls for ``'ex1()'``. Most of
these calls are quick and take around 0.000 seconds, so the functions of
interest are the ones with non-zero execution times:

.. code-block:: bash

  ncalls  tottime  percall  cumtime  percall filename:lineno(function)
  ...
      1    0.000    0.000    2.509    2.509 your_script_here.py:31(ex1)
      5    0.000    0.000    0.001    0.000 remote_function.py:103(remote)
      5    0.000    0.000    0.001    0.000 remote_function.py:107(_remote)
  ...
     10    0.000    0.000    0.000    0.000 worker.py:2459(__init__)
      5    0.000    0.000    2.508    0.502 worker.py:2535(get)
      5    0.000    0.000    0.000    0.000 worker.py:2695(get_global_worker)
     10    0.000    0.000    2.507    0.251 worker.py:374(retrieve_and_deserialize)
      5    0.000    0.000    2.508    0.502 worker.py:424(get_object)
      5    0.000    0.000    0.000    0.000 worker.py:514(submit_task)
  ...

The 5 separate calls to Ray's ``get``, taking the full 0.502 seconds each call,
can be noticed at ``worker.py:2535(get)``. Meanwhile, the act of calling the
remote function itself at ``remote_function.py:103(remote)`` only takes 0.001
seconds over 5 calls, and thus is not the source of the slow performance of
``ex1()``.


Profiling Ray Actors with cProfile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Considering that the detailed output of cProfile can be quite different depending
on what Ray functionalities we use, let us see what cProfile's output might look
like if our example involved Actors (for an introduction to Ray actors, see our
`Actor documentation here`_).

.. _`Actor documentation here`: http://docs.ray.io/en/master/actors.html

Now, instead of looping over five calls to a remote function like in ``ex1``,
let's create a new example and loop over five calls to a remote function
**inside an actor**. Our actor's remote function again just sleeps for 0.5
seconds:

.. code-block:: python

  # Our actor
  @ray.remote
  class Sleeper(object):
      def __init__(self):
          self.sleepValue = 0.5

      # Equivalent to func(), but defined within an actor
      def actor_func(self):
          time.sleep(self.sleepValue)

Recalling the suboptimality of ``ex1``, let's first see what happens if we
attempt to perform all five ``actor_func()`` calls within a single actor:

.. code-block:: python

  def ex4():
      # This is suboptimal in Ray, and should only be used for the sake of this example
      actor_example = Sleeper.remote()

      five_results = []
      for i in range(5):
          five_results.append(actor_example.actor_func.remote())

      # Wait until the end to call ray.get()
      ray.get(five_results)

We enable cProfile on this example as follows:

.. code-block:: python

  def main():
      ray.init()
      cProfile.run('ex4()')

  if __name__ == "__main__":
      main()

Running our new Actor example, cProfile's abbreviated output is as follows:

.. code-block:: bash

  12519 function calls (11956 primitive calls) in 2.525 seconds

  ncalls  tottime  percall  cumtime  percall filename:lineno(function)
  ...
  1    0.000    0.000    0.015    0.015 actor.py:546(remote)
  1    0.000    0.000    0.015    0.015 actor.py:560(_remote)
  1    0.000    0.000    0.000    0.000 actor.py:697(__init__)
  ...
  1    0.000    0.000    2.525    2.525 your_script_here.py:63(ex4)
  ...
  9    0.000    0.000    0.000    0.000 worker.py:2459(__init__)
  1    0.000    0.000    2.509    2.509 worker.py:2535(get)
  9    0.000    0.000    0.000    0.000 worker.py:2695(get_global_worker)
  4    0.000    0.000    2.508    0.627 worker.py:374(retrieve_and_deserialize)
  1    0.000    0.000    2.509    2.509 worker.py:424(get_object)
  8    0.000    0.000    0.001    0.000 worker.py:514(submit_task)
  ...

It turns out that the entire example still took 2.5 seconds to execute, or the
time for five calls to ``actor_func()`` to run in serial. If you recall ``ex1``,
this behavior was because we did not wait until after submitting all five
remote function tasks to call ``ray.get()``, but we can verify on cProfile's
output line ``worker.py:2535(get)`` that ``ray.get()`` was only called once at
the end, for 2.509 seconds. What happened?

It turns out Ray cannot parallelize this example, because we have only
initialized a single ``Sleeper`` actor. Because each actor is a single,
stateful worker, our entire code is submitted and ran on a single worker the
whole time.

To better parallelize the actors in ``ex4``, we can take advantage
that each call to ``actor_func()`` is independent, and instead
create five ``Sleeper`` actors. That way, we are creating five workers
that can run in parallel, instead of creating a single worker that
can only handle one call to ``actor_func()`` at a time.

.. code-block:: python

  def ex4():
      # Modified to create five separate Sleepers
      five_actors = [Sleeper.remote() for i in range(5)]

      # Each call to actor_func now goes to a different Sleeper
      five_results = []
      for actor_example in five_actors:
          five_results.append(actor_example.actor_func.remote())

      ray.get(five_results)

Our example in total now takes only 1.5 seconds to run:

.. code-block:: bash

  1378 function calls (1363 primitive calls) in 1.567 seconds

  ncalls  tottime  percall  cumtime  percall filename:lineno(function)
  ...
  5    0.000    0.000    0.002    0.000 actor.py:546(remote)
  5    0.000    0.000    0.002    0.000 actor.py:560(_remote)
  5    0.000    0.000    0.000    0.000 actor.py:697(__init__)
  ...
  1    0.000    0.000    1.566    1.566 your_script_here.py:71(ex4)
  ...
  21    0.000    0.000    0.000    0.000 worker.py:2459(__init__)
  1    0.000    0.000    1.564    1.564 worker.py:2535(get)
  25    0.000    0.000    0.000    0.000 worker.py:2695(get_global_worker)
  3    0.000    0.000    1.564    0.521 worker.py:374(retrieve_and_deserialize)
  1    0.000    0.000    1.564    1.564 worker.py:424(get_object)
  20    0.001    0.000    0.001    0.000 worker.py:514(submit_task)
  ...

Profiling (Internal)
--------------------
If you are developing Ray core or debugging some system level failures, profiling the Ray core could help. In this case, see :ref:`Profiling (Internal) <ray-core-internal-profiling>`.
