How-to: Profile Ray Programs
============================

Profiling the performance of your code can be very helpful to determine
performance bottlenecks or to find out where your code may not be parallelized
properly.

Visualizing Tasks in the Ray Timeline
-------------------------------------

The most important tool is the timeline visualization tool. To visualize tasks
in the Ray timeline, you can dump the timeline as a JSON file by running ``ray
timeline`` from the command line or by using the following command.

.. code-block:: python

  ray.timeline(filename="/tmp/timeline.json")

Then open `chrome://tracing`_ in the Chrome web browser, and load
``timeline.json``.

.. _`chrome://tracing`: chrome://tracing


A Basic Example to Profile
--------------------------

Let's try to profile a simple example, and compare how different ways to
write a simple loop can affect performance.

As a proxy for a computationally intensive and possibly slower function,
let's define our remote function to just sleep for 0.5 seconds:

.. code-block:: python

  import ray
  import time

  # Our time-consuming remote function
  @ray.remote
  def func():
      time.sleep(0.5)

In our example setup, we wish to call our remote function ``func()`` five
times, and store the result of each call into a list. To compare the
performance of different ways of looping our calls to our remote function,
we can define each loop version as a separate function on the driver script.

For the first version **ex1**, each iteration of the loop calls the remote
function, then calls ``ray.get`` in an attempt to store the current result
into the list, as follows:

.. code-block:: python

  # This loop is suboptimal in Ray, and should only be used for the sake of this example
  def ex1():
      list1 = []
      for i in range(5):
          list1.append(ray.get(func.remote()))

For the second version **ex2**, each iteration of the loop calls the remote
function, and stores it into the list **without** calling ``ray.get`` each time.
``ray.get`` is used after the loop has finished, in preparation for processing
``func()``'s results:

.. code-block:: python

  # This loop is more proper in Ray
  def ex2():
      list2 = []
      for i in range(5):
          list2.append(func.remote())
      ray.get(list2)

Finally, for an example that's not so parallelizable, let's create a
third version **ex3** where the driver has to call a local
function in between each call to the remote function ``func()``:

.. code-block:: python

  # A local function executed on the driver, not on Ray
  def other_func():
      time.sleep(0.3)

  def ex3():
      list3 = []
      for i in range(5):
          other_func()
          list3.append(func.remote())
      ray.get(list3)


Timing Performance Using Python's Timestamps
--------------------------------------------

One way to sanity-check the performance of the three loops is simply to
time how long it takes to complete each loop version. We can do this using
python's built-in ``time`` `module`_.

.. _`module`: https://docs.python.org/3/library/time.html

The ``time`` module contains a useful ``time()`` function that returns the
current timestamp in unix time whenever it's called. We can create a generic
function wrapper to call ``time()`` right before and right after each loop
function to print out how long each loop takes overall:

.. code-block:: python

  # This is a generic wrapper for any driver function you want to time
  def time_this(f):
      def timed_wrapper(*args, **kw):
          start_time = time.time()
          result = f(*args, **kw)
          end_time = time.time()

          # Time taken = end_time - start_time
          print('| func:%r args:[%r, %r] took: %2.4f seconds |' % \
                (f.__name__, args, kw, end_time - start_time))
          return result
      return timed_wrapper

To always print out how long the loop takes to run each time the loop
function ``ex1()`` is called, we can evoke our ``time_this`` wrapper with
a function decorator. This can similarly be done to functions ``ex2()``
and ``ex3()``:

.. code-block:: python

  @time_this  # Added decorator
  def ex1():
      list1 = []
      for i in range(5):
          list1.append(ray.get(func.remote()))

  def main():
      ray.init()
      ex1()
      ex2()
      ex3()

  if __name__ == "__main__":
      main()

Then, running the three timed loops should yield output similar to this:

.. code-block:: bash

  | func:'ex1' args:[(), {}] took: 2.5083 seconds |
  | func:'ex2' args:[(), {}] took: 1.0032 seconds |
  | func:'ex3' args:[(), {}] took: 2.0039 seconds |

Let's interpret these results.

Here, ``ex1()`` took substantially more time than ``ex2()``, where
their only difference is that ``ex1()`` calls ``ray.get`` on the remote
function before adding it to the list, while ``ex2()`` waits to fetch the
entire list with ``ray.get`` at once.

.. code-block:: python

  @ray.remote
  def func(): # A single call takes 0.5 seconds
      time.sleep(0.5)

  def ex1():  # Took Ray 2.5 seconds
      list1 = []
      for i in range(5):
          list1.append(ray.get(func.remote()))

  def ex2():  # Took Ray 1 second
      list2 = []
      for i in range(5):
          list2.append(func.remote())
      ray.get(list2)

Notice how ``ex1()`` took 2.5 seconds, exactly five times 0.5 seconds, or
the time it would take to wait for our remote function five times in a row.

By calling ``ray.get`` after each call to the remote function, ``ex1()``
removes all ability to parallelize work, by forcing the driver to wait for
each ``func()``'s result in succession. We are not taking advantage of Ray
parallelization here!

Meanwhile, ``ex2()`` takes about 1 second, much faster than it would normally
take to call ``func()`` five times iteratively. Ray is running each call to
``func()`` in parallel, saving us time.

``ex1()`` is actually a common user mistake in Ray. ``ray.get`` is not
necessary to do before adding the result of ``func()`` to the list. Instead,
the driver should send out all parallelizable calls to the remote function
to Ray before waiting to receive their results with ``ray.get``. ``ex1()``'s
suboptimal behavior can be noticed just using this simple timing test.

Realistically, however, many applications are not as highly parallelizable
as ``ex2()``, and the application includes sections where the code must run in
serial. ``ex3()`` is such an example, where the local function ``other_func()``
must run first before each call to ``func()`` can be submitted to Ray.

.. code-block:: python

  # A local function that must run in serial
  def other_func():
      time.sleep(0.3)

  def ex3():  # Took Ray 2 seconds, vs. ex1 taking 2.5 seconds
      list3 = []
      for i in range(5):
          other_func()
          list2.append(func.remote())
      ray.get(list3)

What results is that while ``ex3()`` still gained 0.5 seconds of speedup
compared to the completely serialized ``ex1()`` version, this speedup is
still nowhere near the ideal speedup of ``ex2()``.

The dramatic speedup of ``ex2()`` is possible because ``ex2()`` is
theoretically completely parallelizable: if we were given 5 CPUs, all 5 calls
to ``func()`` can be run in parallel. What is happening with ``ex3()``,
however, is that each parallelized call to ``func()`` is staggered by a wait
of 0.3 seconds for the local ``other_func()`` to finish.

``ex3()`` is thus a manifestation of `Amdahls Law`_: the fastest theoretically
possible execution time from parallelizing an application is limited to be
no better than the time it takes to run all serial parts in serial.

.. _`Amdahls Law`: https://en.wikipedia.org/wiki/Amdahl%27s_law

Due to Amdahl's Law, ``ex3()`` must take at least 1.5
seconds -- the time it takes for 5 serial calls to ``other_func()`` to finish!
After an additional 0.5 seconds to execute func and get the result, the
computation is done.


Profiling Using An External Profiler (Line Profiler)
----------------------------------------------------

One way to profile the performance of our code using Ray is to use a third-party
profiler such as `Line_profiler`_. Line_profiler is a useful line-by-line
profiler for pure Python applications that formats its output side-by-side with
the profiled code itself.

Alternatively, another third-party profiler (not covered in this documentation)
that you could use is `Pyflame`_, which can generate profiling graphs.

.. _`Line_profiler`: https://github.com/rkern/line_profiler
.. _`Pyflame`: https://github.com/uber/pyflame

First install ``line_profiler`` with pip:

.. code-block:: bash

  pip install line_profiler

``line_profiler`` requires each section of driver code that you want to profile as
its own independent function. Conveniently, we have already done so by defining
each loop version as its own function. To tell ``line_profiler`` which functions
to profile, just add the ``@profile`` decorator to ``ex1()``, ``ex2()`` and
``ex3()``. Note that you do not need to import ``line_profiler`` into your Ray
application:

.. code-block:: python

  @profile  # Added decorator
  def ex1():
      list1 = []
      for i in range(5):
          list1.append(ray.get(func.remote()))

  def main():
      ray.init()
      ex1()
      ex2()
      ex3()

  if __name__ == "__main__":
      main()

Then, when we want to execute our Python script from the command line, instead
of ``python your_script_here.py``, we use the following shell command to run the
script with ``line_profiler`` enabled:

.. code-block:: bash

  kernprof -l your_script_here.py

This command runs your script and prints only your script's output as usual.
``Line_profiler`` instead outputs its profiling results to a corresponding
binary file called ``your_script_here.py.lprof``.

To read ``line_profiler``'s results to terminal, use this shell command:

.. code-block:: bash

  python -m line_profiler your_script_here.py.lprof

In our loop example, this command outputs results for ``ex1()`` as follows.
Note that execution time is given in units of 1e-06 seconds:

.. code-block:: bash

  Timer unit: 1e-06 s

  Total time: 2.50883 s
  File: your_script_here.py
  Function: ex1 at line 28

  Line #      Hits         Time  Per Hit   % Time  Line Contents
  ==============================================================
      29                                           @profile
      30                                           def ex1():
      31         1          3.0      3.0      0.0   list1 = []
      32         6         18.0      3.0      0.0   for i in range(5):
      33         5    2508805.0 501761.0    100.0     list1.append(ray.get(func.remote()))


Notice that each hit to ``list1.append(ray.get(func.remote()))`` at line 33
takes the full 0.5 seconds waiting for ``func()`` to finish. Meanwhile, in
``ex2()`` below, each call of ``func.remote()`` at line 40 only takes 0.127 ms,
and the majority of the time (about 1 second) is spent on waiting for ``ray.get()``
at the end:


.. code-block:: bash

  Total time: 1.00357 s
  File: your_script_here.py
  Function: ex2 at line 35

  Line #      Hits         Time  Per Hit   % Time  Line Contents
  ==============================================================
      36                                           @profile
      37                                           def ex2():
      38         1          2.0      2.0      0.0   list2 = []
      39         6         13.0      2.2      0.0   for i in range(5):
      40         5        637.0    127.4      0.1     list2.append(func.remote())
      41         1    1002919.0 1002919.0     99.9    ray.get(list2)


And finally, ``line_profiler``'s output for ``ex3()``. Each call to
``func.remote()`` at line 50 still take magnitudes faster than 0.5 seconds,
showing that Ray is successfully parallelizing the remote calls. However, each
call to the local function ``other_func()`` takes the full 0.3 seconds,
totalling up to the guaranteed minimum application execution time of 1.5
seconds:

.. code-block:: bash

  Total time: 2.00446 s
  File: basic_kernprof.py
  Function: ex3 at line 44

  Line #      Hits         Time  Per Hit   % Time  Line Contents
  ==============================================================
      44                                           @profile
      45                                           #@time_this
      46                                           def ex3():
      47         1          2.0      2.0      0.0   list3 = []
      48         6         13.0      2.2      0.0   for i in range(5):
      49         5    1501934.0 300386.8     74.9     other_func()
      50         5        917.0    183.4      0.0     list3.append(func.remote())
      51         1     501589.0 501589.0     25.0   ray.get(list3)


Profiling Using Python's CProfile
---------------------------------

A second way to profile the performance of your Ray application is to
use Python's native cProfile `profiling module`_. Rather than tracking
line-by-line of your application code, cProfile can give the total runtime
of each loop function, as well as list the number of calls made and
execution time of all function calls made within the profiled code.

.. _`profiling module`: https://docs.python.org/3/library/profile.html#module-cProfile

Unlike ``line_profiler`` above, this detailed list of profiled function calls
**includes** internal function calls and function calls made within Ray!

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

Now, when executing your Python script, a cProfile list of profiled function
calls will be outputted to terminal for each call made to ``cProfile.run()``.
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
      5    0.000    0.000    0.001    0.000 remote_function.py:107(_submit)
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

.. _`Actor documentation here`: http://ray.readthedocs.io/en/latest/actors.html

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
  1    0.000    0.000    0.015    0.015 actor.py:560(_submit)
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
time for five calls to ``actor_func()`` to run in serial. We remember in ``ex1``
that this behavior was because we did not wait until after submitting all five
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
  5    0.000    0.000    0.002    0.000 actor.py:560(_submit)
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
