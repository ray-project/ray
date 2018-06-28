Profiling for Ray Users
=======================

This document is intended for users of Ray who want to know how to time 
the performance of their code while running on Ray. Profiling the 
performance of your code can be very helpful to determine where your 
code may not be parallelizing properly. If you are interested in 
pinpointing why your code running on Ray may not be achieving speedups 
as expected, then do read on!

A Basic Profiling Example
-------------------------

Let's start on a simple example, and compare how different looping 
structures of the same remote function affects performance.

As a stand-in for a computationally intensive and possibly slower function,
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
we can define each loop version as a separate function on the driver.

For the first version, each iteration of the loop calls the remote function, 
then calls ``ray.get`` in an attempt to store the current result into the 
list, as follows:

.. code-block:: python
  # This loop is suboptimal in Ray, and should only be used for the sake of this example
  def ex1():  
    list1 = []
    for i in range(5):
      list1.append(ray.get(func.remote()))

For the second version, each iteration of the loop calls the remote function, 
and stores it into the list **without** calling ``ray.get`` each time. ``ray.get`` 
is used after the loop has finished in preparation for processing ``func()``'s 
results:

.. code-block:: python
  # This loop is more proper in Ray
  def ex2():
    list2 = []
    for i in range(5):
      list2.append(func.remote())
    ray.get(list2)

Finally, as a demonstration of Ray's parallelism abilities, let's create a 
third version where the driver calls a second time-consuming remote function 
in between each call to ``func()``:

.. code-block:: python
  # Some other time-consuming remote function
  @ray.remote
    def other_func():
      time.sleep(0.2)

  def ex3():
    list3 = []
    for i in range(5):
      other_func.remote()
      list2.append(func.remote())
    ray.get(list3)


Timing Performance Using Python's Timestamps
--------------------------------------------
One quick way to sanity-check the performance of the three loops is simply to
time how long it takes to complete each loop version. We can do this using 
python's built-in ``time`` `module`_.

.. _`module`: https://docs.python.org/2/library/time.html

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

To **always** print out how long the loop takes to run each time the loop 
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

Alternatively, to print out the timer on **selective** calls to ``ex1()``,
we can forgo the decorator and make explicit calls using ``time_this``
as follows:

.. code-block:: python
  def ex1():  # Removed decorator
    list1 = []
    for i in range(5):
      list1.append(ray.get(func.remote()))

  def main():
    ray.init()
    ex1()  # This call outputs nothing
    time_this(ex1)()
    time_this(ex2)()
    time_this(ex3)()

  if __name__ == "__main__":
    main()

Finally, running the three timed loops should yield output similar to this:

.. code-block:: bash
  | func:'ex1' args:[(), {}] took: 2.5083 seconds |
  | func:'ex2' args:[(), {}] took: 1.0032 seconds |
  | func:'ex3' args:[(), {}] took: 1.1045 seconds |

Let's interpret these results. 

Most pertinently, ``ex1()`` took substantially more time than ``ex2()``, 
despite their only difference being that ``ex1()`` calls ``ray.get`` on the 
remote function before adding it to the list, while ``ex2()`` waits to fetch 
the entire list with ``ray.get`` at once.

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
each ``func()``'s result in succession. We are completely sabotaging any 
speedup via Ray parallelization! 

Meanwhile, ``ex2()`` takes about 1 second, much faster than it would normally 
take to call ``func()`` five times iteratively. Ray is running each call to 
``func()`` in parallel, saving us time. 

``ex1()`` is actually a common user mistake in Ray. ``ray.get`` is not 
necessary to do before adding the result of ``func()`` to the list. Instead, 
the driver should send out all parallelizable calls to the remote function 
to Ray before waiting to receive their results with ``ray.get``. ``ex1()``'s
suboptimal behavior can be noticed just using this simple timing test.

Additionally, to drive home Ray's speedup benefits of running remote function 
calls in parallel, ``ex3()`` takes only 1.1 seconds, despite making five calls
to a remote function that takes 0.2 seconds each call, and making five calls to
our first remote function that takes 0.5 seconds each call. If we weren't using
Ray and multiple CPUs, this loop would take at least 3.5 seconds to finish.


Profiling Using Python's CProfile
---------------------------------
TO-DO


Profiling Using An External Profiler (Line_Profiler)
----------------------------------------------------
A second way to profile the performance of our code using Ray is to use a third-
party profiler such as `Line_profiler`_. Line_profiler is a useful line-by-line
profiler for pure Python applications that formats its output side-by-side with
the profiled code itself. An alternative third-party profiler (not covered in this 
documentation) you could use is `Pyflame`_, which can generate profiling graphs.

.. _`Line_profiler`: https://github.com/rkern/line_profiler
.. _`Pyflame`: https://github.com/uber/pyflame

First install ``line_profiler`` with pip:

.. code-block:: bash
  pip install line_profiler

``line_profiler`` requires each section of driver code to profile as its own 
independent function. Conveniently, we have already done so by defining each
loop version in its own function. To tell ``line_profiler`` which functions
to profile, just add the ``@profile`` decorator:

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

Then, when running our Python script from the command line, we use the following
shell command to run the script with ``line_profiler`` enabled:

.. code-block:: bash
  kernprof -l your_script_here.py 

This command runs your script and prints your script's output as usual. ``Line_profiler``
instead outputs its profiling results to a corresponding binary file called 
``your_script_here.py.lprof``.

To read ``line_profiler``'s results to terminal, use this shell command:

.. code-block:: bash
  python -m line_profiler your_script_here.py.lprof

In our loop example, this command outputs results for ``ex1()`` as follows:

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


Notice that each hit to line 33, ``list1.append(ray.get(func.remote()))``, 
takes the full 0.5 seconds waiting for ``func()`` to finish. Meanwhile, in 
``ex2()``, each call of ``func.remote()`` at line 40 only takes 0.127 ms, 
and the majority of the time is spent on waiting for ``ray.get()`` at the end:


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


And finally, ``line_profiler``'s output for ``ex3()``:

.. code-block:: bash
  Total time: 1.10395 s
  File: your_script_here.py
  Function: ex3 at line 43

  Line #      Hits         Time  Per Hit   % Time  Line Contents
  ==============================================================
      44                                           @profile
      45                                           def ex3():
      46         1          1.0      1.0      0.0   list3 = []
      47         6         13.0      2.2      0.0   for i in range(5):
      48         5        673.0    134.6      0.1     func2.remote()
      49         5        639.0    127.8      0.1     list3.append(func.remote())
      50         1    1102625.0 1102625.0     99.9    ray.get(list3)


Visualizing Tasks in the Ray Timeline
-------------------------------------
TO-DO

