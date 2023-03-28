.. _generator-pattern:

Pattern: Using generators to reduce heap memory usage
=====================================================

In this pattern, we use **generators** in Python to reduce the total heap memory usage during a task. The key idea is that for tasks that return multiple objects, we can return them one at a time instead of all at once. This allows a worker to free the heap memory used by a previous return value before returning the next one.

Example use case
----------------

You have a task that returns multiple large values. Another possibility is a task that returns a single large value, but you want to stream this value through Ray's object store by breaking it up into smaller chunks.

Using normal Python functions, we can write such a task like this. Here's an example that returns numpy arrays of size 100MB each:

.. literalinclude:: ../doc_code/pattern_generators.py
    :language: python
    :start-after: __large_values_start__
    :end-before: __large_values_end__

However, this will require the task to hold all ``num_returns`` arrays in heap memory at the same time at the end of the task. If there are many return values, this can lead to high heap memory usage and potentially an out-of-memory error.

We can fix the above example by rewriting ``large_values`` as a **generator**. Instead of returning all values at once as a tuple or list, we can ``yield`` one value at a time.

.. literalinclude:: ../doc_code/pattern_generators.py
    :language: python
    :start-after: __large_values_generator_start__
    :end-before: __large_values_generator_end__

Code example
------------

.. literalinclude:: ../doc_code/pattern_generators.py
    :language: python
    :start-after: __program_start__

.. code-block:: text

    $ RAY_IGNORE_UNHANDLED_ERRORS=1 python test.py 100

    Using normal functions...
    ... -- A worker died or was killed while executing a task by an unexpected system error. To troubleshoot the problem, check the logs for the dead worker...
    Worker failed
    Using generators...
    (large_values_generator pid=373609) yielded return value 0
    (large_values_generator pid=373609) yielded return value 1
    (large_values_generator pid=373609) yielded return value 2
    ...
    Success!
