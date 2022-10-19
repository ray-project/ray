Pattern: Using tasks to achieve MapReduce
=========================================

In this pattern, Ray remote tasks can be used to implement the MapReduce paradigm.
For the ``map`` stage, we can submit multiple tasks to process data in a distributed and parallel fashion.
For the ``reduce`` stage, we use :ref:`ray.get() <ray-get-ref>` to fetch the results of each of these tasks and aggregate on them.
We can also have many ``map`` stages and many ``reduce`` stages.

Example use case
----------------

Use Ray tasks to calculate the sum of a doubled list of integers in a MapReduce fasion.

.. figure:: ../images/map-reduce.svg

    Map and reduce

Code examples
-------------

**Single-threaded MapReduce:**

.. literalinclude:: ../doc_code/pattern_map_reduce.py
    :language: python
    :start-after: __single_threaded_map_reduce_start__
    :end-before: __single_threaded_map_reduce_end__

**Ray parallel MapReduce:**

.. literalinclude:: ../doc_code/pattern_map_reduce.py
    :language: python
    :start-after: __parallel_map_reduce_start__
    :end-before: __parallel_map_reduce_end__
