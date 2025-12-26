Anti-pattern: Over-parallelizing with too fine-grained tasks harms speedup
==========================================================================

**Summary:** Avoid over-parallelizing. Parallelizing tasks has higher overhead than using normal functions.

Parallelizing or distributing tasks usually comes with higher overhead than an ordinary function call. Therefore, if you parallelize a function that executes very quickly, the overhead could take longer than the actual function call.

To handle this problem, be careful about parallelizing too much. If you have a function or task that's too small, you can use a technique called **batching** to make your tasks do more meaningful work in a single call.


Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_too_fine_grained_tasks.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:** Use batching.

.. literalinclude:: ../doc_code/anti_pattern_too_fine_grained_tasks.py
    :language: python
    :start-after: __batching_start__
    :end-before: __batching_end__

As you can see from the preceding example, over-parallelizing has higher overhead and the program runs slower than the serial version.
Through batching with a proper batch size, the code amortizes the overhead and achieves the expected speedup.
