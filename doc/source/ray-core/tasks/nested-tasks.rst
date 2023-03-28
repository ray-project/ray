Nested Remote Functions
=======================

Remote functions can call other remote functions, resulting in nested tasks.
For example, consider the following.

.. literalinclude:: ../doc_code/nested-tasks.py
    :language: python
    :start-after: __nested_start__
    :end-before: __nested_end__

Then calling ``g`` and ``h`` produces the following behavior.

.. code:: python

    >>> ray.get(g.remote())
    [ObjectRef(b1457ba0911ae84989aae86f89409e953dd9a80e),
     ObjectRef(7c14a1d13a56d8dc01e800761a66f09201104275),
     ObjectRef(99763728ffc1a2c0766a2000ebabded52514e9a6),
     ObjectRef(9c2f372e1933b04b2936bb6f58161285829b9914)]

    >>> ray.get(h.remote())
    [1, 1, 1, 1]

**One limitation** is that the definition of ``f`` must come before the
definitions of ``g`` and ``h`` because as soon as ``g`` is defined, it
will be pickled and shipped to the workers, and so if ``f`` hasn't been
defined yet, the definition will be incomplete.

Yielding Resources While Blocked
--------------------------------

Ray will release CPU resources when being blocked. This prevents
deadlock cases where the nested tasks are waiting for the CPU
resources held by the parent task.
Consider the following remote function.

.. literalinclude:: ../doc_code/nested-tasks.py
    :language: python
    :start-after: __yield_start__
    :end-before: __yield_end__

When a ``g`` task is executing, it will release its CPU resources when it gets
blocked in the call to ``ray.get``. It will reacquire the CPU resources when
``ray.get`` returns. It will retain its GPU resources throughout the lifetime of
the task because the task will most likely continue to use GPU memory.
