Antipattern: Redefining task or actor in loop
=============================================

**TLDR:** Limit the number of times you re-define a remote function.

Decorating the same function or class multiple times using the ``@ray.remote`` decorator leads to slow performance in Ray. This is because Ray has to export all the function and class definitions to all Ray workers.

Instead, define tasks and actors outside of the loop instead multiple times inside a loop.


Code example
------------

**Antipattern:**

.. code-block:: python

    outputs = []
    for i in range(10):
        @ray.remote
        def exp(i, j):
            return i**j
        step_i_out = ray.get([exp.remote(i, j) for j in range(10)])
        outputs.append(step_i_out)


**Better approach:**

.. code-block:: python

    @ray.remote
    def exp(i, j):
        return i**j

    outputs = []
    for i in range(10):
        step_i_out = ray.get([exp.remote(i, j) for j in range(10)])
        outputs.append(step_i_out)
