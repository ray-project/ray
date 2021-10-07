Antipattern: Too fine-grained tasks
===================================

**TLDR:** Avoid over-parallelizing. Parallelizing tasks has higher overhead than using normal functions.

Parallelizing or distributing tasks usually comes with higher overhead than an ordinary function call. Therefore, if you parallelize a function that executes very quickly, the overhead could take longer than the actual function call!

To handle this problem, we should be careful about parallelizing too much. If you have a function or task that’s too small, you can use a technique called batching to make your tasks do more meaningful work in a single task.


Code example
------------

**Antipattern:**

.. code-block:: python

    @ray.remote
    def double(number):
        return number * 2

    numbers = list(range(10000))

    doubled_numbers = []
    for i in numbers:
        doubled_numbers.append(ray.get(double.remote(i)))

**Better approach:** Use batching.

.. code-block:: python

    @ray.remote
    def double_list(list_of_numbers):
        return [number * 2 for number in list_of_numbers]

    numbers = list(range(10000))
    doubled_list_refs = []
    BATCH_SIZE = 100
    for i in range(0, len(numbers), BATCH_SIZE):
        batch = numbers[i : i + BATCH_SIZE]
        doubled_list_refs.append(double_list.remote(batch))

    doubled_numbers = []
