Antipattern: Closure capture of large / unserializable object
=============================================================

**TLDR:** Be careful when using large objects in ``@ray.remote`` functions or classes.

When you define a ``ray.remote`` function or class, it is easy to accidentally capture large (more than a few MB) objects implicitly in the function definition. This can lead to slow performance or ``MemoryError`` when attempting to define the function, since Ray is not designed to handle serialized functions or classes that are very large.

For such large objects, there are a couple options to resolve this problem:
- Use ``ray.put`` to put the object in the Ray object store, and then use ``ray.get`` to get a view of the object within the task (*"better approach #1"* below)
- Create the object inside the task instead of in the driver script by passing a lambda method (*"better approach #2"*)
- The second method is the only option available for unserializable objects.



Code example
------------

**Antipattern:**

.. code-block:: python

    # Create a 838 MB array, verify via: sys.getsizeof(big_array)
    big_array = np.zeros(100 * 1024 * 1024)

    @ray.remote
    def f():
        return len(big_array) # big_array is serialized along with f!

    ray.init()
    ray.get(f.remote())

**Better approach #1:**

.. code-block:: python

    big_array = ray.put(np.zeros(100 * 1024 * 1024))

    @ray.remote
    def f():
        return len(ray.get(big_array))

    ray.init()
    ray.get(f.remote())

**Better approach #2:**

.. code-block:: python

    array_creator = lambda: np.zeros(100 * 1024 * 1024)

    @ray.remote
    def f():
        array = array_creator()
        return len(array)

    ray.init()
    ray.get(f.remote())
