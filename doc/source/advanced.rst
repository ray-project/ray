Advanced Usage
==============

This page will cover some more advanced examples of using Ray's flexible programming model.

Nested Remote Functions
-----------------------

Remote functions can call other remote functions, resulting in nested tasks.
For example, consider the following.

.. code:: python

    @ray.remote
    def f():
        return 1

    @ray.remote
    def g():
        # Call f 4 times and return the resulting object IDs.
        return [f.remote() for _ in range(4)]

    @ray.remote
    def h():
        # Call f 4 times, block until those 4 tasks finish,
        # retrieve the results, and return the values.
        return ray.get([f.remote() for _ in range(4)])

Then calling ``g`` and ``h`` produces the following behavior.

.. code:: python

    >>> ray.get(g.remote())
    [ObjectID(b1457ba0911ae84989aae86f89409e953dd9a80e),
     ObjectID(7c14a1d13a56d8dc01e800761a66f09201104275),
     ObjectID(99763728ffc1a2c0766a2000ebabded52514e9a6),
     ObjectID(9c2f372e1933b04b2936bb6f58161285829b9914)]

    >>> ray.get(h.remote())
    [1, 1, 1, 1]

**One limitation** is that the definition of ``f`` must come before the
definitions of ``g`` and ``h`` because as soon as ``g`` is defined, it
will be pickled and shipped to the workers, and so if ``f`` hasn't been
defined yet, the definition will be incomplete.

Circular Dependencies
---------------------

Consider the following remote function.

.. code-block:: python

  @ray.remote(num_cpus=1, num_gpus=1)
  def g():
      return ray.get(f.remote())

When a ``g`` task is executing, it will release its CPU resources when it gets
blocked in the call to ``ray.get``. It will reacquire the CPU resources when
``ray.get`` returns. It will retain its GPU resources throughout the lifetime of
the task because the task will most likely continue to use GPU memory.

Cython Code in Ray
------------------

To use Cython code in Ray, run the following from directory ``$RAY_HOME/examples/cython``:

.. code-block:: bash

   pip install scipy # For BLAS example
   pip install -e .
   python cython_main.py --help

You can import the ``cython_examples`` module from a Python script or interpreter.

Notes
~~~~~

* You **must** include the following two lines at the top of any ``*.pyx`` file:

.. code-block:: python

   #!python
   # cython: embedsignature=True, binding=True

* You cannot decorate Cython functions within a ``*.pyx`` file (there are ways around this, but creates a leaky abstraction between Cython and Python that would be very challenging to support generally). Instead, prefer the following in your Python code:

.. code-block:: python

   some_cython_func = ray.remote(some_cython_module.some_cython_func)

* You cannot transfer memory buffers to a remote function (see ``example8``, which currently fails); your remote function must return a value
* Have a look at ``cython_main.py``, ``cython_simple.pyx``, and ``setup.py`` for examples of how to call, define, and build Cython code, respectively. The Cython `documentation <http://cython.readthedocs.io/>`_ is also very helpful.
* Several limitations come from Cython's own `unsupported <https://github.com/cython/cython/wiki/Unsupported>`_ Python features.
* We currently do not support compiling and distributing Cython code to ``ray`` clusters. In other words, Cython developers are responsible for compiling and distributing any Cython code to their cluster (much as would be the case for users who need Python packages like ``scipy``).
* For most simple use cases, developers need not worry about Python 2 or 3, but users who do need to care can have a look at the ``language_level`` Cython compiler directive (see `here <http://cython.readthedocs.io/en/latest/src/reference/compilation.html>`_).
