Advanced Usage
==============

This page will cover some more advanced examples of using Ray's flexible programming model.

Dynamic Remote Parameters
-------------------------

You can dynamically adjust resource requirements or return values of ``ray.remote`` during execution with ``.options``.

For example, here we instantiate many copies of the same actor with varying resource requirements. Note that to create these actors successfully, Ray will need to be started with sufficient CPU resources and the relevant custom resources:

.. code-block:: python

  @ray.remote(num_cpus=4)
  class Counter(object):
      def __init__(self):
          self.value = 0

      def increment(self):
          self.value += 1
          return self.value

  a1 = Counter.options(num_cpus=1, resources={"Custom1": 1}).remote()
  a2 = Counter.options(num_cpus=2, resources={"Custom2": 1}).remote()
  a3 = Counter.options(num_cpus=3, resources={"Custom3": 1}).remote()

You can specify different resource requirements for tasks (but not for actor methods):

.. code-block:: python

    @ray.remote
    def g():
        return ray.get_gpu_ids()

    object_gpu_ids = g.remote()
    assert ray.get(object_gpu_ids) == [0]

    dynamic_object_gpu_ids = g.options(num_cpus=1, num_gpus=1).remote()
    assert ray.get(dynamic_object_gpu_ids) == [0]

And vary the number of return values for tasks (and actor methods too):

.. code-block:: python

    @ray.remote
    def f(n):
        return list(range(n))

    id1, id2 = f.options(num_return_vals=2).remote(2)
    assert ray.get(id1) == 0
    assert ray.get(id2) == 1


Dynamic Custom Resources
------------------------

Ray enables explicit developer control with respect to the task and actor placement by using custom resources. Further, users are able to dynamically adjust custom resources programmatically with ``ray.experimental.set_resource``. This allows the Ray application to implement virtually any scheduling policy, including task affinity, data locality, anti-affinity,
load balancing, gang scheduling, and priority-based scheduling.


.. code-block:: python

    ray.init()
    resource_name = "test_resource"
    resource_capacity = 1.0

    @ray.remote
    def set_resource(resource_name, resource_capacity):
        ray.experimental.set_resource(resource_name, resource_capacity)

    ray.get(set_resource.remote(resource_name, resource_capacity))

    available_resources = ray.available_resources()
    cluster_resources = ray.cluster_resources()

    assert available_resources[resource_name] == resource_capacity
    assert cluster_resources[resource_name] == resource_capacity


.. autofunction:: ray.experimental.set_resource
    :noindex:



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
        # Call f 4 times and return the resulting object refs.
        return [f.remote() for _ in range(4)]

    @ray.remote
    def h():
        # Call f 4 times, block until those 4 tasks finish,
        # retrieve the results, and return the values.
        return ray.get([f.remote() for _ in range(4)])

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

Inspecting Cluster State
------------------------

Applications written on top of Ray will often want to have some information
or diagnostics about the cluster. Some common questions include:

    1. How many nodes are in my autoscaling cluster?
    2. What resources are currently available in my cluster, both used and total?
    3. What are the objects currently in my cluster?

For this, you can use the global state API.

Node Information
~~~~~~~~~~~~~~~~

To get information about the current nodes in your cluster, you can use ``ray.nodes()``:

.. autofunction:: ray.nodes
    :noindex:


.. code-block:: python

    import ray

    ray.init()

    print(ray.nodes())

    """
    [{'ClientID': 'a9e430719685f3862ed7ba411259d4138f8afb1e',
      'IsInsertion': True,
      'NodeManagerAddress': '192.168.19.108',
      'NodeManagerPort': 37428,
      'ObjectManagerPort': 43415,
      'ObjectStoreSocketName': '/tmp/ray/session_2019-07-28_17-03-53_955034_24883/sockets/plasma_store',
      'RayletSocketName': '/tmp/ray/session_2019-07-28_17-03-53_955034_24883/sockets/raylet',
      'Resources': {'CPU': 4.0},
      'alive': True}]
    """

The above information includes:

  - `ClientID`: A unique identifier for the raylet.
  - `alive`: Whether the node is still alive.
  - `NodeManagerAddress`: PrivateIP of the node that the raylet is on.
  - `Resources`: The total resource capacity on the node.

Resource Information
~~~~~~~~~~~~~~~~~~~~

To get information about the current total resource capacity of your cluster, you can use ``ray.cluster_resources()``.

.. autofunction:: ray.cluster_resources
    :noindex:


To get information about the current available resource capacity of your cluster, you can use ``ray.available_resources()``.

.. autofunction:: ray.available_resources
    :noindex:

Detached Actors
-----------------------------------

When original actor handles goes out of scope or the driver that originally
created the actor exits, ray will clean up the actor by default. If you want
to make sure the actor is kept alive, you can use
``_remote(name="some_name")`` to keep the actor alive after
the driver exits. The actor will have a globally unique name and can be
accessed across different drivers.

For example, you can instantiate and register a persistent actor as follows:

.. code-block:: python

  counter = Counter.options(name="CounterActor").remote()

The CounterActor will be kept alive even after the driver running above script
exits. Therefore it is possible to run the following script in a different
driver:

.. code-block:: python

  counter = ray.get_actor("CounterActor")
  print(ray.get(counter.get_counter.remote()))
