Advanced Usage
==============

This page will cover some more advanced examples of using Ray's flexible programming model.

.. contents::
  :local:

Synchronization
---------------

Tasks or actors can often contend over the same resource or need to communicate with each other. Here are some standard ways to perform synchronization across Ray processes.

Inter-process synchronization using FileLock
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you have several tasks or actors writing to the same file or downloading a file on a single node, you can use `FileLock <https://pypi.org/project/filelock/>`_ to synchronize.

This often occurs for data loading and preprocessing.

.. code-block:: python

    import ray
    from filelock import FileLock

    @ray.remote
    def write_to_file(text):
        # Create a filelock object. Consider using an absolute path for the lock.
        with FileLock("my_data.txt.lock"):
            with open("my_data.txt","a") as f:
                f.write(text)

    ray.init()
    ray.get([write_to_file.remote("hi there!\n") for i in range(3)])

    with open("my_data.txt") as f:
        print(f.read())

    ## Output is:

    # hi there!
    # hi there!
    # hi there!

Multi-node synchronization using an Actor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you have multiple tasks that need to wait on some condition or otherwise
need to synchronize across tasks & actors on a cluster, you can use a central
actor to coordinate among them. Below is an example of using a ``SignalActor``
that wraps an ``asyncio.Event`` for basic synchronization.

.. code-block:: python

    import asyncio

    import ray

    ray.init()

    # We set num_cpus to zero because this actor will mostly just block on I/O.
    @ray.remote(num_cpus=0)
    class SignalActor:
        def __init__(self):
            self.ready_event = asyncio.Event()

        def send(self, clear=False):
            self.ready_event.set()
            if clear:
                self.ready_event.clear()

        async def wait(self, should_wait=True):
            if should_wait:
                await self.ready_event.wait()

    @ray.remote
    def wait_and_go(signal):
        ray.get(signal.wait.remote())

        print("go!")

    signal = SignalActor.remote()
    tasks = [wait_and_go.remote(signal) for _ in range(4)]
    print("ready...")
    # Tasks will all be waiting for the signals.
    print("set..")
    ray.get(signal.send.remote())

    # Tasks are unblocked.
    ray.get(tasks)

    ##  Output is:
    # ready...
    # get set..

    # (pid=77366) go!
    # (pid=77372) go!
    # (pid=77367) go!
    # (pid=77358) go!


Message passing using Ray Queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes just using one signal to synchronize is not enough. If you need to send data among many tasks or
actors, you can use :ref:`ray.util.queue.Queue <ray-queue-ref>`.

.. code-block:: python

    import ray
    from ray.util.queue import Queue

    ray.init()
    # You can pass this object around to different tasks/actors
    queue = Queue(maxsize=100)

    @ray.remote
    def consumer(queue):
        next_item = queue.get(block=True)
        print(f"got work {next_item}")

    consumers = [consumer.remote(queue) for _ in range(2)]

    [queue.put(i) for i in range(10)]

Ray's Queue API has similar API as Python's ``asyncio.Queue`` and ``queue.Queue``.


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

    id1, id2 = f.options(num_returns=2).remote(2)
    assert ray.get(id1) == 0
    assert ray.get(id2) == 1

And specify a name for tasks (and actor methods too) at task submission time:

.. code-block:: python

   import setproctitle

   @ray.remote
   def f(x):
      assert setproctitle.getproctitle() == "ray::special_f"
      return x + 1

   obj = f.options(name="special_f").remote(3)
   assert ray.get(obj) == 4

This name will appear as the task name in the machine view of the dashboard, will appear
as the worker process name when this task is executing (if a Python task), and will
appear as the task name in the logs.

.. image:: images/task_name_dashboard.png


Accelerator Types
------------------

Ray supports resource specific accelerator types. The `accelerator_type` field can be used to force to a task to run on a node with a specific type of accelerator. Under the hood, the accelerator type option is implemented as a custom resource demand of ``"accelerator_type:<type>": 0.001``. This forces the task to be placed on a node with that particular accelerator type available. This also lets the multi-node-type autoscaler know that there is demand for that type of resource, potentially triggering the launch of new nodes providing that accelerator.

.. code-block:: python

    from ray.accelerators import NVIDIA_TESLA_V100

    @ray.remote(num_gpus=1, accelerator_type=NVIDIA_TESLA_V100)
    def train(data):
        return "This function was run on a node with a Tesla V100 GPU"

See `ray.util.accelerators` to see available accelerator types. Current automatically detected accelerator types include Nvidia GPUs.


Overloaded Functions
--------------------
Ray Java API supports calling overloaded java functions remotely. However, due to the limitation of Java compiler type inference, one must explicitly cast the method reference to the correct function type. For example, consider the following.

Overloaded normal task call:

.. code:: java

    public static class MyRayApp {

      public static int overloadFunction() {
        return 1;
      }

      public static int overloadFunction(int x) {
        return x;
      }
    }

    // Invoke overloaded functions.
    Assert.assertEquals((int) Ray.task((RayFunc0<Integer>) MyRayApp::overloadFunction).remote().get(), 1);
    Assert.assertEquals((int) Ray.task((RayFunc1<Integer, Integer>) MyRayApp::overloadFunction, 2).remote().get(), 2);

Overloaded actor task call:

.. code:: java

    public static class Counter {
      protected int value = 0;

      public int increment() {
        this.value += 1;
        return this.value;
      }
    }

    public static class CounterOverloaded extends Counter {
      public int increment(int diff) {
        super.value += diff;
        return super.value;
      }

      public int increment(int diff1, int diff2) {
        super.value += diff1 + diff2;
        return super.value;
      }
    }

.. code:: java

    ActorHandle<CounterOverloaded> a = Ray.actor(CounterOverloaded::new).remote();
    // Call an overloaded actor method by super class method reference.
    Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 1);
    // Call an overloaded actor method, cast method reference first.
    a.task((RayFunc1<CounterOverloaded, Integer>) CounterOverloaded::increment).remote();
    a.task((RayFunc2<CounterOverloaded, Integer, Integer>) CounterOverloaded::increment, 10).remote();
    a.task((RayFunc3<CounterOverloaded, Integer, Integer, Integer>) CounterOverloaded::increment, 10, 10).remote();
    Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 33);


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
    [{'NodeID': '2691a0c1aed6f45e262b2372baf58871734332d7',
      'Alive': True,
      'NodeManagerAddress': '192.168.1.82',
      'NodeManagerHostname': 'host-MBP.attlocal.net',
      'NodeManagerPort': 58472,
      'ObjectManagerPort': 52383,
      'ObjectStoreSocketName': '/tmp/ray/session_2020-08-04_11-00-17_114725_17883/sockets/plasma_store',
      'RayletSocketName': '/tmp/ray/session_2020-08-04_11-00-17_114725_17883/sockets/raylet',
      'MetricsExportPort': 64860,
      'alive': True,
      'Resources': {'CPU': 16.0, 'memory': 100.0, 'object_store_memory': 34.0, 'node:192.168.1.82': 1.0}}]
    """

The above information includes:

  - `NodeID`: A unique identifier for the raylet.
  - `alive`: Whether the node is still alive.
  - `NodeManagerAddress`: PrivateIP of the node that the raylet is on.
  - `Resources`: The total resource capacity on the node.
  - `MetricsExportPort`: The port number at which metrics are exposed to through a `Prometheus endpoint <ray-metrics.html>`_.

Resource Information
~~~~~~~~~~~~~~~~~~~~

To get information about the current total resource capacity of your cluster, you can use ``ray.cluster_resources()``.

.. autofunction:: ray.cluster_resources
    :noindex:


To get information about the current available resource capacity of your cluster, you can use ``ray.available_resources()``.

.. autofunction:: ray.available_resources
    :noindex:

.. _runtime-environments-old:

Runtime Environments
--------------------

:ref:`This section has moved.<runtime-environments>`