Tips for testing Ray programs
=============================

Ray programs can be a little tricky to test due to the nature of parallel programs. We've put together a list of tips and tricks for common testing practices for Ray programs.

.. contents::
  :local:

Tip 1: Fixing the resource quantity with ``ray.init(num_cpus=...)``
-------------------------------------------------------------------

By default, ``ray.init()`` detects the number of CPUs and GPUs on your local machine/cluster.

However, your testing environment may have a significantly lower number of resources. For example, the TravisCI build environment only has `2 cores <https://docs.travis-ci.com/user/reference/overview/>`_

If tests are written to depend on ``ray.init()``, they may be implicitly written in a way that relies on a larger multi-core machine.

This may easily result in tests exhibiting unexpected, flaky, or faulty behavior that is hard to reproduce.

To overcome this, you should override the detected resources by setting them in ``ray.init`` like: ``ray.init(num_cpus=2)``


.. _local-mode-tips:

Tip 2: Use ``ray.init(local_mode=True)`` if possible
----------------------------------------------------

A test suite for a Ray program may take longer to run than other test suites. One common culprit for long test durations is the overheads from inter-process communication.

Ray provides a local mode for running Ray programs in a single process via ``ray.init(local_mode=True)``. This can be especially useful for testing since it allows you to reduce/remove inter-process communication.

However, there are some caveats with using this. You should not do this if:

1. If your application depends on setting environment variables per process
2. If your application has recursive actor calls
3. If your remote actor/task sets any sort of process-level global variables
4. If you use are using async actors

Also note, if you are using GPUs, you must set the ``CUDA_VISIBLE_DEVICES`` environment
variable to a comma separated list of your GPU Device IDs.


Tip 3: Sharing the ray cluster across tests if possible
--------------------------------------------------------

It is safest to start a new ray cluster for each test.

.. code-block:: python

    class RayTest(unittest.TestCase):
        def setUp(self):
            ray.init(num_cpus=4, num_gpus=0)

        def tearDown(self):
            ray.shutdown()

However, starting and stopping a Ray cluster can actually incur a non-trivial amount of latency. For example, on a typical Macbook Pro laptop, starting and stopping can take nearly 5 seconds:

.. code-block:: bash

    python -c 'import ray; ray.init(); ray.shutdown()'  3.93s user 1.23s system 116% cpu 4.420 total

Across 20 tests, this ends up being 90 seconds of added overhead.

Reusing a Ray cluster across tests can provide significant speedups to your test suite. This reduces the overhead to a constant, amortized quantity:

.. code-block:: python

    class RayClassTest(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            # Start it once for the entire test suite/module
            ray.init(num_cpus=4, num_gpus=0)

        @classmethod
        def tearDownClass(cls):
            ray.shutdown()

Depending on your application, there are certain cases where it may be unsafe to reuse a Ray cluster across tests. For example:

1. If your application depends on setting environment variables per process.
2. If your remote actor/task sets any sort of process-level global variables.


Tip 4: Create a mini-cluster with ``ray.cluster_utils.Cluster``
---------------------------------------------------------------

If writing an application for a cluster setting, you may want to mock a multi-node Ray cluster. This can be done with the ``ray.cluster_utils.Cluster`` utility.

.. note::

  On Windows, support for multi-node Ray clusters is currently experimental and untested.
  If you run into issues please file a report at https://github.com/ray-project/ray/issues.

.. code-block:: python

    from ray.cluster_utils import Cluster

    # Starts a head-node for the cluster.
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 10,
        })

After starting a cluster, you can execute a typical ray script in the same process:

.. code-block:: python

    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return x

    for _ in range(1):
        ray.get([f.remote(1) for _ in range(1000)])

    for _ in range(10):
        ray.get([f.remote(1) for _ in range(100)])

    for _ in range(100):
        ray.get([f.remote(1) for _ in range(10)])

    for _ in range(1000):
        ray.get([f.remote(1) for _ in range(1)])


You can also add multiple nodes, each with different resource quantities:

.. code-block:: python

    mock_node = cluster.add_node(num_cpus=10)

    assert ray.cluster_resources()["CPU"] == 20

You can also remove nodes, which is useful when testing failure-handling logic:

.. code-block:: python

    cluster.remove_node(mock_node)

    assert ray.cluster_resources()["CPU"] == 10

See the `Cluster Util for more details <https://github.com/ray-project/ray/blob/master/python/ray/cluster_utils.py>`_.


Tip 5: Be careful when running tests in parallel
------------------------------------------------

Since Ray starts a variety of services, it is easy to trigger timeouts if too many services are started at once. Therefore, when using tools such as `pytest xdist <https://pypi.org/project/pytest-xdist/>`_ that run multiple tests in parallel, one should keep in mind that this may introduce flakiness into the test environment.
