Starting Ray
============

This page covers how to start Ray on your single machine or cluster of machines.

.. tip:: Be sure to have :ref:`installed Ray <installation>` before following the instructions on this page.


What is the Ray runtime?
------------------------

Ray programs are able to parallelize and distribute by leveraging an underlying *Ray runtime*.
The Ray runtime consists of multiple services/processes started in the background for communication, data transfer, scheduling, and more. The Ray runtime can be started on a laptop, a single server, or multiple servers.

There are three ways of starting the Ray runtime:

* Implicitly via ``ray.init()`` (:ref:`start-ray-init`)
* Explicitly via CLI (:ref:`start-ray-cli`)
* Explicitly via the cluster launcher (:ref:`start-ray-up`)

.. _start-ray-init:

Starting Ray on a single machine
--------------------------------

Calling ``ray.init()`` (without any ``address`` args) starts a Ray runtime on your laptop/machine. This laptop/machine becomes the  "head node".

.. note::

  In recent versions of Ray (>=1.5), ``ray.init()`` will automatically be called on the first use of a Ray remote API.

.. tabs::
  .. code-tab:: python

    import ray
    # Other Ray APIs will not work until `ray.init()` is called.
    ray.init()

  .. code-tab:: java

    import io.ray.api.Ray;

    public class MyRayApp {

      public static void main(String[] args) {
        // Other Ray APIs will not work until `Ray.init()` is called.
        Ray.init();
        ...
      }
    }

  .. code-tab:: c++

    #include <ray/api.h>
    // Other Ray APIs will not work until `ray::Init()` is called.
    ray::Init()

When the process calling ``ray.init()`` terminates, the Ray runtime will also terminate. To explicitly stop or restart Ray, use the shutdown API.

.. tabs::
  .. code-tab:: python

    import ray
    ray.init()
    ... # ray program
    ray.shutdown()

  .. code-tab:: java

    import io.ray.api.Ray;

    public class MyRayApp {

      public static void main(String[] args) {
        Ray.init();
        ... // ray program
        Ray.shutdown();
      }
    }

  .. code-tab:: c++

    #include <ray/api.h>
    ray::Init()
    ... // ray program
    ray::Shutdown()

.. tabs::
  .. group-tab:: Python

    To check if Ray is initialized, you can call ``ray.is_initialized()``:

    .. code-block:: python

      import ray
      ray.init()
      assert ray.is_initialized() == True

      ray.shutdown()
      assert ray.is_initialized() == False

  .. group-tab:: Java

    To check if Ray is initialized, you can call ``Ray.isInitialized()``:

    .. code-block:: java

      import io.ray.api.Ray;

      public class MyRayApp {

        public static void main(String[] args) {
          Ray.init();
          Assert.assertTrue(Ray.isInitialized());
          Ray.shutdown();
          Assert.assertFalse(Ray.isInitialized());
        }
      }

  .. group-tab:: C++

    To check if Ray is initialized, you can call ``ray::IsInitialized()``:

    .. code-block:: c++

      #include <ray/api.h>

      int main(int argc, char **argv) {
        ray::Init();
        assert(ray::IsInitialized());

        ray::Shutdown();
        assert(!ray::IsInitialized());
      }

See the `Configuration <configure.html>`__ documentation for the various ways to configure Ray.

.. _start-ray-cli:

Starting Ray via the CLI (``ray start``)
----------------------------------------

Use ``ray start`` from the CLI to start a 1 node ray runtime on a machine. This machine becomes the "head node".

.. code-block:: bash

  $ ray start --head --port=6379

  Local node IP: 192.123.1.123
  2020-09-20 10:38:54,193 INFO services.py:1166 -- View the Ray dashboard at http://localhost:8265

  --------------------
  Ray runtime started.
  --------------------

  ...


You can connect to this Ray runtime by starting a driver process on the same node as where you ran ``ray start``:

.. tabs::
  .. code-tab:: python

    # This must
    import ray
    ray.init(address='auto')

  .. group-tab:: java

    .. code-block:: java

      import io.ray.api.Ray;

      public class MyRayApp {

        public static void main(String[] args) {
          Ray.init();
          ...
        }
      }

    .. code-block:: bash

      java -classpath <classpath> \
        -Dray.address=<address> \
        <classname> <args>

  .. group-tab:: C++

    .. code-block:: c++

      #include <ray/api.h>

      int main(int argc, char **argv) {
        ray::Init();
        ...
      }

    .. code-block:: bash

      RAY_ADDRESS=<address> ./<binary> <args>


You can connect other nodes to the head node, creating a Ray cluster by also calling ``ray start`` on those nodes. See :ref:`manual-cluster` for more details. Calling ``ray.init(address="auto")`` on any of the cluster machines will connect to the ray cluster.

.. _start-ray-up:

Launching a Ray cluster (``ray up``)
------------------------------------

Ray clusters can be launched with the :ref:`Cluster Launcher <cluster-cloud>`.
The ``ray up`` command uses the Ray cluster launcher to start a cluster on the cloud, creating a designated "head node" and worker nodes. Underneath the hood, it automatically calls ``ray start`` to create a Ray cluster.

Your code **only** needs to execute on one machine in the cluster (usually the head node). Read more about :ref:`running programs on a Ray cluster <using-ray-on-a-cluster>`.

To connect to the existing cluster, similar to the method outlined in :ref:`start-ray-cli`, you must call ``ray.init`` and specify the address of the Ray cluster when initializing Ray in your code. This allows Ray to connect to the cluster.

.. code-block:: python

    ray.init(address="auto")

Note that the machine calling ``ray up`` will not be considered as part of the Ray cluster, and therefore calling ``ray.init`` on that same machine will not attach to the cluster.

.. _local_mode:

Local mode
----------

.. caution:: This feature is maintained solely to help with debugging, so it's possible you may encounter some issues. If you do, please `file an issue <https://github.com/ray-project/ray/issues>`_.

By default, Ray will parallelize its workload and run tasks on multiple processes and multiple nodes. However, if you need to debug your Ray program, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process by enabling local mode as the following:

.. tabs::

  .. code-tab:: python

    ray.init(local_mode=True)

  .. group-tab:: Java

    .. code-block:: bash

      java -classpath <classpath> \
        -Dray.local-mode=true \
        <classname> <args>

    .. note:: If you just want to run your Java code in local mode, you can run it without Ray or even Python installed.

  .. group-tab:: C++

    .. code-block:: c++

      RayConfig config;
      config.local_mode = true;
      ray::Init(config);

    .. note:: If you just want to run your C++ code in local mode, you can run it without Ray or even Python installed.

Note that there are some known issues with local mode. Please read :ref:`these tips <local-mode-tips>` for more information.


What's next?
------------

Check out our `Deployment section <cluster/index.html>`_ for more information on deploying Ray in different settings, including Kubernetes, YARN, and SLURM.
