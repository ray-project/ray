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

In all cases, ``ray.init()`` will try to automatically find a Ray instance to
connect to. It checks, in order:
1. The ``RAY_ADDRESS`` OS environment variable.
2. The concrete address passed to ``ray.init(address=<address>)``.
3. If no address is provided, the latest Ray instance that was started on the same machine using ``ray start``.

.. _start-ray-init:

Starting Ray on a single machine
--------------------------------

Calling ``ray.init()`` starts a local Ray instance on your laptop/machine. This laptop/machine becomes the  "head node".

.. note::

  In recent versions of Ray (>=1.5), ``ray.init()`` will automatically be called on the first use of a Ray remote API.

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            import ray
            # Other Ray APIs will not work until `ray.init()` is called.
            ray.init()

    .. tab-item:: Java

        .. code-block:: java

            import io.ray.api.Ray;

            public class MyRayApp {

              public static void main(String[] args) {
                // Other Ray APIs will not work until `Ray.init()` is called.
                Ray.init();
                ...
              }
            }

    .. tab-item:: C++

        .. code-block:: c++

            #include <ray/api.h>
            // Other Ray APIs will not work until `ray::Init()` is called.
            ray::Init()

When the process calling ``ray.init()`` terminates, the Ray runtime will also terminate. To explicitly stop or restart Ray, use the shutdown API.

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            import ray
            ray.init()
            ... # ray program
            ray.shutdown()

    .. tab-item:: Java

        .. code-block:: java

            import io.ray.api.Ray;

            public class MyRayApp {

              public static void main(String[] args) {
                Ray.init();
                ... // ray program
                Ray.shutdown();
              }
            }

    .. tab-item:: C++

        .. code-block:: c++

            #include <ray/api.h>
            ray::Init()
            ... // ray program
            ray::Shutdown()

To check if Ray is initialized, use the ``is_initialized`` API.

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            import ray
            ray.init()
            assert ray.is_initialized()

            ray.shutdown()
            assert not ray.is_initialized()

    .. tab-item:: Java

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

    .. tab-item:: C++

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


You can connect to this Ray instance by starting a driver process on the same node as where you ran ``ray start``.
``ray.init()`` will now automatically connect to the latest Ray instance.

.. tab-set::

    .. tab-item:: Python

      .. code-block:: python

        import ray
        ray.init()

    .. tab-item:: java

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

    .. tab-item:: C++

        .. code-block:: c++

          #include <ray/api.h>

          int main(int argc, char **argv) {
            ray::Init();
            ...
          }

        .. code-block:: bash

          RAY_ADDRESS=<address> ./<binary> <args>


You can connect other nodes to the head node, creating a Ray cluster by also calling ``ray start`` on those nodes. See :ref:`on-prem` for more details. Calling ``ray.init()`` on any of the cluster machines will connect to the same Ray cluster.

.. _start-ray-up:

Launching a Ray cluster (``ray up``)
------------------------------------

Ray clusters can be launched with the :ref:`Cluster Launcher <cluster-index>`.
The ``ray up`` command uses the Ray cluster launcher to start a cluster on the cloud, creating a designated "head node" and worker nodes. Underneath the hood, it automatically calls ``ray start`` to create a Ray cluster.

Your code **only** needs to execute on one machine in the cluster (usually the head node). Read more about :ref:`running programs on a Ray cluster <cluster-index>`.

To connect to the Ray cluster, call ``ray.init`` from one of the machines in the cluster. This will connect to the latest Ray cluster:

.. code-block:: python

    ray.init()

Note that the machine calling ``ray up`` will not be considered as part of the Ray cluster, and therefore calling ``ray.init`` on that same machine will not attach to the cluster.

What's next?
------------

Check out our `Deployment section <cluster/index.html>`_ for more information on deploying Ray in different settings, including Kubernetes, YARN, and SLURM.
