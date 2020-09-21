.. _cluster-index:

Distributed Ray Overview
========================

One of Ray's strengths is the ability to leverage multiple machines in the same program. Ray can, of course, be run on a single machine (and is done so often) but the real power is using Ray on a cluster of machines.

Key Concepts
------------

* **Ray Nodes**: A Ray cluster consists of a **head node** and a set of **worker nodes**. The head node needs to be started first, and the worker nodes are given the address of the head node to form the cluster. The Ray cluster itself can also "auto-scale," meaning that it can interact with a Cloud Provider to request or release instances according to application workload.

* **Ports**: Ray processes communicate via TCP ports. When starting a Ray cluster, either on prem or on the cloud, it is important to open the right ports so that Ray functions correctly. See :ref:`the Ray Ports documentation <ray-ports>` for more details.

* **Ray Cluster Launcher**: The :ref:`Ray Cluster Launcher <ref-automatic-cluster>` is a simple tool that automatically provisions machines and launches a multi-node Ray cluster. You can use the cluster launcher on GCP, Amazon EC2, Azure, or even Kubernetes.

Summary
-------

Clusters are started with the :ref:`Ray Cluster Launcher <ref-automatic-cluster>` or :ref:`manually <manual-cluster>`.

You can also create a Ray cluster using a standard cluster manager such as :ref:`Kubernetes <ray-k8s-deploy>`, :ref:`YARN <ray-yarn-deploy>`, or :ref:`SLURM <ray-slurm-deploy>`.

After a cluster is started, you need to connect your program to the Ray cluster.

.. tabs::
  .. group-tab:: python

    You can connect to this Ray runtime by starting a Python process that calls the following on the same node as where you ran ``ray start``:

    .. code-block:: python

      # This must
      import ray
      ray.init(address='auto')

  .. group-tab:: java

    If you want to run Java code, you need to specify the classpath via the ``--code-search-path`` option. See :ref:`code_search_path` for more details.

    .. code-block:: bash

      $ ray start ... --code-search-path=/path/to/jars

and then the rest of your script should be able to leverage Ray as a distributed framework!


Using the cluster launcher
--------------------------

The ``ray up`` command uses the :ref:`Ray Cluster Launcher <ref-automatic-cluster>` to start a cluster on the cloud, creating a designated "head node" and worker nodes. Any Python process that runs ``ray.init(address=...)`` on any of the cluster nodes will connect to the ray cluster.

.. important:: Calling ``ray.init`` on your laptop will not work if using ``ray up``, since your laptop will not be the head node.

Here is an example of using the Cluster Launcher on AWS:

.. code-block:: shell

    # First, run `pip install boto3` and `aws configure`
    #
    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

You can monitor the Ray cluster status with ``ray monitor cluster.yaml`` and ssh into the head node with ``ray attach cluster.yaml``.

.. _manual-cluster:

Manual Ray Cluster Setup
------------------------

The most preferable way to run a Ray cluster is via the :ref:`Ray Cluster Launcher <ref-automatic-cluster>`. However, it is also possible to start a Ray cluster by hand.

This section assumes that you have a list of machines and that the nodes in the cluster can communicate with each other. It also assumes that Ray is installed
on each machine. To install Ray, follow the `installation instructions`_.

To configure the Ray cluster to run Java code, you need to add the ``--code-search-path`` option. See :ref:`code_search_path` for more details.

.. _`installation instructions`: http://docs.ray.io/en/latest/installation.html

Starting Ray on each machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the head node (just choose some node to be the head node), run the following.
If the ``--port`` argument is omitted, Ray will choose port 6379, falling back to a
random port.

.. code-block:: bash

  ray start --head --port=6379

The command will print out the address of the Redis server that was started
(and some other address information).

**Then on all of the other nodes**, run the following. Make sure to replace
``<address>`` with the value printed by the command on the head node (it
should look something like ``123.45.67.89:6379``).

.. code-block:: bash

  ray start --address=<address>

If you wish to specify that a machine has 10 CPUs and 1 GPU, you can do this
with the flags ``--num-cpus=10`` and ``--num-gpus=1``. See the :ref:`Configuration <configuring-ray>` page for more information.

Now we've started the Ray runtime.

Stopping Ray
~~~~~~~~~~~~

When you want to stop the Ray processes, run ``ray stop`` on each node.

.. _using-ray-on-a-cluster:

Running a Ray program on the Ray cluster
----------------------------------------

To run a distributed Ray program, you'll need to execute your program on the same machine as one of the nodes.

.. tabs::
  .. group-tab:: Python

    Within your program/script, you must call ``ray.init`` and add the ``address`` parameter to ``ray.init`` (like ``ray.init(address=...)``). This causes Ray to connect to the existing cluster. For example:

    .. code-block:: python

        ray.init(address="auto")

  .. group-tab:: Java

    You need to add the ``ray.address`` parameter to your command line (like ``-Dray.address=...``).

    To connect your program to the Ray cluster, run it like this:

        .. code-block:: bash

            java -classpath /path/to/jars/ \
              -Dray.address=<address> \
              <classname> <args>

    .. note:: Specifying ``auto`` as the address hasn't been implemented in Java yet. You need to provide the actual address. You can find the address of the server from the output of the ``ray up`` command.


.. note:: A common mistake is setting the address to be a cluster node while running the script on your laptop. This will not work because the script needs to be started/executed on one of the Ray nodes.

To verify that the correct number of nodes have joined the cluster, you can run the following.

.. code-block:: python

  import time

  @ray.remote
  def f():
      time.sleep(0.01)
      return ray.services.get_node_ip_address()

  # Get a list of the IP addresses of the nodes that have joined the cluster.
  set(ray.get([f.remote() for _ in range(1000)]))
