.. _cluster-index:

Distributed Ray Overview
========================

One of Ray's strengths is the ability to leverage multiple machines in the same program Ray can, of course, be run on a single machine (and is done so often) but the real power is using Ray on a cluster of machines.

Key Concepts
------------

* **Ray Nodes**: A Ray cluster consists of a **head node** and a set of **worker nodes**. The head node needs to be started first, and the worker nodes are given the address of the head node to form the cluster. The Ray cluster itself can also "auto-scale," meaning that it can interact with a Cloud Provider to request or release instances according to application workload.

* **Ports**: Ray processes communicate via TCP ports. When starting a Ray cluster, either on prem or on the cloud, it is important to open the right ports so that Ray functions correctly.

* **Ray Cluster Launcher**: The :ref:`Ray Cluster Launcher <ref-automatic-cluster>` is a simple tool that automatically provisions machines and launches a multi-node Ray cluster. You can use the cluster launcher on GCP, Amazon EC2, Azure, or even Kubernetes.

Starting a Ray cluster
----------------------

Clusters can be started with the :ref:`Cluster Launcher <ref-automatic-cluster>` or :ref:`manually <manual-cluster>`. You can also create a Ray cluster using a standard cluster manager such as :ref:`Kubernetes <ray-k8s-deploy>`, :ref:`YARN <ray-yarn-deploy>`, or :ref:`SLURM <ray-slurm-deploy>`.

Here is an example of using the Cluster Launcher on AWS:

.. code-block:: shell

    # First, run `pip install boto3` and `aws configure`
    #
    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

Running a Ray program on the Ray cluster
----------------------------------------

To run a distributed Ray program, you'll need to execute your program on the same machine as one of the nodes. For example, start up Python on one of the nodes in the cluster:

.. code-block:: python

  import ray
  ray.init(address="auto")

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

.. _manual-cluster:

Manual Ray Cluster Setup
------------------------

The most preferable way to run a Ray cluster is via the Ray Cluster Launcher. However, it is also possible to start a Ray cluster by hand.

This section assumes that you have a list of machines and that the nodes in the cluster can communicate with each other. It also assumes that Ray is installed
on each machine. To install Ray, follow the `installation instructions`_.

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

Now we've started all of the Ray processes on each node Ray. This includes

- Some worker processes on each machine.
- An object store on each machine.
- A raylet on each machine.
- Multiple Redis servers (on the head node).

Stopping Ray
~~~~~~~~~~~~

When you want to stop the Ray processes, run ``ray stop`` on each node.
