Using Ray on a Cluster
======================

.. note::

    Starting with Ray 0.4.0 if you're using AWS you can use the automated `setup commands <http://ray.readthedocs.io/en/latest/autoscaling.html>`__.

The instructions in this document work well for small clusters. For larger
clusters, follow the instructions for `managing a cluster with parallel ssh`_.

.. _`managing a cluster with parallel ssh`: http://ray.readthedocs.io/en/latest/using-ray-on-a-large-cluster.html

Deploying Ray on a Cluster
--------------------------

This section assumes that you have a cluster running and that the nodes in the
cluster can communicate with each other. It also assumes that Ray is installed
on each machine. To install Ray, follow the instructions for
`installation on Ubuntu`_.

.. _`installation on Ubuntu`: http://ray.readthedocs.io/en/latest/install-on-ubuntu.html

Starting Ray on each machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the head node (just choose some node to be the head node), run the following.
If the ``--redis-port`` argument is omitted, Ray will choose a port at random.

.. code-block:: bash

  ray start --head --redis-port=6379

The command will print out the address of the Redis server that was started
(and some other address information).

Then on all of the other nodes, run the following. Make sure to replace
``<redis-address>`` with the value printed by the command on the head node (it
should look something like ``123.45.67.89:6379``).

.. code-block:: bash

  ray start --redis-address=<redis-address>

If you wish to specify that a machine has 10 CPUs and 1 GPU, you can do this
with the flags ``--num-cpus=10`` and ``--num-gpus=1``. If these flags are not
used, then Ray will detect the number of CPUs automatically and will assume
there are 0 GPUs.

Now we've started all of the Ray processes on each node Ray. This includes

- Some worker processes on each machine.
- An object store on each machine.
- A local scheduler on each machine.
- Multiple Redis servers (on the head node).
- One global scheduler (on the head node).

To run some commands, start up Python on one of the nodes in the cluster, and do
the following.

.. code-block:: python

  import ray
  ray.init(redis_address="<redis-address>")

Now you can define remote functions and execute tasks. For example, to verify
that the correct number of nodes have joined the cluster, you can run the
following.

.. code-block:: python

  import time

  @ray.remote
  def f():
      time.sleep(0.01)
      return ray.services.get_node_ip_address()

  # Get a list of the IP addresses of the nodes that have joined the cluster.
  set(ray.get([f.remote() for _ in range(1000)]))

Stopping Ray
~~~~~~~~~~~~

When you want to stop the Ray processes, run ``ray stop`` on each node.
