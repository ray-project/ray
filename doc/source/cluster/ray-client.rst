.. _ray-client:

Ray Client
==========

**What is the Ray Client?**

The Ray Client is an API that connects a python script to a Ray cluster. Effectively, it allows you to leverage a remote Ray cluster just like you would with Ray running on your local machine.


By changing ``ray.init()`` to ``ray.client(...).connect()``, you can connect to a remote cluster and scale out your Ray code, while maintaining the ability to develop interactively in a python shell.


.. code-block:: python

   # You can run this code outside of the Ray cluster!
   import ray

   # Starting the Ray client. This connects to a remote Ray cluster.
   ray.client("<head_node_host>:10001").connect()

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)
   #....


How do you use the Ray client?
------------------------------

Step 1: set up your Ray cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you'll want to create a remote Ray cluster. Follow the directions in :ref:`ref-cluster-quick-start` to do this.

If using the `Ray cluster launcher <cluster-cloud>`_, the remote cluster will be listening on port ``10001`` of the head node. If necessary, you can modify this port by setting ``--ray-client-server-port`` to the ``ray start`` `command <http://127.0.0.1:5500/doc/_build/html/package-ref.html#ray-start>`_.

If not using the `Ray cluster launcher <cluster-cloud>`_, you can start the "Ray Client Server" manually on the head node of your remote cluster by running the following:

.. code-block:: bash

    python -m ray.util.client.server [--host host_ip] [--port port] [--redis-address address] [--redis-password password]

Step 2: Check ports
~~~~~~~~~~~~~~~~~~~

Ensure that the Ray Client port on the head node is reachable from your local machine.
This means opening that port up (on  `EC2 <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html>`_)
or proxying from your local machine to the cluster (on `K8s <https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod>`_).

Step 3: Run Ray code
~~~~~~~~~~~~~~~~~~~~

Now, connect to the Ray Cluster with the following and then use Ray like you normally would:

..
.. code-block:: python

   import ray

   # replace with the appropriate host and port
   ray.client("<head_node_host>:10001").connect()

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)

   #....



Things to know
--------------

Client disconnections
~~~~~~~~~~~~~~~~~~~~~

When the client disconnects, any object or actor references held by the server on behalf of the client are dropped, as if directly disconnecting from the cluster.


Versioning requirements
~~~~~~~~~~~~~~~~~~~~~~~

Generally, the client Ray version must match the server Ray version. An error will be raised if an incompatible version is used.

Similarly, the minor Python (e.g., 3.6 vs 3.7) must match between the client and server. An error will be raised if this is not the case.
