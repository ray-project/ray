.. _ray-client:

**********
Ray Client
**********

==================
What is Ray Client
==================

Ray Client allows you to interact with a remote Ray cluster just like you would with Ray running on your local machine. The entire `Ray API  <package-ref.html>`_ works over Ray Client!
You can scale your local Ray scripts by changing ``ray.init()`` to ``ray.client(...).connect()``, while maintaining the ability to develop interactively in a python shell.


==================
Getting Started
==================

1. Follow the directions in `"Start a Ray Cluster"  <cluster/quickstart.html>`_ to start a Ray Cluster. 
The server side of Ray Client is automatically started on port ``10001`` of the head node. 
This can be modified by adding ``--ray-client-server-port`` to the ``ray start`` `command <http://127.0.0.1:5500/doc/_build/html/package-ref.html#ray-start>`_.

2. Ensure that the Ray Client port on the head node is reachable from your local machine.
This means opening that port up (on  `EC2 <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html>`_) 
or proxying from your local machine to the cluster (on `K8s <https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod>`_). 

3. Connect to the Ray Cluster with the following and then use Ray like you normally would:

..

.. code-block:: python

   import ray
   ray.client("<head_node_host>:10001").connect()  # replace with the appropriate host and port

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)
   #....



=======================
Things to know
=======================


* When the client disconnects, any object or actor references held by the server on behalf of the client are dropped, as if directly disconnecting from the cluster.

=======================
Versioning requirements
=======================

Generally, the client Ray version must match the server Ray version. An error will be raised if an incompatible version is used.

Similarly, the minor Python (e.g., 3.6 vs 3.7) must match between the client and server. An error will be raised if this is not the case.
