.. _ray-client:

Ray Client
==========

What is Ray Client?
-------------------

The Ray Client is an API that connects a python script to a Ray cluster without the script having to be executed on the cluster. Effectively, it allows you to leverage a remote Ray cluster just like you would with Ray running on your local machine.


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


Why use Ray Client?
-------------------

TODO


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

**Option 1: Expose port on the head node**
Ensure that the Ray Client port on the head node is reachable from your local machine.
This means opening that port up (on  `EC2 <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html>`_)

**Option 2: Set up port forwarding from local machine to head node**
Instead of directly connecting to the head node address, you can alternatively setup up port-forwarding and then connect to your localhost.
You can easily port forward by calling ``ray attach <CLUSTER-YAML> --port-forward <CLIENT-SERVER-PORT>``.
Since by default the Ray cluster launcher listens on port 10001, an example command would be ``ray attach cluster.yaml --port-forward 10001``.
With port-forwarding setup, you can now connect to the server through the localhost: ``ray.client("localhost:10001").connect()``.

If using Kubernetes, you can follow these `instructions <https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod>`_ to setup port
forwarding to your pod.

Step 3: Run Ray code
~~~~~~~~~~~~~~~~~~~~

Now, connect to the Ray Cluster with the following and then use Ray like you normally would:

..
.. code-block:: python

   import ray

   # replace with the appropriate host and port
   ray.client("<head_node_host>:10001").connect()

   # If you have port forwarding setup, use this line instead.
   # ray.client("localhost:10001").connect()

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)

   #....



Things to know
--------------

Versioning requirements
~~~~~~~~~~~~~~~~~~~~~~~

Generally, the client Ray version must match the server Ray version. An error will be raised if an incompatible version is used.

Similarly, the minor Python version (e.g., 3.6 vs 3.7) must match between the client and server. An error will be raised if this is not the case.

Configuring Ray Client
~~~~~~~~~~~~~~~~~~~~~~

TODO

Client disconnections
~~~~~~~~~~~~~~~~~~~~~

When the client disconnects, any object or actor references held by the server on behalf of the client are dropped, as if directly disconnecting from the cluster.

Dependencies
~~~~~~~~~~~~

With Ray Client, your program is executed on the client-side, except for tasks and actors which are run on the server.
Any dependencies used by your program need to be available at the appropriate place.

1. If a dependency is only needed by driver, then it only needs to be installed on the client side.
2. If a dependency is only needed by tasks or actors, then it only needs to be installed on the server side. Tip: if a
dependency is only needed by a task or actor, you can move your import statement to inside the task/actor so the client doesn't
complain about the dependency not being installed.

..
.. code-block:: python

   import ray

   # replace with the appropriate host and port
   ray.client("<head_node_host>:10001").connect()

   # If you have port forwarding setup, use this line instead.
   # ray.client("localhost:10001").connect()

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       # import the module inside the task so the client doesn't complain about the module not being installed.
       import x
       return x.do_something()

   do_work.remote(2)

   #....

3. If the dependency is needed by both the client and the server, then it needs to be installed in both places. It is important
to have the same version of the dependency installed in both places. Otherwise you could run into pickling or other issues.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

TODO

Advanced: How does Ray Client work?
-----------------------------------

TODO