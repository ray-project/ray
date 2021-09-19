.. _ray-client:

Ray Client
==========

**What is the Ray Client?**

The Ray Client is an API that connects a python script to a **remote** Ray cluster. Effectively, it allows you to leverage a remote Ray cluster just like you would with Ray running on your local machine.

By changing ``ray.init()`` to ``ray.init("ray://<head_node_host>:<port>")``, you can connect from your laptop (or anywhere) directly to a remote cluster and scale-out your Ray code, while maintaining the ability to develop interactively in a python shell. **This will only work with Ray 1.5+.** If you're using an older version of ray, see the `1.4.1 docs <https://docs.ray.io/en/releases-1.4.1/cluster/ray-client.html>`_


.. code-block:: python

   # You can run this code outside of the Ray cluster!
   import ray

   # Starting the Ray client. This connects to a remote Ray cluster.
   # If you're using a version of Ray prior to 1.5, use the 1.4.1 example
   # instead: https://docs.ray.io/en/releases-1.4.1/cluster/ray-client.html
   ray.init("ray://<head_node_host>:10001")

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)
   #....

Client arguments
----------------

Ray client is used when the address passed into ``ray.init`` is prefixed with ``ray://``. Client mode currently accepts two arguments:

- ``namespace``: Sets the namespace for the session
- ``runtime_env``: Sets the `runtime environment <../advanced.html?highlight=runtime environment#runtime-environments-experimental>`_ for the session

.. code-block:: python

   # Connects to an existing cluster at 1.2.3.4 listening on port 10001, using
   # the namespace "my_namespace"
   ray.init("ray://1.2.3.4:10001", namespace="my_namespace")
   #....

When to use Ray client
----------------------

Ray client should be used when you want to connect a script or an interactive shell session to a **remote** cluster.

* Use ``ray.init("ray://<head_node_host>:10001")`` (Ray client) if you've set up a remote cluster at ``<head_node_host>``. This will connect your local script or shell to the cluster. See the section on :ref:`using ray client<how-do-you-use-the-ray-client>` for more details on setting up your cluster.
* Use ``ray.init("localhost:<port>")`` (non-client connection, local address) if you're developing locally or on the head node of your cluster and you have already started the cluster (i.e. ``ray start --head`` has already been run)
* Use ``ray.init()`` (non-client connection, no address specified) if you're developing locally and want to automatically create a local cluster and attach directly to it.

.. _how-do-you-use-the-ray-client:

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
This means opening that port up by configuring security groups or other access controls (on  `EC2 <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html>`_)
or proxying from your local machine to the cluster (on `K8s <https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod>`_).

Step 3: Run Ray code
~~~~~~~~~~~~~~~~~~~~

Now, connect to the Ray Cluster with the following and then use Ray like you normally would:

..
.. code-block:: python

   import ray

   # replace with the appropriate host and port
   ray.init("ray://<head_node_host>:10001")

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

Starting a connection on older Ray versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you encounter ``socket.gaierror: [Errno -2] Name or service not known`` when using ``ray.init("ray://...")`` then you may be on a version of Ray prior to 1.5 that does not support starting client connections through ``ray.init``. If this is the case, see the `1.4.1 docs <https://docs.ray.io/en/releases-1.4.1/cluster/ray-client.html>`_ for Ray client.

Connection through the Ingress
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you encounter the following error message when connecting to the ``Ray Cluster`` using an ``Ingress``,  it may be caused by the Ingress's configuration.

..
.. code-block:: python

   grpc._channel._MultiThreadedRendezvous: <_MultiThreadedRendezvous of RPC that terminated with:
       status = StatusCode.INVALID_ARGUMENT
       details = ""
       debug_error_string = "{"created":"@1628668820.164591000","description":"Error received from peer ipv4:10.233.120.107:443","file":"src/core/lib/surface/call.cc","file_line":1062,"grpc_message":"","grpc_status":3}"
   >
   Got Error from logger channel -- shutting down: <_MultiThreadedRendezvous of RPC that terminated with:
       status = StatusCode.INVALID_ARGUMENT
       details = ""
       debug_error_string = "{"created":"@1628668820.164713000","description":"Error received from peer ipv4:10.233.120.107:443","file":"src/core/lib/surface/call.cc","file_line":1062,"grpc_message":"","grpc_status":3}"
   >


If you are using the ``nginx-ingress-controller``, you may be able to resolve the issue by adding the following Ingress configuration.


.. code-block:: yaml
   
   metadata:
     annotations:
        nginx.ingress.kubernetes.io/server-snippet: |
          underscores_in_headers on;
          ignore_invalid_headers on;
   
