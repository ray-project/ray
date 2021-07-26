.. _ray-client:

Ray Client
==========

**What is the Ray Client?**

The Ray Client is an API that connects a python script to a Ray cluster. Effectively, it allows you to leverage a remote Ray cluster just like you would with Ray running on your local machine.


By changing ``ray.init()`` to ``ray.init("ray://<host>:<port>")``, you can connect to a remote cluster and scale out your Ray code, while maintaining the ability to develop interactively in a python shell.


.. code-block:: python

   # You can run this code outside of the Ray cluster!
   import ray

   # Starting the Ray client. This connects to a remote Ray cluster.
   # If you're using a version of Ray prior to 1.5, use the ClientBuilder API
   # instead.
   ray.init("ray://<head_node_host>:10001").connect()

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)
   #....


You can also connect using the ClientBuilder API, but this will eventually be deprecated.

.. code-block:: python

   # You can run this code outside of the Ray cluster!
   import ray

   # Starting the Ray client. This connects to a remote Ray cluster.
   # `ray.client` will be deprecated in future releases, so we recommend
   # using `ray.init("ray://<head_node_host>:10001")` instead.
   ray.client("<head_node_host>:10001").connect()

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)
   #....

Client arguments
----------------

Ray client is used when the address passed into ``ray.init`` is prefixed with the name of a protocol and ``://``. **This will only work with Ray 1.5+.** If you're using an older version of ray, see the ClientBuilder API section for information on passing arguments. Currently, two protocols are built into ray by default:

**Local mode** is used when the address is prefixed with ``local://``, i.e. ``ray.init("local://")``. In local mode, a local cluster is created and connected to by Ray Client. Local mode accepts all of the same keywords as ``ray.init`` and uses them to configure the local cluster.

.. code-block:: python

   # Start a local cluster with 2 cpus and expose the dashboard on port 8888
   ray.init("local://", num_cpus=2, dashboard_port=8888)
   #....

**Client mode** is used when the address is prefixed with ``ray://``. i.e. ``ray.init("ray://<head_node_host>:10001")``. Client mode connects to an existing Ray Client Server at the given address. Client mode currently accepts two arguments:

   - ``namespace``: Sets the namespace for the session
   - ``runtime_env``: Sets the `runtime environment </advanced.html?highlight=runtime environment#runtime-environments-experimental>`_ for the session

.. code-block:: python

   # Connects to an existing cluster at 1.2.3.4 listening on port 10001, using
   # the namespace "my_namespace"
   ray.init("ray://1.2.3.4:10001", namespace="my_namespace")
   #....

**Custom protocols** can be created by third parties to extend or alter connection behavior. More details can be found in the source for `client_builder.py <https://github.com/ray-project/ray/blob/master/python/ray/client_builder.py>`_

ClientBuilder API
-----------------

The ClientBuilder API is an alternative way to create a ray client connection. It is currently supported for backwards compatibility, but will eventually be deprecated. To use the ClientBuilder API, replace any calls to ``ray.init()`` with ``ray.client("<head_node_host>:10001").connect()``. This will connect to an existing Ray Client Server at that address.

The client connection can be configured using the `fluent builder pattern <https://en.wikipedia.org/wiki/Fluent_interface>`_. The following settings can be configured through method chaining:

- ``namespace``: Sets the namespace for the session
- ``env``: Sets the `runtime environment </advanced.html?highlight=runtime environment#runtime-environments-experimental>`_ for the session

.. code-block:: python

   # Start a ray client connection in the namespace "my_namespace"
   ray.client("1.2.3.4:10001").namespace("my_namespace").connect()
   #....

.. code-block:: python

   # Start with both a namespace and runtime environment (order does not matter)
   ray.client("1.2.3.4:10001").namespace("my_namespace").env({...}).connect()
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

If you encounter ``socket.gaierror: [Errno -2] Name or service not known`` when using ``ray.init("ray://...")`` then you may be on a version of Ray prior to 1.5 that does not support starting client connections through ``ray.init``. If this is the case, use the ClientBuilder API instead.
