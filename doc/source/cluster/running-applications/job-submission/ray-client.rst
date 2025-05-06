.. _ray-client-ref:

Ray Client
==========

.. warning::
   Ray Client requires pip package `ray[client]`. If you installed the minimal Ray (e.g. `pip install ray`), please reinstall by executing `pip install ray[client]`.

**What is the Ray Client?**

The Ray Client is an API that connects a Python script to a **remote** Ray cluster. Effectively, it allows you to leverage a remote Ray cluster just like you would with Ray running on your local machine.

By changing ``ray.init()`` to ``ray.init("ray://<head_node_host>:<port>")``, you can connect from your laptop (or anywhere) directly to a remote cluster and scale-out your Ray code, while maintaining the ability to develop interactively in a Python shell. **This will only work with Ray 1.5+.**


.. code-block:: python

   # You can run this code outside of the Ray cluster!
   import ray

   # Starting the Ray client. This connects to a remote Ray cluster.
   ray.init("ray://<head_node_host>:10001")

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)
   #....


When to use Ray Client
----------------------

.. note::
   Ray Client has architectural limitations and may not work as expected when using Ray for ML workloads (like Ray Tune or Ray Train). Use :ref:`Ray Jobs API<jobs-overview>` for interactive development on ML projects.

Ray Client can be used when you want to connect an interactive Python shell to a **remote** cluster.

* Use ``ray.init("ray://<head_node_host>:10001")`` (Ray Client) if you've set up a remote cluster at ``<head_node_host>`` and you want to do interactive work. This will connect your shell to the cluster. See the section on :ref:`using Ray Client<how-do-you-use-the-ray-client>` for more details on setting up your cluster.
* Use ``ray.init()`` (non-client connection, no address specified) if you're developing locally and want to connect to an existing cluster (i.e. ``ray start --head`` has already been run), or automatically create a local cluster and attach directly to it. This can also be used for :ref:`Ray Job <jobs-overview>` submission.

Ray Client is useful for developing interactively in a local Python shell. However, it requires a stable connection to the remote cluster and will terminate the workload if the connection is lost for :ref:`more than 30 seconds <client-disconnections>`. If you have a long running workload that you want to run on your cluster, we recommend using :ref:`Ray Jobs <jobs-overview>` instead.

Client arguments
----------------

Ray Client is used when the address passed into ``ray.init`` is prefixed with ``ray://``. Besides the address, Client mode currently accepts two other arguments:

- ``namespace`` (optional): Sets the namespace for the session.
- ``runtime_env`` (optional): Sets the :ref:`runtime environment <runtime-environments>` for the session, allowing you to dynamically specify environment variables, packages, local files, and more.

.. code-block:: python

   # Connects to an existing cluster at 1.2.3.4 listening on port 10001, using
   # the namespace "my_namespace". The Ray workers will run inside a cluster-side
   # copy of the local directory "files/my_project", in a Python environment with
   # `toolz` and `requests` installed.
   ray.init(
       "ray://1.2.3.4:10001",
       namespace="my_namespace",
       runtime_env={"working_dir": "files/my_project", "pip": ["toolz", "requests"]},
   )
   #....

.. _how-do-you-use-the-ray-client:

How do you use the Ray Client?
------------------------------

Step 1: Set up your Ray cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you have a running Ray cluster (version >= 1.5), Ray Client server is likely already running on port ``10001`` of the head node by default. Otherwise, you'll want to create a Ray cluster. To start a Ray cluster locally, you can run

.. code-block:: bash

   ray start --head

To start a Ray cluster remotely, you can follow the directions in :ref:`vm-cluster-quick-start`.

If necessary, you can modify the Ray Client server port to be other than ``10001``, by specifying ``--ray-client-server-port=...`` to the ``ray start`` :ref:`command <ray-start-doc>`.

Step 2: Configure Access
~~~~~~~~~~~~~~~~~~~~~~~~

Ensure that your local machine can access the Ray Client port on the head node.

The easiest way to accomplish this is to use SSH port forwarding or `K8s port-forwarding <https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod>`_.
This allows you to connect to the Ray Client server on the head node via ``localhost``.

First, open up an SSH connection with your Ray cluster and forward the
listening port (``10001``). For Clusters launched with the Ray Cluster launcher this looks like:

.. code-block:: bash

  $ ray up cluster.yaml
  $ ray attach cluster.yaml -p 10001

Then connect to the Ray cluster **from another terminal** using  ``localhost`` as the
``head_node_host``.

.. code-block:: python

   import ray

   # This will connect to the cluster via the open SSH session.
   ray.init("ray://localhost:10001")

   # Normal Ray code follows
   @ray.remote
   def do_work(x):
       return x ** x

   do_work.remote(2)

   #....

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

Alternative Connection Approach:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of port-forwarding, you can directly connect to the Ray Client server on the head node if your computer
has network access to the head node. This is an option if your computer is on the same network as the Cluster or
if your computer can connect to the Cluster with a VPN.

If your computer does not have direct access, you can modify the network configuration to grant access. On `EC2 <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html>`_,
this can be done by modifying the security group to allow inbound access from your local IP address to the Ray Client server port (``10001`` by default).

.. tab-set::

    .. tab-item:: AWS

        With the Ray cluster launcher, you can configure the security group
        to allow inbound access by defining :ref:`cluster-configuration-security-group`
        in your `cluster.yaml`.

        .. code-block:: yaml

            # An unique identifier for the head node and workers of this cluster.
            cluster_name: minimal_security_group

            # Cloud-provider specific configuration.
            provider:
                type: aws
                region: us-west-2
                security_group:
                    GroupName: ray_client_security_group
                    IpPermissions:
                          - FromPort: 10001
                            ToPort: 10001
                            IpProtocol: TCP
                            IpRanges:
                                # Allow traffic only from your local IP address.
                                - CidrIp: <YOUR_IP_ADDRESS>/32


.. warning::
   Anyone with Ray Client access can execute arbitrary code on the Ray Cluster.

   **Do not expose this to `0.0.0.0/0`.**

Connect to multiple Ray clusters (Experimental)
-----------------------------------------------

Ray Client allows connecting to multiple Ray clusters in one Python process. To do this, just pass ``allow_multiple=True`` to ``ray.init``:

.. code-block:: python

    import ray
    # Create a default client.
    ray.init("ray://<head_node_host_cluster>:10001")

    # Connect to other clusters.
    cli1 = ray.init("ray://<head_node_host_cluster_1>:10001", allow_multiple=True)
    cli2 = ray.init("ray://<head_node_host_cluster_2>:10001", allow_multiple=True)

    # Data is put into the default cluster.
    obj = ray.put("obj")

    with cli1:
        obj1 = ray.put("obj1")

    with cli2:
        obj2 = ray.put("obj2")

    with cli1:
        assert ray.get(obj1) == "obj1"
        try:
            ray.get(obj2)  # Cross-cluster ops not allowed.
        except:
            print("Failed to get object which doesn't belong to this cluster")

    with cli2:
        assert ray.get(obj2) == "obj2"
        try:
            ray.get(obj1)  # Cross-cluster ops not allowed.
        except:
            print("Failed to get object which doesn't belong to this cluster")
    assert "obj" == ray.get(obj)
    cli1.disconnect()
    cli2.disconnect()


When using Ray multi-client, there are some different behaviors to pay attention to:

* The client won't be disconnected automatically. Call ``disconnect`` explicitly to close the connection.
* Object references can only be used by the client from which it was obtained.
* ``ray.init`` without ``allow_multiple`` will create a default global Ray client.

Things to know
--------------

.. _client-disconnections:

Client disconnections
~~~~~~~~~~~~~~~~~~~~~

When the client disconnects, any object or actor references held by the server on behalf of the client are dropped, as if directly disconnecting from the cluster.

If the client disconnects unexpectedly, i.e. due to a network failure, the client will attempt to reconnect to the server for 30 seconds before all of the references are dropped. You can increase this time by setting the environment variable ``RAY_CLIENT_RECONNECT_GRACE_PERIOD=N``, where ``N`` is the number of seconds that the client should spend trying to reconnect before giving up.


Versioning requirements
~~~~~~~~~~~~~~~~~~~~~~~

Generally, the client Ray version must match the server Ray version. An error will be raised if an incompatible version is used.

Similarly, the minor Python (e.g., 3.6 vs 3.7) must match between the client and server. An error will be raised if this is not the case.

Starting a connection on older Ray versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you encounter ``socket.gaierror: [Errno -2] Name or service not known`` when using ``ray.init("ray://...")`` then you may be on a version of Ray prior to 1.5 that does not support starting client connections through ``ray.init``.

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

Ray client logs
~~~~~~~~~~~~~~~

Ray client logs can be found at ``/tmp/ray/session_latest/logs`` on the head node.

Uploads
~~~~~~~

If a ``working_dir`` is specified in the runtime env, when running ``ray.init()`` the Ray client will upload the ``working_dir`` on the laptop to ``/tmp/ray/session_latest/runtime_resources/_ray_pkg_<hash of directory contents>``.

Ray workers are started in the ``/tmp/ray/session_latest/runtime_resources/_ray_pkg_<hash of directory contents>`` directory on the cluster. This means that relative paths in the remote tasks and actors in the code will work on the laptop and on the cluster without any code changes. For example, if the ``working_dir`` on the laptop contains ``data.txt`` and ``run.py``, inside the remote task definitions in ``run.py`` one can just use the relative path ``"data.txt"``. Then ``python run.py`` will work on my laptop, and also on the cluster. As a side note, since relative paths can be used in the code, the absolute path is only useful for debugging purposes.

Troubleshooting
---------------

Error: Attempted to reconnect a session that has already been cleaned up
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This error happens when Ray Client reconnects to a head node that does not recognize the client. This can happen if the head node restarts unexpectedly and loses state. On Kubernetes, this can happen if the head pod restarts after being evicted or crashing.
