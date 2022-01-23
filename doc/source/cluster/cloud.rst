.. include:: we_are_hiring.rst

.. _cluster-cloud:

Launching Cloud Clusters
========================

This section provides instructions for configuring the Ray Cluster Launcher to use with various cloud providers or on a private cluster of host machines.

See this blog post for a `step by step guide`_ to using the Ray Cluster Launcher.

To learn about deploying Ray on an existing Kubernetes cluster, refer to the guide :ref:`here<ray-k8s-deploy>`.

.. _`step by step guide`: https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1

.. _ref-cloud-setup:

Ray with cloud providers
------------------------

.. toctree::
    :hidden:

    /cluster/aws-tips.rst

.. tabs::
    .. group-tab:: AWS

        First, install boto (``pip install boto3``) and configure your AWS credentials in ``~/.aws/credentials``,
        as described in `the boto docs <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`__.

        Once boto is configured to manage resources on your AWS account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/aws/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__ cluster config file will create a small cluster with an m5.large head node (on-demand) configured to autoscale up to two m5.large `spot workers <https://aws.amazon.com/ec2/spot/>`__.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/aws/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster.
            $ ray down ray/python/ray/autoscaler/aws/example-full.yaml


        See :ref:`aws-cluster` for recipes on customizing AWS clusters.
    .. group-tab:: Azure

        First, install the Azure CLI (``pip install azure-cli``) then login using (``az login``).

        Set the subscription to use from the command line (``az account set -s <subscription_id>``) or by modifying the provider section of the config provided e.g: `ray/python/ray/autoscaler/azure/example-full.yaml`

        Once the Azure CLI is configured to manage resources on your Azure account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/azure/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/azure/example-full.yaml>`__ cluster config file will create a small cluster with a Standard DS2v3 head node (on-demand) configured to autoscale up to two Standard DS2v3 `spot workers <https://docs.microsoft.com/en-us/azure/virtual-machines/windows/spot-vms>`__. Note that you'll need to fill in your resource group and location in those templates.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/azure/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/azure/example-full.yaml
            # test ray setup
            $ python -c 'import ray; ray.init(address="auto")'
            $ exit
            # Tear down the cluster.
            $ ray down ray/python/ray/autoscaler/azure/example-full.yaml

        **Azure Portal**:
        Alternatively, you can deploy a cluster using Azure portal directly. Please note that autoscaling is done using Azure VM Scale Sets and not through
        the Ray autoscaler. This will deploy `Azure Data Science VMs (DSVM) <https://azure.microsoft.com/en-us/services/virtual-machines/data-science-virtual-machines/>`_
        for both the head node and the auto-scalable cluster managed by `Azure Virtual Machine Scale Sets <https://azure.microsoft.com/en-us/services/virtual-machine-scale-sets/>`_.
        The head node conveniently exposes both SSH as well as JupyterLab.

        .. image:: https://aka.ms/deploytoazurebutton
           :target: https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fray-project%2Fray%2Fmaster%2Fdoc%2Fazure%2Fazure-ray-template.json
           :alt: Deploy to Azure

        Once the template is successfully deployed the deployment Outputs page provides the ssh command to connect and the link to the JupyterHub on the head node (username/password as specified on the template input).
        Use the following code in a Jupyter notebook (using the conda environment specified in the template input, py38_tensorflow by default) to connect to the Ray cluster.

        .. code-block:: python

            import ray
            ray.init(address='auto')

        Note that on each node the `azure-init.sh <https://github.com/ray-project/ray/blob/master/doc/azure/azure-init.sh>`_ script is executed and performs the following actions:

        1. Activates one of the conda environments available on DSVM
        2. Installs Ray and any other user-specified dependencies
        3. Sets up a systemd task (``/lib/systemd/system/ray.service``) to start Ray in head or worker mode

    .. group-tab:: GCP

        First, install the Google API client (``pip install google-api-python-client``), set up your GCP credentials, and create a new GCP project.

        Once the API client is configured to manage resources on your GCP account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/gcp/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gcp/example-full.yaml>`__ cluster config file will create a small cluster with a n1-standard-2 head node (on-demand) configured to autoscale up to two n1-standard-2 `preemptible workers <https://cloud.google.com/preemptible-vms/>`__. Note that you'll need to fill in your project id in those templates.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/gcp/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/gcp/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster.
            $ ray down ray/python/ray/autoscaler/gcp/example-full.yaml

    .. group-tab:: Staroid Kubernetes Engine (contributed)

        The Ray Cluster Launcher can be used to start Ray clusters on an existing Staroid Kubernetes Engine (SKE) cluster.

        First, install the staroid client package (``pip install staroid``) then get `access token <https://staroid.com/settings/accesstokens>`_.
        Once you have an access token, you should be ready to launch your cluster.

        The provided `ray/python/ray/autoscaler/staroid/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/staroid/example-full.yaml>`__ cluster config file will create a cluster with

        - a Jupyter notebook running on head node.
          (Staroid management console -> Kubernetes -> ``<your_ske_name>`` -> ``<ray_cluster_name>`` -> Click "notebook")
        - a shared nfs volume across all ray nodes mounted under ``/nfs`` directory.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Configure access token through environment variable.
            $ export STAROID_ACCESS_TOKEN=<your access token>

            # Create or update the cluster. When the command finishes,
            # you can attach a screen to the head node.
            $ ray up ray/python/ray/autoscaler/staroid/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/staroid/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster
            $ ray down ray/python/ray/autoscaler/staroid/example-full.yaml

    .. group-tab:: Aliyun

        First, install the aliyun client package (``pip install aliyun-python-sdk-core aliyun-python-sdk-ecs``). Obtain the AccessKey pair of the Aliyun account as described in `the docs <https://www.alibabacloud.com/help/en/doc-detail/175967.htm>`__ and grant AliyunECSFullAccess/AliyunVPCFullAccess permissions to the RAM user. Finally, set the AccessKey pair in your cluster config file.

        Once the above is done, you should be ready to launch your cluster. The provided `aliyun/example-full.yaml </ray/python/ray/autoscaler/aliyun/example-full.yaml>`__ cluster config file will create a small cluster with an ``ecs.n4.large`` head node (on-demand) configured to autoscale up to two ``ecs.n4.2xlarge`` nodes.

        Make sure your account balance is not less than 100 RMB, otherwise you will receive a `InvalidAccountStatus.NotEnoughBalance` error.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/aliyun/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/aliyun/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster.
            $ ray down ray/python/ray/autoscaler/aliyun/example-full.yaml

        Aliyun Node Provider Maintainer: zhuangzhuang131419, chenk008

    .. group-tab:: Custom

        Ray also supports external node providers (check `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__ implementation).
        You can specify the external node provider using the yaml config:

        .. code-block:: yaml

            provider:
                type: external
                module: mypackage.myclass

        The module needs to be in the format ``package.provider_class`` or ``package.sub_package.provider_class``.


.. _cluster-private-setup:

Local On Premise Cluster (List of nodes)
----------------------------------------
You would use this mode if you want to run distributed Ray applications on some local nodes available on premise.

The most preferable way to run a Ray cluster on a private cluster of hosts is via the Ray Cluster Launcher.

There are two ways of running private clusters:

- Manually managed, i.e., the user explicitly specifies the head and worker ips.

- Automatically managed, i.e., the user only specifies a coordinator address to a coordinating server that automatically coordinates its head and worker ips.

.. tip:: To avoid getting the password prompt when running private clusters make sure to setup your ssh keys on the private cluster as follows:

    .. code-block:: bash

        $ ssh-keygen
        $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

.. tabs::

    .. group-tab:: Manually Managed

        You can get started by filling out the fields in the provided `ray/python/ray/autoscaler/local/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/local/example-full.yaml>`__.
        Be sure to specify the proper ``head_ip``, list of ``worker_ips``, and the ``ssh_user`` field.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to get a remote shell into the head node.
            $ ray up ray/python/ray/autoscaler/local/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/local/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster
            $ ray down ray/python/ray/autoscaler/local/example-full.yaml

    .. group-tab:: Automatically Managed


        Start by launching the coordinator server that will manage all the on prem clusters. This server also makes sure to isolate the resources between different users. The script for running the coordinator server is `ray/python/ray/autoscaler/local/coordinator_server.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/local/coordinator_server.py>`__. To launch the coordinator server run:

        .. code-block:: bash

            $ python coordinator_server.py --ips <list_of_node_ips> --port <PORT>

        where ``list_of_node_ips`` is a comma separated list of all the available nodes on the private cluster. For example, ``160.24.42.48,160.24.42.49,...`` and ``<PORT>`` is the port that the coordinator server will listen on.
        After running the coordinator server it will print the address of the coordinator server. For example:

        .. code-block:: bash

          >> INFO:ray.autoscaler.local.coordinator_server:Running on prem coordinator server
                on address <Host:PORT>

        Next, the user only specifies the ``<Host:PORT>`` printed above in the ``coordinator_address`` entry instead of specific head/worker ips in the provided `ray/python/ray/autoscaler/local/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/local/example-full.yaml>`__.

        Now we can test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to get a remote shell into the head node.
            $ ray up ray/python/ray/autoscaler/local/example-full.yaml

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/local/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster
            $ ray down ray/python/ray/autoscaler/local/example-full.yaml


.. _manual-cluster:

Manual Ray Cluster Setup
------------------------

The most preferable way to run a Ray cluster is via the Ray Cluster Launcher. However, it is also possible to start a Ray cluster by hand.

This section assumes that you have a list of machines and that the nodes in the cluster can communicate with each other. It also assumes that Ray is installed
on each machine. To install Ray, follow the `installation instructions`_.

.. _`installation instructions`: http://docs.ray.io/en/master/installation.html

Starting Ray on each machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the head node (just choose one node to be the head node), run the following.
If the ``--port`` argument is omitted, Ray will choose port 6379, falling back to a
random port.

.. code-block:: bash

  $ ray start --head --port=6379
  ...
  Next steps
    To connect to this Ray runtime from another node, run
      ray start --address='<ip address>:6379' --redis-password='<password>'

  If connection fails, check your firewall settings and network configuration.

The command will print out the address of the Redis server that was started
(the local node IP address plus the port number you specified).

.. note::

    If you already has remote redis instances, you can specify `--address=ip1:port1,ip2:port2...` to use them. The first one is primary and rest are shards. Ray will create a redis instance if the default is unreachable.

**Then on each of the other nodes**, run the following. Make sure to replace
``<address>`` with the value printed by the command on the head node (it
should look something like ``123.45.67.89:6379``).

Note that if your compute nodes are on their own subnetwork with Network
Address Translation, to connect from a regular machine outside that subnetwork,
the command printed by the head node will not work. You need to find the
address that will reach the head node from the second machine. If the head node
has a domain address like compute04.berkeley.edu, you can simply use that in
place of an IP address and rely on the DNS.

.. code-block:: bash

  $ ray start --address=<address> --redis-password='<password>'
  --------------------
  Ray runtime started.
  --------------------

  To terminate the Ray runtime, run
    ray stop

If you wish to specify that a machine has 10 CPUs and 1 GPU, you can do this
with the flags ``--num-cpus=10`` and ``--num-gpus=1``. See the :ref:`Configuration <configuring-ray>` page for more information.

If you see ``Unable to connect to Redis. If the Redis instance is on a
different machine, check that your firewall is configured properly.``,
this means the ``--port`` is inaccessible at the given IP address (because, for
example, the head node is not actually running Ray, or you have the wrong IP
address).

If you see ``Ray runtime started.``, then the node successfully connected to
the IP address at the ``--port``. You should now be able to connect to the
cluster with ``ray.init(address='auto')``.

If ``ray.init(address='auto')`` keeps repeating
``redis_context.cc:303: Failed to connect to Redis, retrying.``, then the node
is failing to connect to some other port(s) besides the main port.

.. code-block:: bash

  If connection fails, check your firewall settings and network configuration.

If the connection fails, to check whether each port can be reached from a node,
you can use a tool such as ``nmap`` or ``nc``.

.. code-block:: bash

  $ nmap -sV --reason -p $PORT $HEAD_ADDRESS
  Nmap scan report for compute04.berkeley.edu (123.456.78.910)
  Host is up, received echo-reply ttl 60 (0.00087s latency).
  rDNS record for 123.456.78.910: compute04.berkeley.edu
  PORT     STATE SERVICE REASON         VERSION
  6379/tcp open  redis   syn-ack ttl 60 Redis key-value store
  Service detection performed. Please report any incorrect results at https://nmap.org/submit/ .
  $ nc -vv -z $HEAD_ADDRESS $PORT
  Connection to compute04.berkeley.edu 6379 port [tcp/*] succeeded!

If the node cannot access that port at that IP address, you might see

.. code-block:: bash

  $ nmap -sV --reason -p $PORT $HEAD_ADDRESS
  Nmap scan report for compute04.berkeley.edu (123.456.78.910)
  Host is up (0.0011s latency).
  rDNS record for 123.456.78.910: compute04.berkeley.edu
  PORT     STATE  SERVICE REASON       VERSION
  6379/tcp closed redis   reset ttl 60
  Service detection performed. Please report any incorrect results at https://nmap.org/submit/ .
  $ nc -vv -z $HEAD_ADDRESS $PORT
  nc: connect to compute04.berkeley.edu port 6379 (tcp) failed: Connection refused


Stopping Ray
~~~~~~~~~~~~

When you want to stop the Ray processes, run ``ray stop`` on each node.


Additional Cloud Providers
--------------------------

To use Ray autoscaling on other Cloud providers or cluster management systems, you can implement the ``NodeProvider`` interface (100 LOC) and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__. Contributions are welcome!


Security
--------

On cloud providers, nodes will be launched into their own security group by default, with traffic allowed only between nodes in the same group. A new SSH key will also be created and saved to your local machine for access to the cluster.

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

            java -classpath <classpath> \
              -Dray.address=<address> \
              <classname> <args>

    .. note:: Specifying ``auto`` as the address hasn't been implemented in Java yet. You need to provide the actual address. You can find the address of the server from the output of the ``ray up`` command.

  .. group-tab:: C++

    You need to add the ``RAY_ADDRESS`` env var to your command line (like ``RAY_ADDRESS=...``).

    To connect your program to the Ray cluster, run it like this:

        .. code-block:: bash

            RAY_ADDRESS=<address> ./<binary> <args>

    .. note:: Specifying ``auto`` as the address hasn't been implemented in C++ yet. You need to provide the actual address. You can find the address of the server from the output of the ``ray up`` command.


.. note:: A common mistake is setting the address to be a cluster node while running the script on your laptop. This will not work because the script needs to be started/executed on one of the Ray nodes.

To verify that the correct number of nodes have joined the cluster, you can run the following.

.. code-block:: python

  import time

  @ray.remote
  def f():
      time.sleep(0.01)
      return ray._private.services.get_node_ip_address()

  # Get a list of the IP addresses of the nodes that have joined the cluster.
  set(ray.get([f.remote() for _ in range(1000)]))


What's Next?
-------------

Now that you have a working understanding of the cluster launcher, check out:

* :ref:`ref-cluster-quick-start`: A end-to-end demo to run an application that autoscales.
* :ref:`cluster-config`: A complete reference of how to configure your Ray cluster.
* :ref:`cluster-commands`: A short user guide to the various cluster launcher commands.



Questions or Issues?
--------------------

.. include:: /_includes/_help.rst
