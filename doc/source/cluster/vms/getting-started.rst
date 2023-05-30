.. include:: /_includes/clusters/announcement.rst

.. _vm-cluster-quick-start:

Getting Started
===============

This quick start demonstrates the capabilities of the Ray cluster. Using the Ray cluster, we'll take a sample application designed to run on a laptop and scale it up in the cloud. Ray will launch clusters and scale Python with just a few commands.

For launching a Ray cluster manually, you can refer to the :ref:`on-premise cluster setup <on-prem>` guide.

About the demo
--------------

This demo will walk through an end-to-end flow:

1. Create a (basic) Python application.
2. Launch a cluster on a cloud provider.
3. Run the application in the cloud.

Requirements
~~~~~~~~~~~~

To run this demo, you will need:

* Python installed on your development machine (typically your laptop), and
* an account at your preferred cloud provider (AWS, Azure or GCP).

Setup
~~~~~

Before we start, you will need to install some Python dependencies as follows:

.. tab-set::

    .. tab-item:: AWS

        .. code-block:: shell

            $ pip install -U "ray[default]" boto3

    .. tab-item:: Azure

        .. code-block:: shell

            $ pip install -U "ray[default]" azure-cli azure-core

    .. tab-item:: GCP

        .. code-block:: shell

            $ pip install -U "ray[default]" google-api-python-client

Next, if you're not set up to use your cloud provider from the command line, you'll have to configure your credentials:

.. tab-set::

    .. tab-item:: AWS

        Configure your credentials in ``~/.aws/credentials`` as described in `the AWS docs <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`_.

    .. tab-item:: Azure

        Log in using ``az login``, then configure your credentials with ``az account set -s <subscription_id>``.

    .. tab-item:: GCP

        Set the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable as described in `the GCP docs <https://cloud.google.com/docs/authentication/getting-started>`_.

Create a (basic) Python application
-----------------------------------

We will write a simple Python application that tracks the IP addresses of the machines that its tasks are executed on:

.. code-block:: python

    from collections import Counter
    import socket
    import time

    def f():
        time.sleep(0.001)
        # Return IP address.
        return socket.gethostbyname(socket.gethostname())

    ip_addresses = [f() for _ in range(10000)]
    print(Counter(ip_addresses))

Save this application as ``script.py`` and execute it by running the command ``python script.py``. The application should take 10 seconds to run and output something similar to ``Counter({'127.0.0.1': 10000})``.

With some small changes, we can make this application run on Ray (for more information on how to do this, refer to :ref:`the Ray Core Walkthrough <core-walkthrough>`):

.. code-block:: python

    from collections import Counter
    import socket
    import time

    import ray

    ray.init()

    @ray.remote
    def f():
        time.sleep(0.001)
        # Return IP address.
        return socket.gethostbyname(socket.gethostname())

    object_ids = [f.remote() for _ in range(10000)]
    ip_addresses = ray.get(object_ids)
    print(Counter(ip_addresses))

Finally, let's add some code to make the output more interesting:

.. code-block:: python

    from collections import Counter
    import socket
    import time

    import ray

    ray.init()

    print('''This cluster consists of
        {} nodes in total
        {} CPU resources in total
    '''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))

    @ray.remote
    def f():
        time.sleep(0.001)
        # Return IP address.
        return socket.gethostbyname(socket.gethostname())

    object_ids = [f.remote() for _ in range(10000)]
    ip_addresses = ray.get(object_ids)

    print('Tasks executed')
    for ip_address, num_tasks in Counter(ip_addresses).items():
        print('    {} tasks on {}'.format(num_tasks, ip_address))

Running ``python script.py`` should now output something like:

.. parsed-literal::

    This cluster consists of
        1 nodes in total
        4.0 CPU resources in total

    Tasks executed
        10000 tasks on 127.0.0.1

Launch a cluster on a cloud provider
------------------------------------

To start a Ray Cluster, first we need to define the cluster configuration. The cluster configuration is defined within a YAML file that will be used by the Cluster Launcher to launch the head node, and by the Autoscaler to launch worker nodes.

A minimal sample cluster configuration file looks as follows:

.. tab-set::

    .. tab-item:: AWS

        .. literalinclude:: ../../../../python/ray/autoscaler/aws/example-minimal.yaml
            :language: yaml


    .. tab-item:: Azure

        .. code-block:: yaml

            # An unique identifier for the head node and workers of this cluster.
            cluster_name: minimal

            # Cloud-provider specific configuration.
            provider:
                type: azure
                location: westus2
                resource_group: ray-cluster

            # How Ray will authenticate with newly launched nodes.
            auth:
                ssh_user: ubuntu
                # you must specify paths to matching private and public key pair files
                # use `ssh-keygen -t rsa -b 4096` to generate a new ssh key pair
                ssh_private_key: ~/.ssh/id_rsa
                # changes to this should match what is specified in file_mounts
                ssh_public_key: ~/.ssh/id_rsa.pub

    .. tab-item:: GCP

        .. code-block:: yaml

            # A unique identifier for the head node and workers of this cluster.
            cluster_name: minimal

            # Cloud-provider specific configuration.
            provider:
                type: gcp
                region: us-west1

Save this configuration file as ``config.yaml``. You can specify a lot more details in the configuration file: instance types to use, minimum and maximum number of workers to start, autoscaling strategy, files to sync, and more. For a full reference on the available configuration properties, please refer to the :ref:`cluster YAML configuration options reference <cluster-config>`.

After defining our configuration, we will use the Ray cluster launcher to start a cluster on the cloud, creating a designated "head node" and worker nodes. To start the Ray cluster, we will use the :ref:`Ray CLI <ray-cluster-cli>`. Run the following command:

.. code-block:: shell

    $ ray up -y config.yaml

Running applications on a Ray Cluster
-------------------------------------

We are now ready to execute an application on our Ray Cluster.
``ray.init()`` will now automatically connect to the newly created cluster.

As a quick example, we execute a Python command on the Ray Cluster that connects to Ray and exits:

.. code-block:: shell

    $ ray exec config.yaml 'python -c "import ray; ray.init()"'
    2022-08-10 11:23:17,093 INFO worker.py:1312 -- Connecting to existing Ray cluster at address: <remote IP address>:6379...
    2022-08-10 11:23:17,097 INFO worker.py:1490 -- Connected to Ray cluster.

You can also optionally get a remote shell using ``ray attach`` and run commands directly on the cluster. This command will create an SSH connection to the head node of the Ray Cluster.

.. code-block:: shell

    # From a remote client:
    $ ray attach config.yaml

    # Now on the head node...
    $ python -c "import ray; ray.init()"

For a full reference on the Ray Cluster CLI tools, please refer to :ref:`the cluster commands reference <cluster-commands>`.

While these tools are useful for ad-hoc execution on the Ray Cluster, the recommended way to execute an application on a Ray Cluster is to use :ref:`Ray Jobs <jobs-quickstart>`. Check out the :ref:`quickstart guide <jobs-quickstart>` to get started!

Deleting a Ray Cluster
----------------------

To shut down your cluster, run the following command:

.. code-block:: shell

    $ ray down -y config.yaml
