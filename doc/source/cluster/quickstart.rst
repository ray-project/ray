.. include:: /_includes/clusters/announcement.rst

.. include:: we_are_hiring.rst

.. _ref-cluster-quick-start:

Ray Clusters Quick Start
========================

This quick start demonstrates the capabilities of the Ray cluster. Using the Ray cluster, we'll take a sample application designed to run on a laptop and scale it up in the cloud. Ray will launch clusters and scale Python with just a few commands.

For launching a Ray cluster manually, you can refer to the :ref:`on-premise cluster setup <cluster-private-setup>` guide.

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

.. tabbed:: AWS

    .. code-block:: shell

        $ pip install -U "ray[default]" boto3

.. tabbed:: Azure

    .. code-block:: shell

        $ pip install -U "ray[default]" azure-cli azure-core

.. tabbed:: GCP

    .. code-block:: shell

        $ pip install -U "ray[default]" google-api-python-client

Next, if you're not set up to use your cloud provider from the command line, you'll have to configure your credentials:

.. tabbed:: AWS

    Configure your credentials in ``~/.aws/credentials`` as described in `the AWS docs <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`_.

.. tabbed:: Azure

    Log in using ``az login``, then configure your credentials with ``az account set -s <subscription_id>``.

.. tabbed:: GCP

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

With some small changes, we can make this application run on Ray (for more information on how to do this, refer to :ref:`the Ray Core Walkthrough<core-walkthrough>`):

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

.. tabbed:: AWS

    .. code-block:: yaml

        # An unique identifier for the head node and workers of this cluster.
        cluster_name: minimal

        # Cloud-provider specific configuration.
        provider:
            type: aws
            region: us-west-2

.. tabbed:: Azure

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

.. tabbed:: GCP

    .. code-block:: yaml

        # A unique identifier for the head node and workers of this cluster.
        cluster_name: minimal

        # Cloud-provider specific configuration.
        provider:
            type: gcp
            region: us-west1

Save this configuration file as ``config.yaml``. You can specify a lot more details in the configuration file: instance types to use, minimum and maximum number of workers to start, autoscaling strategy, files to sync, and more. For a full reference on the available configuration properties, please refer to the :ref:`cluster YAML configuration options reference <cluster-config>`.

After defining our configuration, we will use the Ray Cluster Launcher to start a cluster on the cloud, creating a designated "head node" and worker nodes. To start the Ray cluster, we will use the :ref:`Ray CLI <ray-cli>`. Run the following command:

.. code-block:: shell

    $ ray up -y config.yaml

Run the application in the cloud
--------------------------------

We are now ready to execute the application in across multiple machines on our Ray cloud cluster.
First, we need to edit the initialization command ``ray.init()`` in ``script.py``.
Change it to

.. code-block:: python

    ray.init(address='auto')

This tells your script to connect to the Ray runtime on the remote cluster instead of initializing a new Ray runtime.

Next, run the following command:

.. code-block:: shell

    $ ray submit config.yaml script.py

The output should now look similar to the following:

.. parsed-literal::

    This cluster consists of
        3 nodes in total
        6.0 CPU resources in total

    Tasks executed
        3425 tasks on xxx.xxx.xxx.xxx
        3834 tasks on xxx.xxx.xxx.xxx
        2741 tasks on xxx.xxx.xxx.xxx

In this sample output, 3 nodes were started. If the output only shows 1 node, you may want to increase the ``secs`` in ``time.sleep(secs)`` to give Ray more time to start additional nodes.

The Ray CLI offers additional functionality. For example, you can monitor the Ray cluster status with ``ray monitor config.yaml``, and you can connect to the cluster (ssh into the head node) with ``ray attach config.yaml``. For a full reference on the Ray CLI, please refer to :ref:`the cluster commands reference <cluster-commands>`.

To finish, don't forget to shut down the cluster. Run the following command:

.. code-block:: shell

    $ ray down -y config.yaml
