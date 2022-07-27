.. warning::
    This page is under construction!

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-cloud-under-construction-azure:

Launching Ray Clusters on Azure
===============================

This section provides instructions for configuring the Ray Cluster Launcher to use with various cloud providers or on a private cluster of host machines.

See this blog post for a `step by step guide`_ to using the Ray Cluster Launcher.

To learn about deploying Ray on an existing Kubernetes cluster, refer to the guide :ref:`here<kuberay-index>`.

.. _`step by step guide`: https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1

.. _ref-cloud-setup-under-construction-azure:

Ray with cloud providers
------------------------

.. toctree::
    :hidden:

    /cluster/aws-tips.rst

.. tabbed::  AWS

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
        $ # Try running a Ray program.

        # Tear down the cluster.
        $ ray down ray/python/ray/autoscaler/aws/example-full.yaml


    AWS Node Provider Maintainers (GitHub handles): pdames, Zyiqin-Miranda, DmitriGekhtman, wuisawesome

    See :ref:`aws-cluster` for recipes on customizing AWS clusters.
.. tabbed:: Azure

    First, install the Azure CLI (``pip install azure-cli azure-identity``) then login using (``az login``).

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
        $ python -c 'import ray; ray.init()'
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
        ray.init()

    Note that on each node the `azure-init.sh <https://github.com/ray-project/ray/blob/master/doc/azure/azure-init.sh>`_ script is executed and performs the following actions:

    1. Activates one of the conda environments available on DSVM
    2. Installs Ray and any other user-specified dependencies
    3. Sets up a systemd task (``/lib/systemd/system/ray.service``) to start Ray in head or worker mode


    Azure Node Provider Maintainers (GitHub handles): gramhagen, eisber, ijrsvt
    .. note:: The Azure Node Provider is community-maintained. It is maintained by its authors, not the Ray team.

.. tabbed:: GCP

    First, install the Google API client (``pip install google-api-python-client``), set up your GCP credentials, and create a new GCP project.

    Once the API client is configured to manage resources on your GCP account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/gcp/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gcp/example-full.yaml>`__ cluster config file will create a small cluster with a n1-standard-2 head node (on-demand) configured to autoscale up to two n1-standard-2 `preemptible workers <https://cloud.google.com/preemptible-vms/>`__. Note that you'll need to fill in your project id in those templates.

    Test that it works by running the following commands from your local machine:

    .. code-block:: bash

        # Create or update the cluster. When the command finishes, it will print
        # out the command that can be used to SSH into the cluster head node.
        $ ray up ray/python/ray/autoscaler/gcp/example-full.yaml

        # Get a remote screen on the head node.
        $ ray attach ray/python/ray/autoscaler/gcp/example-full.yaml
        $ # Try running a Ray program with 'ray.init()'.

        # Tear down the cluster.
        $ ray down ray/python/ray/autoscaler/gcp/example-full.yaml

    GCP Node Provider Maintainers (GitHub handles): wuisawesome, DmitriGekhtman, ijrsvt

.. tabbed:: Aliyun

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
        $ # Try running a Ray program with 'ray.init()'.

        # Tear down the cluster.
        $ ray down ray/python/ray/autoscaler/aliyun/example-full.yaml

    Aliyun Node Provider Maintainers (GitHub handles): zhuangzhuang131419, chenk008

    .. note:: The Aliyun Node Provider is community-maintained. It is maintained by its authors, not the Ray team.


.. tabbed:: Custom

    Ray also supports external node providers (check `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__ implementation).
    You can specify the external node provider using the yaml config:

    .. code-block:: yaml

        provider:
            type: external
            module: mypackage.myclass

    The module needs to be in the format ``package.provider_class`` or ``package.sub_package.provider_class``.

Additional Cloud Providers
--------------------------

To use Ray autoscaling on other Cloud providers or cluster management systems, you can implement the ``NodeProvider`` interface (100 LOC) and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__. Contributions are welcome!


Security
--------

On cloud providers, nodes will be launched into their own security group by default, with traffic allowed only between nodes in the same group. A new SSH key will also be created and saved to your local machine for access to the cluster.

.. _using-ray-on-a-cluster-under-construction-azure:

Running a Ray program on the Ray cluster
----------------------------------------

To run a distributed Ray program, you'll need to execute your program on the same machine as one of the nodes.

.. tabbed:: Python

    Within your program/script, ``ray.init()`` will now automatically find and connect to the latest Ray cluster.
    For example:

    .. code-block:: python

        ray.init()
        # Connecting to existing Ray cluster at address: <IP address>...

.. tabbed:: Java

    You need to add the ``ray.address`` parameter to your command line (like ``-Dray.address=...``).

    To connect your program to the Ray cluster, run it like this:

        .. code-block:: bash

            java -classpath <classpath> \
              -Dray.address=<address> \
              <classname> <args>

    .. note:: Specifying ``auto`` as the address hasn't been implemented in Java yet. You need to provide the actual address. You can find the address of the server from the output of the ``ray up`` command.

.. tabbed:: C++

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
