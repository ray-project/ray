.. _cluster-cloud:

Launching Cloud Clusters
========================

This section provides instructions for configuring the Ray Cluster Launcher to use with AWS/Azure/GCP, an existing Kubernetes cluster, or on a private cluster of host machines.

See this blog post for a `step by step guide`_ to using the Ray Cluster Launcher.

.. _`step by step guide`: https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1

.. _ref-cloud-setup:

AWS/GCP/Azure
-------------

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
        Use the following code in a Jupyter notebook (using the conda environment specified in the template input, py37_tensorflow by default) to connect to the Ray cluster.

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

    .. group-tab:: Custom

        Ray also supports external node providers (check `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__ implementation).
        You can specify the external node provider using the yaml config:

        .. code-block:: yaml

            provider:
                type: external
                module: mypackage.myclass

        The module needs to be in the format ``package.provider_class`` or ``package.sub_package.provider_class``.

.. _ray-launch-k8s:

Kubernetes
----------

The cluster launcher can also be used to start Ray clusters on an existing Kubernetes cluster.

.. tabs::
    .. group-tab:: Kubernetes
        First, install the Kubernetes API client (``pip install kubernetes``), then make sure your Kubernetes credentials are set up properly to access the cluster (if a command like ``kubectl get pods`` succeeds, you should be good to go).

        Once you have ``kubectl`` configured locally to access the remote cluster, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/kubernetes/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-full.yaml>`__ cluster config file will create a small cluster of one pod for the head node configured to autoscale up to two worker node pods, with all pods requiring 1 CPU and 0.5GiB of memory.
        It's also possible to deploy service and ingress resources for each scaled worker pod. An example is provided in `ray/python/ray/autoscaler/kubernetes/example-ingress.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-ingress.yaml>`__.

        Test that it works by running the following commands from your local machine:

        .. code-block:: bash

            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to get a remote shell into the head node.
            $ ray up ray/python/ray/autoscaler/kubernetes/example-full.yaml

            # List the pods running in the cluster. You shoud only see one head node
            # until you start running an application, at which point worker nodes
            # should be started. Don't forget to include the Ray namespace in your
            # 'kubectl' commands ('ray' by default).
            $ kubectl -n ray get pods

            # Get a remote screen on the head node.
            $ ray attach ray/python/ray/autoscaler/kubernetes/example-full.yaml
            $ # Try running a Ray program with 'ray.init(address="auto")'.

            # Tear down the cluster
            $ ray down ray/python/ray/autoscaler/kubernetes/example-full.yaml

        .. tip:: This section describes the easiest way to launch a Ray cluster on Kubernetes. See this :ref:`document for advanced usage <ray-k8s-deploy>` of Kubernetes with Ray.

        .. tip:: If you would like to use Ray Tune in your Kubernetes cluster, have a look at :ref:`this short guide to make it work <tune-kubernetes>`.

    .. group-tab:: Staroid (contributed)

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


Additional Cloud Providers
--------------------------

To use Ray autoscaling on other Cloud providers or cluster management systems, you can implement the ``NodeProvider`` interface (100 LOC) and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__. Contributions are welcome!


Security
--------

On cloud providers, nodes will be launched into their own security group by default, with traffic allowed only between nodes in the same group. A new SSH key will also be created and saved to your local machine for access to the cluster.


What's Next?
-------------

Now that you have a working understanding of the cluster launcher, check out:

* :ref:`cluster-config`: A guide to configuring your Ray cluster.
* :ref:`cluster-commands`: A short user guide to the various cluster launcher commands.
* A `step by step guide`_ to using the cluster launcher
* :ref:`ref-autoscaling`: An overview of how Ray autoscaling works.



Questions or Issues?
--------------------

.. include:: /_help.rst
