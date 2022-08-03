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

What's Next?
-------------

Now that you have a working understanding of the cluster launcher, check out:

* :ref:`ref-cluster-quick-start`: A end-to-end demo to run an application that autoscales.
* :ref:`cluster-config`: A complete reference of how to configure your Ray cluster.
* :ref:`cluster-commands`: A short user guide to the various cluster launcher commands.



Questions or Issues?
--------------------

.. include:: /_includes/_help.rst
