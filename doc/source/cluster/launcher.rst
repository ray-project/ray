.. _ref-automatic-cluster:

Launching Cloud Clusters with Ray
=================================

Ray comes with a built-in cluster launcher that makes deploying a Ray cluster simple.

The cluster launcher will provision resources from a node provider (like :ref:`AWS EC2 <ref-cloud-setup>` or :ref:`Kubernetes <ray-launch-k8s>`) to instantiate the specified cluster, and start a Ray cluster on the provisioned resources.

You can configure the Ray Cluster Launcher to use with :ref:`a cloud provider <cluster-cloud>`, an existing :ref:`Kubernetes cluster <ray-launch-k8s>`, or a private cluster of machines.

.. tabs::
    .. group-tab:: AWS

        .. code-block:: shell

            # First, run `pip install boto3` and `aws configure`
            #
            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

        See :ref:`the AWS section <ref-cloud-setup>` for full instructions.

    .. group-tab:: GCP

        .. code-block:: shell

            #  First, ``pip install google-api-python-client``
            # set up your GCP credentials, and
            # create a new GCP project.
            #
            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/gcp/example-full.yaml

        See :ref:`the GCP section <ref-cloud-setup>` for full instructions.

    .. group-tab:: Azure

        .. code-block:: shell

            # First, install the Azure CLI
            # ``pip install azure-cli azure-core``) then
            # login using (``az login``).
            #
            # Create or update the cluster. When the command finishes, it will print
            # out the command that can be used to SSH into the cluster head node.
            $ ray up ray/python/ray/autoscaler/azure/example-full.yaml

        See :ref:`the Azure section <ref-cloud-setup>` for full instructions.


Once the Ray cluster is running, you can manually SSH into it or use provided commands like ``ray attach``, ``ray rsync-up``, and ``ray exec`` to access it and run Ray programs.


.. toctree::

    /cluster/cloud.rst
    /cluster/config.rst
    /cluster/commands.rst

Questions or Issues?
--------------------

.. include:: /_help.rst
