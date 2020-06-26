.. _ref-automatic-cluster:

Ray Cluster Launcher
====================

Ray comes with a built-in cluster launcher that makes deploying a Ray cluster simple.

.. code-block:: shell

    # First, run `pip install boto3` and `aws configure`
    #
    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

This will provision resources from a node provider (like AWS EC2 or Kubernetes) to instantiate the specified cluster, and start a Ray cluster on the provisioned resources. Once the Ray cluster is running, you can manually SSH into it or use provided commands like ``ray attach``, ``ray rsync-up``, and ``ray exec`` to access it and run Ray programs. Check out :ref:`the Usage Guide <launcher-usage>` for instructions on how to use the cluster launcher.

You can configure the Ray Cluster Launcher to use with :ref:`a cloud provider <cluster-cloud>`, an existing :ref:`Kubernetes cluster <ray-launch-k8s>`, or a private cluster of machines.

Check out the below for more information about how to configure and use the cluster launcher.

.. toctree::
   :maxdepth: 2

   launcher-usage.rst
   cloud.rst
   config.rst

Questions or Issues?
--------------------

You can post questions or issues or feedback through the following channels:

1. `ray-dev@googlegroups.com`_: For discussions about development or any general
   questions and feedback.
2. `StackOverflow`_: For questions about how to use Ray.
3. `GitHub Issues`_: For bug reports and feature requests.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
