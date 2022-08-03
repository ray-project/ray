.. warning::
    This page is under construction!

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-cloud-under-construction-gcp:

Launching Ray Clusters on GCP
=============================

This section provides instructions for configuring the Ray Cluster Launcher to use with various cloud providers or on a private cluster of host machines.

See this blog post for a `step by step guide`_ to using the Ray Cluster Launcher.

To learn about deploying Ray on an existing Kubernetes cluster, refer to the guide :ref:`here<kuberay-index>`.

.. _`step by step guide`: https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1

.. _ref-cloud-setup-under-construction-gcp:

Ray with cloud providers
------------------------

.. toctree::
    :hidden:

    /cluster/aws-tips.rst

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
