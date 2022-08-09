.. include:: /_includes/clusters/we_are_hiring.rst

.. _ref-cluster-setup-under-construction:

Community Supported Cluster Managers
====================================

The following is a list of community supported cluster managers.

.. note::

    If you're using AWS, Azure or GCP you can use the :ref:`Ray Cluster Launcher <cluster-cloud>` to simplify the cluster setup process.


.. toctree::
   :maxdepth: 2

   yarn.rst
   slurm.rst
   lsf.rst


Implementing a custom node provider
===================================

To use the Ray Cluster Launcher and Autoscaler on other cloud providers or cluster managers, you can implement the `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`_ interface (100 LOC).
Once the node provider is implemented, you can register it in the `provider section <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/local/example-full.yaml#L18>`_ of the cluster launcher config.

.. code-block:: yaml
    provider:
      type: "external"
      module: "my.module.MyCustomNodeProvider"

You can refer to `AwsNodeProvider <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/aws/node_provider.py#L95>`_, `KuberayNodeProvider <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/kuberay/node_provider.py#L148>`_ and
 `LocalNodeProvider <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/local/node_provider.py#L166>`_ for more examples.
