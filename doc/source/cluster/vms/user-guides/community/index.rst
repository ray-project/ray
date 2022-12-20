.. _ref-cluster-setup:

Community Supported Cluster Managers
====================================

.. note::

    If you're using AWS, Azure or GCP you can use the :ref:`Ray cluster launcher <cluster-index>` to simplify the cluster setup process.

The following is a list of community supported cluster managers.

.. toctree::
   :maxdepth: 2

   yarn.rst
   slurm.rst
   lsf.rst

.. _ref-additional-cloud-providers:

Using a custom cloud or cluster manager
=======================================

The Ray cluster launcher currently supports AWS, Azure, GCP, Aliyun and Kuberay out of the box. To use the Ray cluster launcher and Autoscaler on other cloud providers or cluster managers, you can implement the `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`_ interface (100 LOC).
Once the node provider is implemented, you can register it in the `provider section <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/local/example-full.yaml#L18>`_ of the cluster launcher config.

.. code-block:: yaml

    provider:
      type: "external"
      module: "my.module.MyCustomNodeProvider"

You can refer to `AWSNodeProvider <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/aws/node_provider.py#L95>`_, `KuberayNodeProvider <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/kuberay/node_provider.py#L148>`_ and
 `LocalNodeProvider <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/local/node_provider.py#L166>`_ for more examples.
