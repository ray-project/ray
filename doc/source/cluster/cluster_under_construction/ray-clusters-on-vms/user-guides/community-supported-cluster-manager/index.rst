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

To use the Ray Cluster Launcher and Autoscaler on other cloud providers or cluster managers, you can implement the ``NodeProvider`` interface (100 LOC).
and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`_. Contributions are welcome!