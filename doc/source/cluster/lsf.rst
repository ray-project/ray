.. include:: we_are_hiring.rst

.. _ray-LSF-deploy:

Deploying on LSF
================

This document describes a couple high-level steps to run ray cluster on LSF.

1) Obtain desired nodes from LSF scheduler using bsub directives.
2) Obtain free ports on the desired nodes to start ray services like dashboard, GCS etc.
3) Start ray head node on one of the available nodes.
4) Connect all the worker nodes to the head node.
5) Perform port forwarding to access ray dashboard.

Steps 1-4 have been automated and can be easily run as a script, please refer to below github repo to access script and run sample workloads:

- `ray_LSF`_ Ray with LSF. Users can start up a Ray cluster on LSF, and run DL workloads through that either in a batch or interactive mode.

.. _`ray_LSF`: https://github.com/IBMSpectrumComputing/ray-integration
