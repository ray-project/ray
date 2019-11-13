Deploying on YARN
=================

.. warning::

  Running Ray on YARN is still a work in progress. If you have a
  suggestion for how to improve this documentation or want to request
  a missing feature, please get in touch using one of the channels in the 
  `Questions or Issues?`_ section below.

This document assumes that you have access to a YARN cluster and will walk
you through using `Skein`_ to deploy a YARN job that starts a Ray cluster and
runs an example script on it. You will need to install Skein first:
`pip install skein`.

The Skein ``yaml`` file and example Ray program used here are provided in the
`Ray repository`_ to get you started. Refer to the provided ``yaml``
files to be sure that you maintain important configuration options for Ray to
function properly.

.. _`Ray repository`: https://github.com/ray-project/ray/tree/master/doc/yarn

Skein Configuration
-------------------

A Ray job is configured to run as two `Skein services`_:

1. The `ray-head` service that starts the Ray head node and then runs the
   application.
2. The `ray-worker` service that starts worker nodes that join the Ray cluster.
   You can change the number of instances in this configuration or at runtime
   using `skein scale` to scale the cluster up/down.

Packaging Dependencies
----------------------

Submitting a Job
----------------

Monitoring
----------

Cleaning Up
-----------

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

.. _`Skein`: https://jcrist.github.io/skein/
