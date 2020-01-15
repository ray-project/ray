Performance Tests
=================

This directory contains scripts for running performance benchmarks. These
benchmarks are intended to be used by Ray developers to check if a given pull
request introduces a performance regression.

To check if a pull request introduces a performance regression, it is necessary
to run these benchmarks on the codebase before and after the change.

Running the Workloads
---------------------

To run the workload on a single machine, do the following.

.. code-block:: bash

    python test_performance.py --num-nodes=3

This will start simulate a 3 node cluster on your local machine, attach to it,
and run the benchmarks. To run the benchmarks on an existing cluster, do the
following.

.. code-block:: bash

    python test_performance.py --num-nodes=3 --address=<redis-address>

The ``--num-nodes`` flag must match the number of nodes in the cluster. The
nodes in the cluster must be configured with the appropriate resource labels. In
particular, the ith node in the cluster must have a resource named ``"i"``
with quantity ``500``.
