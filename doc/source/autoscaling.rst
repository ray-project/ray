Cluster setup and auto-scaling (Experimental)
=============================================

Quick start
-----------

First, ensure you have configured your AWS credentials in ``~/.aws/credentials``,
as described in `the boto docs <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`__.

Then you're ready to go. The provided `ray/python/ray/autoscaler/aws/example.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example.yaml>`__ cluster config file will create a small cluster with a m4.large
head node (on-demand), and two m4.large `spot workers <https://aws.amazon.com/ec2/spot/>`__.
Try it out with these commands:

.. code-block:: bash

    # Create or update the cluster
    $ ray create_or_update ray/python/ray/autoscaler/aws/example.yaml

    # Resize the cluster without interrupting running jobs
    $ ray create_or_update ray/python/ray/autoscaler/aws/example.yaml \
        --max-workers=N --sync-only

    # Teardown the cluster
    $ ray teardown ray/python/ray/autoscaler/aws/example.yaml

Common configurations
---------------------

Note: auto-scaling support is not fully implemented yet (targeted for 0.4.0).

The example configuration above is enough to get started with Ray, but for more
compute intensive workloads you will want to change the instance types to e.g.
use GPU or larger compute instance by editing the yaml file. Here are a few common
configurations:

**GPU single node**: use Ray on a single large GPU instance.

.. code-block:: yaml

    max_workers: 0
    head_node:
        InstanceType: p2.8xlarge

**Mixed GPU and CPU nodes**: for RL applications that require proportionally more
CPU than GPU resources, you can use additional CPU workers with a GPU head node.

.. code-block:: yaml

    max_workers: 10
    head_node:
        InstanceType: p2.8xlarge
    worker_nodes:
        InstanceType: m4.16xlarge

**Autoscaling CPU cluster**: use a small head node and have Ray auto-scale
workers as needed. This can be a cost-efficient configuration for clusters with
bursty workloads. You can also request spot workers for additional cost savings.

.. code-block:: yaml

    min_workers: 0
    max_workers: 10
    head_node:
        InstanceType: m4.large
    worker_nodes:
        InstanceMarketOptions:
            MarketType: spot
        InstanceType: m4.16xlarge

**Autoscaling GPU cluster**: similar to the autoscaling CPU cluster, but
with GPU worker nodes instead.

.. code-block:: yaml

    min_workers: 0
    max_workers: 10
    head_node:
        InstanceType: m4.large
    worker_nodes:
        InstanceMarketOptions:
            MarketType: spot
        InstanceType: p2.8xlarge

Additional Cloud providers
--------------------------

To use Ray autoscaling on other Cloud providers, you can implement the ``NodeProvider`` interface
(~100 LOC) and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__.
