.. _cluster-config:

Configuring the Cluster
=======================

The Ray Cluster Launcher requires a *cluster configuration file*, which specifies some important details about the cluster. At a minimum, we need to specify:

 * the name of our cluster,
 * the number of workers in the cluster
 * the cloud provider
 * any setup commands that should run on the node upon launch.

Here is an example cluster configuration file:

.. code-block:: yaml

    # A unique identifier for this cluster.
    cluster_name: basic-ray

    # The maximum number of workers nodes to launch in addition to the head
    # node.
    max_workers: 0 # this means zero workers

    # Cloud-provider specific configuration.
    provider:
       type: aws
       region: us-west-2
       availability_zone: us-west-2a

    # How Ray will authenticate with newly launched nodes.
    auth:
       ssh_user: ubuntu

    setup_commands:
      - pip install ray[all]
      # The following line demonstrate that you can specify arbitrary
      # startup scripts on the cluster.
      - touch /tmp/some_file.txt

Most of the example YAML file is optional. Here is a `reference minimal YAML file <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-minimal.yaml>`__, and you can find the defaults for `optional fields in this YAML file <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__.

You are encouraged to copy the example YAML file and modify it to your needs. This may include adding additional setup commands to install libraries or sync local data files.

Setup Commands
--------------

.. note:: After you have customized the nodes, it is also a good idea to create a new machine image (or docker container) and use that in the config file. This reduces worker setup time, improving the efficiency of auto-scaling.

The setup commands you use should ideally be *idempotent*, that is, can be run more than once. This allows Ray to update nodes after they have been created. You can usually make commands idempotent with small modifications, e.g. ``git clone foo`` can be rewritten as ``test -e foo || git clone foo`` which checks if the repo is already cloned first.


Common cluster configurations
-----------------------------

The ``example-full.yaml`` configuration is enough to get started with Ray, but for more compute intensive workloads you will want to change the instance types to e.g. use GPU or larger compute instance by editing the yaml file. Here are a few common configurations:

**GPU single node**: use Ray on a single large GPU instance.

.. code-block:: yaml

    max_workers: 0
    head_node:
        InstanceType: p2.8xlarge

**Docker**: Specify docker image. This executes all commands on all nodes in the docker container,
and opens all the necessary ports to support the Ray cluster.

.. code-block:: yaml

    docker:
        image: tensorflow/tensorflow:1.5.0-py3
        container_name: ray_docker

If Docker is not installed, add the following commands to ``initialization_commands`` to install it.

.. code-block:: yaml

    initialization_commands:
    - curl -fsSL https://get.docker.com -o get-docker.sh
    - sudo sh get-docker.sh
    - sudo usermod -aG docker $USER
    - sudo systemctl restart docker -f


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

    min_workers: 0  # NOTE: older Ray versions may need 1+ GPU workers (#2106)
    max_workers: 10
    head_node:
        InstanceType: m4.large
    worker_nodes:
        InstanceMarketOptions:
            MarketType: spot
        InstanceType: p2.xlarge
