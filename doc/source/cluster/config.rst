.. _cluster-config:

Configuring your Cluster
========================

.. tip:: Before you continue, be sure to have read :ref:`cluster-cloud`.

To launch a cluster, you must first create a *cluster configuration file*, which specifies some important details about the cluster.

Quickstart
----------

At a minimum, we need to specify:

* the name of your cluster,
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

In another example, the `AWS example configuration file <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__ cluster config file will create a small cluster with an m5.large head node (on-demand) configured to autoscale up to two m5.large `spot workers <https://aws.amazon.com/ec2/spot/>`__.

**You are encouraged to copy the example YAML file and modify it to your needs. This may include adding additional setup commands to install libraries or sync local data files.**

Setup Commands
--------------

.. tip:: After you have customized the nodes, create a new machine image (or docker container) and use that in the config file to reduce setup times.

The setup commands you use should ideally be *idempotent* (i.e., can be run multiple times without changing the result). This allows Ray to safely update nodes after they have been created.

You can usually make commands idempotent with small modifications, e.g. ``git clone foo`` can be rewritten as ``test -e foo || git clone foo`` which checks if the repo is already cloned first.

.. _autoscaler-docker:

Docker Support
--------------

The cluster launcher is fully compatible with Docker images. To use Docker, provide a ``docker_image`` and ``container_name`` in the ``docker`` field of the YAML.

.. code-block:: yaml

    docker:
        container_name: "ray_container"
        image: "rayproject/ray-ml:latest-gpu"

We provide docker images on `DockerHub <https://hub.docker.com/u/rayproject>`__. The ``rayproject/ray-ml:latest`` image is a quick way to get up and running .

When the cluster is launched, all of the Ray tasks will be executed completely inside of the container. For GPU support, Ray will automatically select the Nvidia docker runtime if available, and you just need to specify a docker image with the CUDA support (``rayproject/ray-ml:latest-gpu`` and all of our ``-gpu`` images have this).

If Docker is not installed, add the following commands to ``initialization_commands`` to install it.

.. code-block:: yaml

    initialization_commands:
        - curl -fsSL https://get.docker.com -o get-docker.sh
        - sudo sh get-docker.sh
        - sudo usermod -aG docker $USER
        - sudo systemctl restart docker -f

Common cluster configurations
-----------------------------

The `example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__ configuration is enough to get started with Ray, but for more compute intensive workloads you will want to change the instance types to e.g. use GPU or larger compute instance by editing the yaml file.

Here are a few common configurations (note that we use AWS in the examples, but these examples are generic):

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

    min_workers: 0  # NOTE: older Ray versions may need 1+ GPU workers (#2106)
    max_workers: 10
    head_node:
        InstanceType: m4.large
    worker_nodes:
        InstanceMarketOptions:
            MarketType: spot
        InstanceType: p2.xlarge

