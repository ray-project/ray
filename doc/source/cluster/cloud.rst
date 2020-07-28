.. _cluster-cloud:

Launching Cloud Clusters
========================

This section provides instructions for configuring the Ray Cluster Launcher to use with AWS/Azure/GCP, an existing Kubernetes cluster, or on a private cluster of host machines.

.. contents::
    :local:
    :backlinks: none

See this blog post for a `step by step guide`_ to using the Ray Cluster Launcher.

.. _`step by step guide`: https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1

AWS (EC2)
---------

First, install boto (``pip install boto3``) and configure your AWS credentials in ``~/.aws/credentials``,
as described in `the boto docs <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`__.

Once boto is configured to manage resources on your AWS account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/aws/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__ cluster config file will create a small cluster with an m5.large head node (on-demand) configured to autoscale up to two m5.large `spot workers <https://aws.amazon.com/ec2/spot/>`__.

Test that it works by running the following commands from your local machine:

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

    # Get a remote screen on the head node.
    $ ray attach ray/python/ray/autoscaler/aws/example-full.yaml
    $ source activate tensorflow_p36
    $ # Try running a Ray program with 'ray.init(address="auto")'.

    # Tear down the cluster.
    $ ray down ray/python/ray/autoscaler/aws/example-full.yaml

.. tip:: For the AWS node configuration, you can set ``"ImageId: latest_dlami"`` to automatically use the newest `Deep Learning AMI <https://aws.amazon.com/machine-learning/amis/>`_ for your region. For example, ``head_node: {InstanceType: c5.xlarge, ImageId: latest_dlami}``.

.. _aws-cluster-efs:

Using Amazon EFS
~~~~~~~~~~~~~~~~

To use Amazon EFS, install some utilities and mount the EFS in ``setup_commands``. Note that these instructions only work if you are using the AWS Autoscaler.

.. note::

  You need to replace the ``{{FileSystemId}}`` to your own EFS ID before using the config. You may also need to set correct ``SecurityGroupIds`` for the instances in the config file.

.. code-block:: yaml

    setup_commands:
        - sudo kill -9 `sudo lsof /var/lib/dpkg/lock-frontend | awk '{print $2}' | tail -n 1`;
            sudo pkill -9 apt-get;
            sudo pkill -9 dpkg;
            sudo dpkg --configure -a;
            sudo apt-get -y install binutils;
            cd $HOME;
            git clone https://github.com/aws/efs-utils;
            cd $HOME/efs-utils;
            ./build-deb.sh;
            sudo apt-get -y install ./build/amazon-efs-utils*deb;
            cd $HOME;
            mkdir efs;
            sudo mount -t efs {{FileSystemId}}:/ efs;
            sudo chmod 777 efs;

Azure
-----

First, install the Azure CLI (``pip install azure-cli azure-core``) then login using (``az login``).

Set the subscription to use from the command line (``az account set -s <subscription_id>``) or by modifying the provider section of the config provided e.g: `ray/python/ray/autoscaler/azure/example-full.yaml`

Once the Azure CLI is configured to manage resources on your Azure account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/azure/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/azure/example-full.yaml>`__ cluster config file will create a small cluster with a Standard DS2v3 head node (on-demand) configured to autoscale up to two Standard DS2v3 `spot workers <https://docs.microsoft.com/en-us/azure/virtual-machines/windows/spot-vms>`__. Note that you'll need to fill in your resource group and location in those templates.

Test that it works by running the following commands from your local machine:

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/azure/example-full.yaml

    # Get a remote screen on the head node.
    $ ray attach ray/python/ray/autoscaler/azure/example-full.yaml
    # test ray setup
    # enable conda environment
    $ exec bash -l
    $ conda activate py37_tensorflow
    $ python -c 'import ray; ray.init()'
    $ exit
    # Tear down the cluster.
    $ ray down ray/python/ray/autoscaler/azure/example-full.yaml

Azure Portal
------------

Alternatively, you can deploy a cluster using Azure portal directly. Please note that autoscaling is done using Azure VM Scale Sets and not through
the Ray autoscaler. This will deploy `Azure Data Science VMs (DSVM) <https://azure.microsoft.com/en-us/services/virtual-machines/data-science-virtual-machines/>`_
for both the head node and the auto-scalable cluster managed by `Azure Virtual Machine Scale Sets <https://azure.microsoft.com/en-us/services/virtual-machine-scale-sets/>`_.
The head node conveniently exposes both SSH as well as JupyterLab.

.. image:: https://aka.ms/deploytoazurebutton
   :target: https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fray-project%2Fray%2Fmaster%2Fdoc%2Fazure%2Fazure-ray-template.json
   :alt: Deploy to Azure

Once the template is successfully deployed the deployment output page provides the ssh command to connect and the link to the JupyterHub on the head node (username/password as specified on the template input).
Use the following code in a Jupyter notebook to connect to the Ray cluster.

.. code-block:: python

    import ray
    ray.init(address='auto')

Note that on each node the `azure-init.sh <https://github.com/ray-project/ray/blob/master/doc/azure/azure-init.sh>`_ script is executed and performs the following actions:

1. Activates one of the conda environments available on DSVM
2. Installs Ray and any other user-specified dependencies
3. Sets up a systemd task (``/lib/systemd/system/ray.service``) to start Ray in head or worker mode

GCP
---

First, install the Google API client (``pip install google-api-python-client``), set up your GCP credentials, and create a new GCP project.

Once the API client is configured to manage resources on your GCP account, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/gcp/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gcp/example-full.yaml>`__ cluster config file will create a small cluster with a n1-standard-2 head node (on-demand) configured to autoscale up to two n1-standard-2 `preemptible workers <https://cloud.google.com/preemptible-vms/>`__. Note that you'll need to fill in your project id in those templates.

Test that it works by running the following commands from your local machine:

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/gcp/example-full.yaml

    # Get a remote screen on the head node.
    $ ray attach ray/python/ray/autoscaler/gcp/example-full.yaml
    $ source activate tensorflow_p36
    $ # Try running a Ray program with 'ray.init(address="auto")'.

    # Tear down the cluster.
    $ ray down ray/python/ray/autoscaler/gcp/example-full.yaml

.. _ray-launch-k8s:

Kubernetes
----------

The cluster launcher can also be used to start Ray clusters on an existing Kubernetes cluster. First, install the Kubernetes API client (``pip install kubernetes``), then make sure your Kubernetes credentials are set up properly to access the cluster (if a command like ``kubectl get pods`` succeeds, you should be good to go).

Once you have ``kubectl`` configured locally to access the remote cluster, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/kubernetes/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-full.yaml>`__ cluster config file will create a small cluster of one pod for the head node configured to autoscale up to two worker node pods, with all pods requiring 1 CPU and 0.5GiB of memory.

Test that it works by running the following commands from your local machine:

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to get a remote shell into the head node.
    $ ray up ray/python/ray/autoscaler/kubernetes/example-full.yaml

    # List the pods running in the cluster. You shoud only see one head node
    # until you start running an application, at which point worker nodes
    # should be started. Don't forget to include the Ray namespace in your
    # 'kubectl' commands ('ray' by default).
    $ kubectl -n ray get pods

    # Get a remote screen on the head node.
    $ ray attach ray/python/ray/autoscaler/kubernetes/example-full.yaml
    $ # Try running a Ray program with 'ray.init(address="auto")'.

    # Tear down the cluster
    $ ray down ray/python/ray/autoscaler/kubernetes/example-full.yaml

.. tip:: This section describes the easiest way to launch a Ray cluster on Kubernetes. See this :ref:`document for advanced usage <ray-k8s-deploy>` of Kubernetes with Ray.

.. _cluster-private-setup:

Private Cluster (List of nodes)
-------------------------------

The most preferable way to run a Ray cluster on a private cluster of hosts is via the Ray Cluster Launcher.

You can get started by filling out the fields in the provided `ray/python/ray/autoscaler/local/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/local/example-full.yaml>`__.
Be sure to specify the proper ``head_ip``, list of ``worker_ips``, and the ``ssh_user`` field.

Test that it works by running the following commands from your local machine:

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to get a remote shell into the head node.
    $ ray up ray/python/ray/autoscaler/local/example-full.yaml

    # Get a remote screen on the head node.
    $ ray attach ray/python/ray/autoscaler/local/example-full.yaml
    $ # Try running a Ray program with 'ray.init(address="auto")'.

    # Tear down the cluster
    $ ray down ray/python/ray/autoscaler/local/example-full.yaml

External Node Provider
----------------------

Ray also supports external node providers (check `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__ implementation).
You can specify the external node provider using the yaml config:

.. code-block:: yaml

    provider:
        type: external
        module: mypackage.myclass

The module needs to be in the format `package.provider_class` or `package.sub_package.provider_class`.


Additional Cloud Providers
--------------------------

To use Ray autoscaling on other Cloud providers or cluster management systems, you can implement the ``NodeProvider`` interface (100 LOC) and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__. Contributions are welcome!


Security
--------

On cloud providers, nodes will be launched into their own security group by default, with traffic allowed only between nodes in the same group. A new SSH key will also be created and saved to your local machine for access to the cluster.

.. _cluster-config:

Configuring your Cluster
------------------------

The Ray Cluster Launcher requires a *cluster configuration file*, which specifies some important details about the cluster. At a minimum, we need to specify:

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

You are encouraged to copy the example YAML file and modify it to your needs. This may include adding additional setup commands to install libraries or sync local data files.

Setup Commands
~~~~~~~~~~~~~~

.. note:: After you have customized the nodes, it is also a good idea to create a new machine image (or docker container) and use that in the config file. This reduces worker setup time, improving the efficiency of auto-scaling.

The setup commands you use should ideally be *idempotent*, that is, can be run more than once. This allows Ray to update nodes after they have been created. You can usually make commands idempotent with small modifications, e.g. ``git clone foo`` can be rewritten as ``test -e foo || git clone foo`` which checks if the repo is already cloned first.


Common cluster configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


