.. _cluster-config:

Cluster YAML Configuration Options
==================================

The cluster configuration is defined within a YAML file that will be used by the Cluster Launcher to launch the head node, and by the Autoscaler to launch worker nodes. Once the cluster configuration is defined, you will need to use the :ref:`Ray CLI <ray-cli>` to perform any operations such as starting and stopping the cluster.

Syntax
------

.. parsed-literal::

    :ref:`cluster_name <cluster-configuration-cluster-name>`: str
    :ref:`max_workers <cluster-configuration-max-workers>`: int
    :ref:`upscaling_speed <cluster-configuration-upscaling-speed>`: float
    :ref:`idle_timeout_minutes <cluster-configuration-idle-timeout-minutes>`: int
    :ref:`docker <cluster-configuration-docker>`:
        :ref:`docker <cluster-configuration-docker-type>`
    :ref:`provider <cluster-configuration-provider>`:
        :ref:`provider <cluster-configuration-provider-type>`
    :ref:`auth <cluster-configuration-auth>`:
        :ref:`auth <cluster-configuration-auth-type>`
    :ref:`available_node_types <cluster-configuration-available-node-types>`:
        :ref:`node_types <cluster-configuration-node-types-type>`
    :ref:`worker_nodes <cluster-configuration-worker-nodes>`:
        :ref:`node_config <cluster-configuration-node-config-type>`
    :ref:`head_node_type <cluster-configuration-head-node-type>`: str
    :ref:`file_mounts <cluster-configuration-file-mounts>`:
        :ref:`file_mounts <cluster-configuration-file-mounts-type>`
    :ref:`cluster_synced_files <cluster-configuration-cluster-synced-files>`:
        - str
    :ref:`rsync_exclude <cluster-configuration-rsync-exclude>`:
        - str
    :ref:`rsync_filter <cluster-configuration-rsync-filter>`:
        - str
    :ref:`initialization_commands <cluster-configuration-initialization-commands>`:
        - str
    :ref:`setup_commands <cluster-configuration-setup-commands>`:
        - str
    :ref:`head_setup_commands <cluster-configuration-head-setup-commands>`:
        - str
    :ref:`worker_setup_commands <cluster-configuration-worker-setup-commands>`:
        - str
    :ref:`head_start_ray_commands <cluster-configuration-head-start-ray-commands>`:
        - str
    :ref:`worker_start_ray_commands <cluster-configuration-worker-start-ray-commands>`:
        - str

Custom types
------------

.. _cluster-configuration-docker-type:

Docker
~~~~~~

.. parsed-literal::
    :ref:`image <cluster-configuration-image>`: str
    :ref:`head_image <cluster-configuration-head-image>`: str
    :ref:`worker_image <cluster-configuration-worker-image>`: str
    :ref:`container_name <cluster-configuration-container-name>`: str
    :ref:`pull_before_run <cluster-configuration-pull-before-run>`: bool
    :ref:`run_options <cluster-configuration-run-options>`:
        - str
    :ref:`head_run_options <cluster-configuration-head-run-options>`:
        - str
    :ref:`worker_run_options <cluster-configuration-worker-run-options>`:
        - str
    :ref:`disable_automatic_runtime_detection <cluster-configuration-disable-automatic-runtime-detection>`: bool
    :ref:`disable_shm_size_detection <cluster-configuration-disable-shm-size-detection>`: bool

.. _cluster-configuration-auth-type:

Auth
~~~~

.. tabs::
    .. group-tab:: AWS

        .. parsed-literal::

            :ref:`ssh_user <cluster-configuration-ssh-user>`: str
            :ref:`ssh_private_key <cluster-configuration-ssh-private-key>`: str

    .. group-tab:: Azure

        .. parsed-literal::

            :ref:`ssh_user <cluster-configuration-ssh-user>`: str
            :ref:`ssh_private_key <cluster-configuration-ssh-private-key>`: str
            :ref:`ssh_public_key <cluster-configuration-ssh-public-key>`: str

    .. group-tab:: GCP

        .. parsed-literal::

            :ref:`ssh_user <cluster-configuration-ssh-user>`: str
            :ref:`ssh_private_key <cluster-configuration-ssh-private-key>`: str

.. _cluster-configuration-provider-type:

Provider
~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        .. parsed-literal::

            :ref:`type <cluster-configuration-type>`: str
            :ref:`region <cluster-configuration-region>`: str
            :ref:`availability_zone <cluster-configuration-availability-zone>`: str
            :ref:`cache_stopped_nodes <cluster-configuration-cache-stopped-nodes>`: bool

    .. group-tab:: Azure

        .. parsed-literal::

            :ref:`type <cluster-configuration-type>`: str
            :ref:`location <cluster-configuration-location>`: str
            :ref:`resource_group <cluster-configuration-resource-group>`: str
            :ref:`subscription_id <cluster-configuration-subscription-id>`: str
            :ref:`cache_stopped_nodes <cluster-configuration-cache-stopped-nodes>`: bool

    .. group-tab:: GCP

        .. parsed-literal::

            :ref:`type <cluster-configuration-type>`: str
            :ref:`region <cluster-configuration-region>`: str
            :ref:`availability_zone <cluster-configuration-availability-zone>`: str
            :ref:`project_id <cluster-configuration-project-id>`: str
            :ref:`cache_stopped_nodes <cluster-configuration-cache-stopped-nodes>`: bool

.. _cluster-configuration-node-types-type:

Node types
~~~~~~~~~~

The nodes types object's keys represent the names of the different node types.

.. parsed-literal::
    <node_type_1_name>:
        :ref:`node_config <cluster-configuration-node-config>`:
            :ref:`Node config <cluster-configuration-node-config-type>`
        :ref:`resources <cluster-configuration-resources>`:
            :ref:`Resources <cluster-configuration-resources-type>`
        :ref:`min_workers <cluster-configuration-node-min-workers>`: int
        :ref:`max_workers <cluster-configuration-node-max-workers>`: int
        :ref:`worker_setup_commands <cluster-configuration-node-type-worker-setup-commands>`:
            - str
        :ref:`docker <cluster-configuration-node-docker>`:
            :ref:`Node Docker <cluster-configuration-node-docker-type>`
    <node_type_2_name>:
        ...
    ...

.. _cluster-configuration-node-config-type:

Node config
~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        A YAML object which conforms to the EC2 ``create_instances`` API in `the AWS docs <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances>`_.

    .. group-tab:: Azure

        A YAML object as defined in `the deployment template <https://docs.microsoft.com/en-us/azure/templates/microsoft.compute/virtualmachines>`_ whose resources are defined in `the Azure docs <https://docs.microsoft.com/en-us/azure/templates/>`_.

    .. group-tab:: GCP

        A YAML object as defined in `the GCP docs <https://cloud.google.com/compute/docs/reference/rest/v1/instances>`_.

.. _cluster-configuration-node-docker-type:

Node Docker
~~~~~~~~~~~

.. parsed-literal::

    :ref:`image <cluster-configuration-image>`: str
    :ref:`pull_before_run <cluster-configuration-pull-before-run>`: bool
    :ref:`worker_run_options <cluster-configuration-worker-run-options>`:
        - str
    :ref:`disable_automatic_runtime_detection <cluster-configuration-disable-automatic-runtime-detection>`: bool
    :ref:`disable_shm_size_detection <cluster-configuration-disable-shm-size-detection>`: bool

.. _cluster-configuration-resources-type:

Resources
~~~~~~~~~

.. parsed-literal::

    :ref:`CPU <cluster-configuration-CPU>`: int
    :ref:`GPU <cluster-configuration-GPU>`: int
    <custom_resource1>: int
    <custom_resource2>: int
    ...

.. _cluster-configuration-file-mounts-type:

File mounts
~~~~~~~~~~~

.. parsed-literal::
    <path1_on_remote_machine>: str # Path 1 on local machine
    <path2_on_remote_machine>: str # Path 2 on local machine
    ...

Properties and Definitions
--------------------------

.. _cluster-configuration-cluster-name:

``cluster_name``
~~~~~~~~~~~~~~~~

The name of the cluster. This is the namespace of the cluster.

* **Required:** Yes
* **Importance:** High
* **Type:** String
* **Default:** "default"
* **Pattern:** ``[a-zA-Z0-9_]+``

.. _cluster-configuration-max-workers:

``max_workers``
~~~~~~~~~~~~~~~

The maximum number of workers the cluster will have at any given time.

* **Required:** No
* **Importance:** High
* **Type:** Integer
* **Default:** ``2``
* **Minimum:** ``0``
* **Maximum:** Unbounded

.. _cluster-configuration-upscaling-speed:

``upscaling_speed``
~~~~~~~~~~~~~~~~~~~

The number of nodes allowed to be pending as a multiple of the current number of nodes. For example, if set to 1.0, the cluster can grow in size by at most 100% at any time, so if the cluster currently has 20 nodes, at most 20 pending launches are allowed. Note that although the autoscaler will scale down to `min_workers` (which could be 0), it will always scale up to 5 nodes at a minimum when scaling up. 

* **Required:** No
* **Importance:** Medium
* **Type:** Float
* **Default:** ``1.0``
* **Minimum:** ``0.0``
* **Maximum:** Unbounded

.. _cluster-configuration-idle-timeout-minutes:

``idle_timeout_minutes``
~~~~~~~~~~~~~~~~~~~~~~~~

The number of minutes that need to pass before an idle worker node is removed by the Autoscaler.

* **Required:** No
* **Importance:** Medium
* **Type:** Integer
* **Default:** ``5``
* **Minimum:** ``0``
* **Maximum:** Unbounded

.. _cluster-configuration-docker:

``docker``
~~~~~~~~~~

Configure Ray to run in Docker containers.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`Docker <cluster-configuration-docker-type>`
* **Default:** ``{}``

In rare cases when Docker is not available on the system by default (e.g., bad AMI), add the following commands to :ref:`initialization_commands <cluster-configuration-initialization-commands>` to install it.

.. code-block:: yaml

    initialization_commands:
        - curl -fsSL https://get.docker.com -o get-docker.sh
        - sudo sh get-docker.sh
        - sudo usermod -aG docker $USER
        - sudo systemctl restart docker -f

.. _cluster-configuration-provider:

``provider``
~~~~~~~~~~~~

The cloud provider-specific configuration properties.

* **Required:** Yes
* **Importance:** High
* **Type:** :ref:`Provider <cluster-configuration-provider-type>`

.. _cluster-configuration-auth:

``auth``
~~~~~~~~

Authentication credentials that Ray will use to launch nodes.

* **Required:** Yes
* **Importance:** High
* **Type:** :ref:`Auth <cluster-configuration-auth-type>`

.. _cluster-configuration-available-node-types:

``available_node_types``
~~~~~~~~~~~~~~~~~~~~~~~~

Tells the autoscaler the allowed node types and the resources they provide.
The key is the name of the node type, which is just for debugging purposes.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`Node types <cluster-configuration-node-types-type>`
* **Default:**

.. tabs::
    .. group-tab:: AWS

        .. code-block:: yaml

          available_node_types:
            ray.head.default:
                node_config:
                  InstanceType: m5.large
                  BlockDeviceMappings:
                      - DeviceName: /dev/sda1
                        Ebs:
                            VolumeSize: 100
                resources: {"CPU": 2}
                min_workers: 0
                max_workers: 0
            ray.worker.default:
                node_config:
                  InstanceType: m5.large
                  InstanceMarketOptions:
                      MarketType: spot
                resources: {"CPU": 2}
                min_workers: 0

.. _cluster-configuration-head-node-type:

``head_node_type``
~~~~~~~~~~~~~~~~~~

The key for one of the node types in :ref:`available_node_types <cluster-configuration-available-node-types>`. This node type will be used to launch the head node.


* **Required:** Yes
* **Importance:** High
* **Type:** String
* **Pattern:** ``[a-zA-Z0-9_]+``

.. _cluster-configuration-worker-nodes:

``worker_nodes``
~~~~~~~~~~~~~~~~

The configuration to be used to launch worker nodes on the cloud service provider. Generally, node configs are set in the :ref:`node config of each node type <cluster-configuration-node-config>`. Setting this property allows propagation of a default value to all the node types when they launch as workers (e.g., using spot instances across all workers can be configured here so that it doesn't have to be set across all instance types).

* **Required:** No
* **Importance:** Low
* **Type:** :ref:`Node config <cluster-configuration-node-config-type>`
* **Default:** ``{}``

.. _cluster-configuration-file-mounts:

``file_mounts``
~~~~~~~~~~~~~~~

The files or directories to copy to the head and worker nodes.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`File mounts <cluster-configuration-file-mounts-type>`
* **Default:** ``[]``

.. _cluster-configuration-cluster-synced-files:

``cluster_synced_files``
~~~~~~~~~~~~~~~~~~~~~~~~

A list of paths to the files or directories to copy from the head node to the worker nodes. The same path on the head node will be copied to the worker node. This behavior is a subset of the file_mounts behavior, so in the vast majority of cases one should just use :ref:`file_mounts <cluster-configuration-file-mounts>`.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-rsync-exclude:

``rsync_exclude``
~~~~~~~~~~~~~~~~~

A list of patterns for files to exclude when running ``rsync up`` or ``rsync down``. The filter is applied on the source directory only.

Example for a pattern in the list: ``**/.git/**``.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-rsync-filter:

``rsync_filter``
~~~~~~~~~~~~~~~~

A list of patterns for files to exclude when running ``rsync up`` or ``rsync down``. The filter is applied on the source directory and recursively through all subdirectories.

Example for a pattern in the list: ``.gitignore``.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-initialization-commands:

``initialization_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A list of commands that will be run before the :ref:`setup commands <cluster-configuration-setup-commands>`. If Docker is enabled, these commands will run outside the container and before Docker is setup.

* **Required:** No
* **Importance:** Medium
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-setup-commands:

``setup_commands``
~~~~~~~~~~~~~~~~~~

A list of commands to run to set up nodes. These commands will always run on the head and worker nodes and will be merged with :ref:`head setup commands <cluster-configuration-head-setup-commands>` for head and with :ref:`worker setup commands <cluster-configuration-worker-setup-commands>` for workers.

* **Required:** No
* **Importance:** Medium
* **Type:** List of String
* **Default:**

.. tabs::
    .. group-tab:: AWS

        .. code-block:: yaml

            # Default setup_commands:
            setup_commands:
              - echo 'export PATH="$HOME/anaconda3/envs/tensorflow_p36/bin:$PATH"' >> ~/.bashrc
              - pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp36-cp36m-manylinux2014_x86_64.whl

- Setup commands should ideally be *idempotent* (i.e., can be run multiple times without changing the result); this allows Ray to safely update nodes after they have been created. You can usually make commands idempotent with small modifications, e.g. ``git clone foo`` can be rewritten as ``test -e foo || git clone foo`` which checks if the repo is already cloned first.

- Setup commands are run sequentially but separately. For example, if you are using anaconda, you need to run ``conda activate env && pip install -U ray`` because splitting the command into two setup commands will not work.

- Ideally, you should avoid using setup_commands by creating a docker image with all the dependencies preinstalled to minimize startup time.

- **Tip**: if you also want to run apt-get commands during setup add the following list of commands:

    .. code-block:: yaml

        setup_commands:
          - sudo pkill -9 apt-get || true
          - sudo pkill -9 dpkg || true
          - sudo dpkg --configure -a

.. _cluster-configuration-head-setup-commands:

``head_setup_commands``
~~~~~~~~~~~~~~~~~~~~~~~

A list of commands to run to set up the head node. These commands will be merged with the general :ref:`setup commands <cluster-configuration-setup-commands>`.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-worker-setup-commands:

``worker_setup_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~

A list of commands to run to set up the worker nodes. These commands will be merged with the general :ref:`setup commands <cluster-configuration-setup-commands>`.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-head-start-ray-commands:

``head_start_ray_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Commands to start ray on the head node. You don't need to change this.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:**

.. tabs::
    .. group-tab:: AWS

        .. code-block:: yaml

            head_start_ray_commands:
              - ray stop
              - ulimit -n 65536; ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml

.. _cluster-configuration-worker-start-ray-commands:

``worker_start_ray_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Command to start ray on worker nodes. You don't need to change this.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:**

.. tabs::
    .. group-tab:: AWS

        .. code-block:: yaml

            worker_start_ray_commands:
              - ray stop
              - ulimit -n 65536; ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076

.. _cluster-configuration-image:

``docker.image``
~~~~~~~~~~~~~~~~

The default Docker image to pull in the head and worker nodes. This can be overridden by the :ref:`head_image <cluster-configuration-head-image>` and :ref:`worker_image <cluster-configuration-worker-image>` fields. If neither `image` nor (:ref:`head_image <cluster-configuration-head-image>` and :ref:`worker_image <cluster-configuration-worker-image>`) are specified, Ray will not use Docker.

* **Required:** Yes (If Docker is in use.)
* **Importance:** High
* **Type:** String

The Ray project provides Docker images on `DockerHub <https://hub.docker.com/u/rayproject>`_. The repository includes following images:

* ``rayproject/ray-ml:latest-gpu``: CUDA support, includes ML dependencies.
* ``rayproject/ray:latest-gpu``: CUDA support, no ML dependencies.
* ``rayproject/ray-ml:latest``: No CUDA support, includes ML dependencies.
* ``rayproject/ray:latest``: No CUDA support, no ML dependencies.

.. _cluster-configuration-head-image:

``docker.head_image``
~~~~~~~~~~~~~~~~~~~~~
Docker image for the head node to override the default :ref:`docker image <cluster-configuration-image>`.

* **Required:** No
* **Importance:** Low
* **Type:** String

.. _cluster-configuration-worker-image:

``docker.worker_image``
~~~~~~~~~~~~~~~~~~~~~~~
Docker image for the worker nodes to override the default :ref:`docker image <cluster-configuration-image>`.

* **Required:** No
* **Importance:** Low
* **Type:** String

.. _cluster-configuration-container-name:

``docker.container_name``
~~~~~~~~~~~~~~~~~~~~~~~~~

The name to use when starting the Docker container.

* **Required:** Yes (If Docker is in use.)
* **Importance:** Low
* **Type:** String
* **Default:** ray_container

.. _cluster-configuration-pull-before-run:

``docker.pull_before_run``
~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, the latest version of image will be pulled when starting Docker. If disabled, ``docker run`` will only pull the image if no cached version is present.

* **Required:** No
* **Importance:** Medium
* **Type:** Boolean
* **Default:** ``True``

.. _cluster-configuration-run-options:

``docker.run_options``
~~~~~~~~~~~~~~~~~~~~~~

The extra options to pass to ``docker run``.

* **Required:** No
* **Importance:** Medium
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-head-run-options:

``docker.head_run_options``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The extra options to pass to ``docker run`` for head node only.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-worker-run-options:

``docker.worker_run_options``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The extra options to pass to ``docker run`` for worker nodes only.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-disable-automatic-runtime-detection:

``docker.disable_automatic_runtime_detection``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, Ray will not try to use the NVIDIA Container Runtime if GPUs are present.

* **Required:** No
* **Importance:** Low
* **Type:** Boolean
* **Default:** ``False``


.. _cluster-configuration-disable-shm-size-detection:

``docker.disable_shm_size_detection``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, Ray will not automatically specify the size ``/dev/shm`` for the started container and the runtime's default value (64MiB for Docker) will be used.
If ``--shm-size=<>`` is manually added to ``run_options``, this is *automatically* set to ``True``, meaning that Ray will defer to the user-provided value.

* **Required:** No
* **Importance:** Low
* **Type:** Boolean
* **Default:** ``False``


.. _cluster-configuration-ssh-user:

``auth.ssh_user``
~~~~~~~~~~~~~~~~~

The user that Ray will authenticate with when launching new nodes.

* **Required:** Yes
* **Importance:** High
* **Type:** String

.. _cluster-configuration-ssh-private-key:

``auth.ssh_private_key``
~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The path to an existing private key for Ray to use. If not configured, Ray will create a new private keypair (default behavior). If configured, the key must be added to the project-wide metadata and ``KeyName`` has to be defined in the :ref:`node configuration <cluster-configuration-node-config>`.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String

    .. group-tab:: Azure

        The path to an existing private key for Ray to use.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String

        You may use ``ssh-keygen -t rsa -b 4096`` to generate a new ssh keypair.

    .. group-tab:: GCP

        The path to an existing private key for Ray to use. If not configured, Ray will create a new private keypair (default behavior). If configured, the key must be added to the project-wide metadata and ``KeyName`` has to be defined in the :ref:`node configuration <cluster-configuration-node-config>`.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String

.. _cluster-configuration-ssh-public-key:

``auth.ssh_public_key``
~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not available.

    .. group-tab:: Azure

        The path to an existing public key for Ray to use.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String

    .. group-tab:: GCP

        Not available.

.. _cluster-configuration-type:

``provider.type``
~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The cloud service provider. For AWS, this must be set to ``aws``.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String

    .. group-tab:: Azure

        The cloud service provider. For Azure, this must be set to ``azure``.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String

    .. group-tab:: GCP

        The cloud service provider. For GCP, this must be set to ``gcp``.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String

.. _cluster-configuration-region:

``provider.region``
~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The region to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Default:** us-west-2

    .. group-tab:: Azure

        Not available.

    .. group-tab:: GCP

        The region to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Default:** us-west1

.. _cluster-configuration-availability-zone:

``provider.availability_zone``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        A string specifying a comma-separated list of availability zone(s) that nodes may be launched in.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Default:** us-west-2a,us-west-2b

    .. group-tab:: Azure

        Not available.

    .. group-tab:: GCP

        A string specifying a comma-separated list of availability zone(s) that nodes may be launched in.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Default:** us-west1-a

.. _cluster-configuration-location:

``provider.location``
~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not available.

    .. group-tab:: Azure

        The location to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Default:** westus2

    .. group-tab:: GCP

        Not available.

.. _cluster-configuration-resource-group:

``provider.resource_group``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not available.

    .. group-tab:: Azure

        The resource group to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Default:** ray-cluster

    .. group-tab:: GCP

        Not available.

.. _cluster-configuration-subscription-id:

``provider.subscription_id``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not available.

    .. group-tab:: Azure

        The subscription ID to use for deployment of the Ray cluster. If not specified, Ray will use the default from the Azure CLI.

        * **Required:** No
        * **Importance:** High
        * **Type:** String
        * **Default:** ``""``

    .. group-tab:: GCP

        Not available.

.. _cluster-configuration-project-id:

``provider.project_id``
~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not available.

    .. group-tab:: Azure

        Not available.

    .. group-tab:: GCP

        The globally unique project ID to use for deployment of the Ray cluster.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Default:** ``null``

.. _cluster-configuration-cache-stopped-nodes:

``provider.cache_stopped_nodes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, nodes will be *stopped* when the cluster scales down. If disabled, nodes will be *terminated* instead. Stopped nodes launch faster than terminated nodes.


* **Required:** No
* **Importance:** Low
* **Type:** Boolean
* **Default:** ``True``

.. _cluster-configuration-node-config:

``available_node_types.<node_type_name>.node_type.node_config``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The configuration to be used to launch the nodes on the cloud service provider. Among other things, this will specify the instance type to be launched.

* **Required:** Yes
* **Importance:** High
* **Type:** :ref:`Node config <cluster-configuration-node-config-type>`

.. _cluster-configuration-resources:

``available_node_types.<node_type_name>.node_type.resources``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The resources that a node type provides, which enables the autoscaler to automatically select the right type of nodes to launch given the resource demands of the application. The resources specified will be automatically passed to the ``ray start`` command for the node via an environment variable. If not provided, Autoscaler can automatically detect them only for AWS/Kubernetes cloud providers. For more information, see also the `resource demand scheduler <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/resource_demand_scheduler.py>`_

* **Required:** Yes (except for AWS/K8s)
* **Importance:** High
* **Type:** :ref:`Resources <cluster-configuration-resources-type>`
* **Default:** ``{}``

In some cases, adding special nodes without any resources may be desirable. Such nodes can be used as a driver which connects to the cluster to launch jobs. In order to manually add a node to an autoscaled cluster, the *ray-cluster-name* tag should be set and *ray-node-type* tag should be set to unmanaged. Unmanaged nodes can be created by setting the resources to ``{}`` and the :ref:`maximum workers <cluster-configuration-node-min-workers>` to 0. The Autoscaler will not attempt to start, stop, or update unmanaged nodes. The user is responsible for properly setting up and cleaning up unmanaged nodes.

.. _cluster-configuration-node-min-workers:

``available_node_types.<node_type_name>.node_type.min_workers``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The minimum number of workers to maintain for this node type regardless of utilization.

* **Required:** No
* **Importance:** High
* **Type:** Integer
* **Default:** ``0``
* **Minimum:** ``0``
* **Maximum:** Unbounded

.. _cluster-configuration-node-max-workers:

``available_node_types.<node_type_name>.node_type.max_workers``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The maximum number of workers to have in the cluster for this node type regardless of utilization. This takes precedence over :ref:`minimum workers <cluster-configuration-node-min-workers>`. By default, the number of workers of a node type is unbounded, constrained only by the cluster-wide :ref:`max_workers <cluster-configuration-max-workers>`. (Prior to Ray 1.3.0, the default value for this field was 0.)

* **Required:** No
* **Importance:** High
* **Type:** Integer
* **Default:** cluster-wide :ref:`max_workers <cluster-configuration-max-workers>`
* **Minimum:** ``0``
* **Maximum:** cluster-wide :ref:`max_workers <cluster-configuration-max-workers>`

.. _cluster-configuration-node-type-worker-setup-commands:

``available_node_types.<node_type_name>.node_type.worker_setup_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A list of commands to run to set up worker nodes of this type. These commands will replace the general :ref:`worker setup commands <cluster-configuration-worker-setup-commands>` for the node.

* **Required:** No
* **Importance:** low
* **Type:** List of String
* **Default:** ``[]``

.. _cluster-configuration-cpu:

``available_node_types.<node_type_name>.node_type.resources.CPU``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The number of CPUs made available by this node. If not configured, Autoscaler can automatically detect them only for AWS/Kubernetes cloud providers.

        * **Required:** Yes (except for AWS/K8s)
        * **Importance:** High
        * **Type:** Integer

    .. group-tab:: Azure

        The number of CPUs made available by this node.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** Integer

    .. group-tab:: GCP

        The number of CPUs made available by this node.

        * **Required:** No
        * **Importance:** High
        * **Type:** Integer


.. _cluster-configuration-gpu:

``available_node_types.<node_type_name>.node_type.resources.GPU``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The number of GPUs made available by this node. If not configured, Autoscaler can automatically detect them only for AWS/Kubernetes cloud providers.

        * **Required:** No
        * **Importance:** Low
        * **Type:** Integer

    .. group-tab:: Azure

        The number of GPUs made available by this node.

        * **Required:** No
        * **Importance:** High
        * **Type:** Integer

    .. group-tab:: GCP

        The number of GPUs made available by this node.

        * **Required:** No
        * **Importance:** High
        * **Type:** Integer

.. _cluster-configuration-node-docker:

``available_node_types.<node_type_name>.docker``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A set of overrides to the top-level :ref:`Docker <cluster-configuration-docker>` configuration.

* **Required:** No
* **Importance:** Low
* **Type:** :ref:`docker <cluster-configuration-node-docker-type>`
* **Default:** ``{}``

Examples
--------

Minimal configuration
~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        .. literalinclude:: ../../../python/ray/autoscaler/aws/example-minimal.yaml
            :language: yaml

    .. group-tab:: Azure

        .. literalinclude:: ../../../python/ray/autoscaler/azure/example-minimal.yaml
            :language: yaml

    .. group-tab:: GCP

        .. literalinclude:: ../../../python/ray/autoscaler/gcp/example-minimal.yaml
            :language: yaml

Full configuration
~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        .. literalinclude:: ../../../python/ray/autoscaler/aws/example-full.yaml
            :language: yaml

    .. group-tab:: Azure

        .. literalinclude:: ../../../python/ray/autoscaler/azure/example-full.yaml
            :language: yaml

    .. group-tab:: GCP

        .. literalinclude:: ../../../python/ray/autoscaler/gcp/example-full.yaml
            :language: yaml
