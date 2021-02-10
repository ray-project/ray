.. _cluster-config:

Configuration Options
=====================

The cluster configuration is defined within a YAML file that will be used by the Cluster Launcher to launch the head node, and by the Autoscaler to launch worker nodes. Once the cluster configuration is defined, you will need to use the :ref:`Ray CLI <package-ref-command-line-api>` to perform any operations such as starting and stopping the cluster.

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
    :ref:`file_mounts_sync_continuously <cluster-configuration-file-mounts-sync-continuously>`: bool
    :ref:`rsync_exclude <cluster-configuration-rsync-exclude>`:
        - str
    :ref:`rsync_filter <cluster-configuration-rsync-filter>`:
        - str
    :ref:`initialization_commands <cluster-configuration-initialization-commands>`:
        - str
    :ref:`setup_commands <cluster-configuration-setup-commands>`:
        - str

Custom types
------------

.. _cluster-configuration-docker-type:

Docker
~~~~~~

.. parsed-literal::
    :ref:`image <cluster-configuration-image>`: str
    :ref:`container_name <cluster-configuration-container-name>`: str
    :ref:`pull_before_run <cluster-configuration-pull-before-run>`: bool
    :ref:`run_options <cluster-configuration-run-options>`:
        - str
    :ref:`disable_automatic_runtime_detection <cluster-configuration-disable-automatic-runtime-detection>`: bool
    :ref:`disable_shm_size_detection <cluster-configuration-disable-shm-size-detection>`: bool
    :ref:`head_image <cluster-configuration-head-image>`: str
    :ref:`worker_image <cluster-configuration-worker-image>`: str
    :ref:`head_run_options <cluster-configuration-run-options>`:
        - str
    :ref:`worker_run_options <cluster-configuration-run-options>`:
        - str    

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
        :ref:`worker_setup_commands <cluster-configuration-worker-setup-commands>`:
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

        A YAML object as defined in `the AWS docs <https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-ec2-instance.html>`_.

    .. group-tab:: Azure

        A YAML object as defined in `the deployment template <https://docs.microsoft.com/en-us/azure/templates/microsoft.compute/virtualmachines>`_ whose resources are defined in `the Azure docs <https://docs.microsoft.com/en-us/azure/templates/>`_.

    .. group-tab:: GCP

        A YAML object as defined in `the GCP docs <https://cloud.google.com/compute/docs/reference/rest/v1/instances>`_.

.. _cluster-configuration-node-docker-type:

Node Docker
~~~~~~~~~~~

.. parsed-literal::

    :ref:`worker_image <cluster-configuration-image>`: str
    :ref:`pull_before_run <cluster-configuration-pull-before-run>`: bool
    :ref:`worker_run_options <cluster-configuration-run-options>`:
        - str

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

Properties
----------

.. _cluster-configuration-cluster-name:

``cluster_name``
~~~~~~~~~~~~~~~~

The name of the cluster.

* **Required:** Yes
* **Importance:** High
* **Type:** String
* **Pattern:** **TODO**
* **Update requires:** Restart

.. _cluster-configuration-max-workers:

``max_workers``
~~~~~~~~~~~~~~~

The maximum number of workers to maintain in the cluster regardless of utilization.

* **Required:** No
* **Importance:** High
* **Type:** Integer
* **Default:** ``3``
* **Minimum:** ``0``
* **Maximum:** Unbounded
* **Update requires:** Restart

.. _cluster-configuration-upscaling-speed:

``upscaling_speed``
~~~~~~~~~~~~~~~~~~~

The number of nodes allowed to be pending as a multiple of the current number of nodes. For example, if set to 1.0, the cluster can grow in size by at most 100% at any time, so if the cluster currently has 20 nodes, at most 20 pending launches are allowed.

* **Required:** No
* **Importance:** Medium
* **Type:** Float
* **Default:** ``1.0``
* **Minimum:** ``0.0``
* **Maximum:** Unbounded
* **Update requires:** Restart

.. _cluster-configuration-idle-timeout-minutes:

``idle_timeout_minutes``
~~~~~~~~~~~~~~~~~~~~~~~~

The number of minutes that need to elapse with an idle node before it is removed by the Autoscaler.

* **Required:** No
* **Importance:** Medium
* **Type:** Integer
* **Default:** ``5``
* **Minimum:** ``0``
* **Maximum:** Unbounded
* **Update requires:** Restart

.. _cluster-configuration-docker:

``docker``
~~~~~~~~~~

Configure to execute all commands on all nodes and Ray tasks in the Docker container, and open all the necessary ports to support the Ray cluster.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`Docker <cluster-configuration-docker-type>`
* **Default:** ``{}``
* **Update requires:** Restart

If Docker is not installed, add the following commands to ``initialization_commands`` to install it.

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
* **Update requires:** Restart

.. _cluster-configuration-auth:

``auth``
~~~~~~~~

A definition for how Ray will authenticate with newly launched nodes.

* **Required:** Yes
* **Importance:** High
* **Type:** :ref:`Auth <cluster-configuration-auth-type>`
* **Update requires:** Restart

.. _cluster-configuration-available-node-types:

``available_node_types``
~~~~~~~~~~~~~~~~~~~~~~~~

The definition of node types that are available to launch and scale the Ray cluster.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`Node types <cluster-configuration-node-types-type>`
* **Default:** **TODO**
* **Update requires:** Restart

.. _cluster-configuration-head-node-type:

``head_node_type``
~~~~~~~~~~~~~~~~~~

The key for one of the node types in ``available_node_types``. This node type will be used to launch the head node.

* **Required:** Yes (unless :ref:`node types <cluster-configuration-available-node-types>` is not configured either)
* **Importance:** High
* **Type:** String
* **Pattern:** **TODO**
* **Update requires:** Restart

.. _cluster-configuration-worker-nodes:

``worker_nodes``
~~~~~~~~~~~~~~~~

The configuration to be used to launch worker nodes on the cloud service provider. Generally, node configs are set in the :ref:`node config of each node type <cluster-configuration-node-config>`. Setting this property allows propagation of a default value to all the node types when they launch as workers (e.g., using spot instances across all workers can be configured here so that it doesn't have to be set across all instance types).

* **Required:** No
* **Importance:** Medium
* **Type:** :ref:`Node config <cluster-configuration-node-config-type>`
* **Default:** ``{}``
* **Update requires:** Restart

.. _cluster-configuration-file-mounts:

``file_mounts``
~~~~~~~~~~~~~~~

The files or directories to copy to the head and worker nodes.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`File mounts <cluster-configuration-file-mounts-type>`
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-cluster-synced-files:

``cluster_synced_files``
~~~~~~~~~~~~~~~~~~~~~~~~

A list of paths to the files or directories to copy from the head node to the worker nodes. The same path on the head node will be copied to the worker node. This behavior is a subset of the file_mounts behavior, so in the vast majority of cases one should just use ``file_mounts``.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-file-mounts-sync-continuously:

``file_mounts_sync_continuously``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, changes to directories in ``file_mounts`` or ``cluster_synced_files`` in the head node should sync to the worker nodes continuously.

* **Required:** No
* **Importance:** Low
* **Type:** Boolean
* **Default:** ``False``
* **Update requires:** Restart

.. _cluster-configuration-rsync-exclude:

``rsync_exclude``
~~~~~~~~~~~~~~~~~

A list of patterns for files to exclude when running ``rsync up`` or ``rsync down``. The filter is applied on the source directory only.

* **Required:** No
* **Importance:** Medium
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-rsync-filter:

``rsync_filter``
~~~~~~~~~~~~~~~~

A list of patterns for files to exclude when running ``rsync up`` or ``rsync down``. The filter is applied on the source directory and recursively through all subdirectories.

* **Required:** No
* **Importance:** Low
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-initialization-commands:

``initialization_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A list of commands that will be run before the :ref:`setup commands <cluster-configuration-setup-commands>`. If Docker is enabled, these commands will run outside the container and before Docker is setup.

* **Required:** No
* **Importance:** Medium
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-setup-commands:

``setup_commands``
~~~~~~~~~~~~~~~~~~

A list of commands to run to set up nodes. These commands will always run on the head node, and will run on the worker nodes unless overridden by :ref:`worker setup commands <cluster-configuration-worker-setup-commands>`.

* **Required:** No
* **Importance:** High
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart

Setup commands you use should ideally be *idempotent* (i.e., can be run multiple times without changing the result); this allows Ray to safely update nodes after they have been created. You can usually make commands idempotent with small modifications, e.g. ``git clone foo`` can be rewritten as ``test -e foo || git clone foo`` which checks if the repo is already cloned first.

After you have customized the nodes, create a new machine image (or docker container) and use that in the config file to reduce setup times.

.. _cluster-configuration-image:

``docker.image``
~~~~~~~~~~~~~~~~

The Docker image to pull in the head and worker nodes. If no image is specified, Ray will not use Docker.

* **Required:** Yes (If Docker is in use.)
* **Importance:** High
* **Type:** String
* **Update requires:** Restart

The Ray project provides Docker images on `DockerHub <https://hub.docker.com/u/rayproject>`_. The repository includes following images:

* ``rayproject/ray-ml:latest-gpu``: CUDA support, includes ML dependencies.
* ``rayproject/ray:latest-gpu``: CUDA support, no ML dependencies.
* ``rayproject/ray-ml:latest``: No CUDA support, includes ML dependencies.
* ``rayproject/ray:latest``: No CUDA support, no ML dependencies.

.. _cluster-configuration-container-name:

``docker.container_name``
~~~~~~~~~~~~~~~~~~~~~~~~~

The name to use when starting the Docker container.

* **Required:** Yes (If Docker is in use.)
* **Importance:** Low
* **Type:** String
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-pull-before-run:

``docker.pull_before_run``
~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, the latest version of image will be pulled when starting Docker. If disabled, ``docker run`` will only pull the image if no cached version is present.

* **Required:** No
* **Importance:** Medium
* **Type:** Boolean
* **Default:** ``False``
* **Update requires:** Restart

.. _cluster-configuration-run-options:

``docker.run_options``
~~~~~~~~~~~~~~~~~~~~~~

The extra options to pass to ``docker run``.

* **Required:** No
* **Importance:** Medium
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart


.. _cluster-configuration-disable-automatic-runtime-detection:

``docker.disable_automatic_runtime_detection``
~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, Ray will not try to use the NVIDIA Container Runtime if GPUs are present.

* **Required:** No
* **Importance:** Low
* **Type:** Boolean
* **Default:** ``False``


### disable_shm_size_detection
.. _cluster-configuration-disable-shm-size-detection:

``docker.disable_shm_size_detection``
~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, Ray will not automatically specify the size /dev/shm for the started container and the runtime's default value (64MiB for Docker) will be used.

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
* **Update requires:** Restart

.. _cluster-configuration-ssh-private-key:

``auth.ssh_private_key``
~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The path to an existing private key for Ray to use. If not configured, Ray will create a new private keypair (default behavior). If configured, the key must be added to the project-wide metadata and ``KeyName`` has to be defined in the :ref:`node configuration <cluster-configuration-node-config>`.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Update requires:** Restart

    .. group-tab:: Azure

        The path to an existing private key for Ray to use.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Update requires:** Restart

        You may use ``ssh-keygen -t rsa -b 4096`` to generate a new ssh keypair.

    .. group-tab:: GCP

        The path to an existing private key for Ray to use. If not configured, Ray will create a new private keypair (default behavior). If configured, the key must be added to the project-wide metadata and ``KeyName`` has to be defined in the :ref:`node configuration <cluster-configuration-node-config>`.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Update requires:** Restart

.. _cluster-configuration-ssh-public-key:

``auth.ssh_public_key``
~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not supported.

    .. group-tab:: Azure

        The path to an existing public key for Ray to use.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Update requires:** Restart

    .. group-tab:: GCP

        Not supported.

.. _cluster-configuration-type:

``provider.type``
~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The cloud service provider. For AWS, this must be set to ``aws``.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Allowed values:** ``aws | azure | gcp``
        * **Update requires:** Restart

    .. group-tab:: Azure

        The cloud service provider. For Azure, this must be set to ``azure``.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Allowed values:** ``aws | azure | gcp``
        * **Update requires:** Restart

    .. group-tab:: GCP

        The cloud service provider. For GCP, this must be set to ``gcp``.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Allowed values:** ``aws | azure | gcp``
        * **Update requires:** Restart

.. _cluster-configuration-region:

``provider.region``
~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The region to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Update requires:** Restart

    .. group-tab:: Azure

        Not supported.

    .. group-tab:: GCP

        The region to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Update requires:** Restart

.. _cluster-configuration-availability-zone:

``provider.availability_zone``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        A string specifying a comma-separated list of availability zone(s) that nodes may be launched in.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Default:** **TODO: WHAT VALUE CAN YOU SET TO GET THE EQUIVALENT OF AN ABSENCE OF VALUE**
        * **Update requires:** Restart

    .. group-tab:: Azure

        Not supported.

    .. group-tab:: GCP

        A string specifying a comma-separated list of availability zone(s) that nodes may be launched in.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Default:** **TODO: WHAT VALUE CAN YOU SET TO GET THE EQUIVALENT OF AN ABSENCE OF VALUE**
        * **Update requires:** Restart

.. _cluster-configuration-location:

``provider.location``
~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not supported.

    .. group-tab:: Azure

        The location to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Update requires:** Restart

    .. group-tab:: GCP

        Not supported.

.. _cluster-configuration-resource-group:

``provider.resource_group``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not supported.

    .. group-tab:: Azure

        The resource group to use for deployment of the Ray cluster.

        * **Required:** Yes
        * **Importance:** High
        * **Type:** String
        * **Update requires:** Restart

    .. group-tab:: GCP

        Not supported.

.. _cluster-configuration-subscription-id:

``provider.subscription_id``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not supported.

    .. group-tab:: Azure

        The subscription ID to use for deployment of the Ray cluster. If not specified, Ray will use the default from the Azure CLI.

        * **Required:** No
        * **Importance:** High
        * **Type:** String
        * **Default:** ``""``
        * **Update requires:** Restart

    .. group-tab:: GCP

        Not supported.

.. _cluster-configuration-project-id:

``provider.project_id``
~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        Not supported.

    .. group-tab:: Azure

        Not supported.

    .. group-tab:: GCP

        The globally unique project ID to use for deployment of the Ray cluster.

        * **Required:** No
        * **Importance:** Low
        * **Type:** String
        * **Default:** ``null``
        * **Update requires:** Restart

.. _cluster-configuration-cache-stopped-nodes:

``provider.cache_stopped_nodes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If enabled, nodes will be stopped when the cluster scales down. If disabled, nodes will be terminated instead.

* **Required:** No
* **Importance:** Medium
* **Type:** Boolean
* **Default:** ``True``
* **Update requires:** Restart

.. _cluster-configuration-node-config:

``available_node_types.<node_type_name>.node_type.node_config``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The configuration to be used to launch the nodes on the cloud service provider. Among other things, this will specify the instance type to be launched.

* **Required:** No
* **Importance:** High
* **Type:** :ref:`Node config <cluster-configuration-node-config-type>`
* **Default:** ``{}``
* **Update requires:** Restart

.. _cluster-configuration-resources:

``available_node_types.<node_type_name>.node_type.resources``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The resources that a node type provides, which enables the autoscaler to automatically select the right kind of nodes to launch given the resource demands of the application. The resources specified will be automatically passed to the ``ray start`` command for the node via an environment variable. If not provided, this is automatically set by the Autoscaler based on the instance type. For more information, see also the `resource demand scheduler <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/resource_demand_scheduler.py>`_

* **Required:** No
* **Importance:** Low
* **Type:** :ref:`Resources <cluster-configuration-resources-type>`
* **Default:** ``{}``
* **Update requires:** Restart

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
* **Update requires:** Restart

.. _cluster-configuration-node-max-workers:

``available_node_types.<node_type_name>.node_type.max_workers``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The maximum number of workers to maintain for this node type regardless of utilization. If higher than the :ref:`minimum workers <cluster-configuration-node-min-workers>`, this value will apply.

* **Required:** No
* **Importance:** High
* **Type:** Integer
* **Default:** ``3``
* **Minimum:** ``0``
* **Maximum:** Unbounded
* **Update requires:** Restart

.. _cluster-configuration-worker-setup-commands:

``available_node_types.<node_type_name>.node_type.worker_setup_commands``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A list of commands to run to set up worker nodes. These commands will only run if the node is started as a worker, and will override the general :ref:`setup commands <cluster-configuration-setup-commands>` for the node.

* **Required:** No
* **Importance:** High
* **Type:** List of String
* **Default:** ``[]``
* **Update requires:** Restart

.. _cluster-configuration-cpu:

``available_node_types.<node_type_name>.node_type.resources.CPU``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The number of CPUs made available by this node. If not configured, the Autoscaler will automatically assign a value based on the instance type.

        * **Required:** No
        * **Importance:** Low
        * **Type:** Integer
        * **Default:** **TODO: WHAT VALUE CAN YOU SET TO GET THE EQUIVALENT OF AN ABSENCE OF VALUE**
        * **Update requires:** Restart

    .. group-tab:: Azure

        The number of CPUs made available by this node.
        
        * **Required:** Yes
        * **Importance:** High
        * **Type:** Integer
        * **Default:** ``0``
        * **Update requires:** Restart

    .. group-tab:: GCP

        The number of CPUs made available by this node.
        
        * **Required:** No
        * **Importance:** High
        * **Type:** Integer
        * **Default:** ``0``
        * **Update requires:** Restart



.. _cluster-configuration-gpu:

``available_node_types.<node_type_name>.node_type.resources.GPU``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::
    .. group-tab:: AWS

        The number of GPUs made available by this node. If not configured, the Autoscaler will automatically assign a value based on the instance type.

        * **Required:** No
        * **Importance:** Low
        * **Type:** Integer
        * **Default:** **TODO: WHAT VALUE CAN YOU SET TO GET THE EQUIVALENT OF AN ABSENCE OF VALUE**
        * **Update requires:** Restart

    .. group-tab:: Azure

        The number of GPUs made available by this node.
        
        * **Required:** No
        * **Importance:** High
        * **Type:** Integer
        * **Default:** ``0``
        * **Update requires:** Restart

    .. group-tab:: GCP

        The number of GPUs made available by this node.
        
        * **Required:** No
        * **Importance:** High
        * **Type:** Integer
        * **Default:** ``0``
        * **Update requires:** Restart

.. _cluster-configuration-node-docker:

``available_node_types.<node_type_name>.docker``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A set of overrides to the top-level :ref:`Docker <cluster-configuration-docker>` configuration.

* **Required:** No
* **Importance:** Low
* **Type:** :ref:`docker <cluster-configuration-node-docker-type>`
* **Default:** ``{}``
* **Update requires:** Restart
