.. _resource-isolation:

=================================
Resource Isolation With Cgroup v2
=================================

This page describes how to use Ray's native cgroup v2 based resource isolation to significantly improve the reliability of a Ray Cluster.

.. note::

   This feature is only available in Ray version 2.51.0 and above on Linux.

Background
==========

A Ray cluster consists of Ray Nodes which run two types of processes:

1. System critical processes internal to Ray which are critical to node health
2. Worker processes that are executing user code inside of remote tasks and actors

Without resource isolation, user processes can starve system processes of CPU and memory leading to node failure. Node failure can cause instability in your workload and in extreme cases lead to job failure.

As of v2.51.0, Ray uses `cgroup v2 <https://docs.kernel.org/admin-guide/cgroup-v2.html>`_ to reserve CPU and memory resources for Ray's system processes to protect them from out-of-memory (OOM) errors and CPU starvation.

Requirements
============

Configuring and enabling Resource Isolation can be involved depending on how you are deploying and running Ray. Let's cover some basic requirements which apply to all environments:

- Ray version 2.51.0 and above
- Linux operating system running kernel version 5.8 or above
- Cgroup v1 is disabled.
- Cgroup v2 enabled with read and write permissions. For more information, see :ref:`How to Enable Cgroup v2 <enable-cgroupv2>`.

.. _resource-isolation-containers:

Running Ray in a Container
--------------------------

If you are running Ray in a container (e.g. through Kubernetes), the container must have read and write access to the cgroup mount point. We can't cover all possible ways of running Ray in a container so here are a few examples that cover the most common cases:

Running in Kubernetes with Privileged Security Context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable privileged pods in Kubernetes, you need to `set the securityContext <https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-container>`_ in your podspec to privileged:

.. code-block:: yaml
   :emphasize-lines: 11-12

   apiVersion: v1
   kind: Pod
   metadata:
     name: ubuntu-privileged
   spec:
     containers:
     - name: ubuntu
       image: ubuntu:22.04
       command: ["/bin/bash", "-c", "--"]
       args: ["while true; do sleep 30; done;"]
       securityContext:
         privileged: true
       resources:
         requests:
           cpu: "32"
           memory: "128Gi"

Running in Google Kubernetes Engine (GKE) with Writable Cgroups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Running pods in a privileged security context may not be acceptable for your use case. To avoid this, GKE allows you to use writable cgroups instead. See the `GKE documentation on writable cgroups <https://cloud.google.com/kubernetes-engine/docs/how-to/writable-cgroups>`_.

Running in a Bare Container
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're running in a bare container (e.g. through Docker), `you can use privileged containers <https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities>`_.

.. _resource-isolation-vm:

Running Ray outside of a Container (VM or Baremetal)
----------------------------------------------------

If you're running Ray directly on Linux, the setup is a little more involved. You will need to:

1. Create a cgroup for Ray
2. Configure the cgroup to allow the user that starts Ray to have read and write permissions
3. Move the process that will start Ray into the created cgroup.
4. Start Ray with the cgroup path.

Here's an example script that shows you how to perform these steps. This is to help you run ray on a single node for tests and not the recommended way to run a Ray cluster in production:

.. code-block:: bash

   # Create the cgroup that will be managed by Ray.
   sudo mkdir -p /sys/fs/cgroup/ray

   # Make the current user the owner of the managed cgroup.
   sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/ray

   # Make the cgroup subtree writable.
   sudo chmod -R u+rwx /sys/fs/cgroup/ray

   # Add the current process to the managed cgroup so ray will start
   # inside that cgroup.
   echo $$ | sudo tee /sys/fs/cgroup/ray/cgroup.procs

   # Start ray with resource isolation enabled passing the cgroup path to Ray.
   ray start --enable-resource-isolation --cgroup-path=/sys/fs/cgroup/ray

Usage
=====

Resource isolation can be enabled and configured when starting a Ray cluster using ``ray start`` or when running Ray locally using ``ray.init``.

Enable Resource Isolation on a Ray Cluster
------------------------------------------

.. code-block:: bash

   # Example of enabling resource isolation with default values.
   ray start --enable-resource-isolation

   # Example of enabling resource isolation overriding reserved resources:
   # - /sys/fs/cgroup/ray is used as the base cgroup.
   # - 1.5 CPU cores reserved for system processes.
   # - 5GB memory reserved for system processes.
   ray start --enable-resource-isolation \
       --cgroup-path=/sys/fs/cgroup/ray \
       --system-reserved-cpu=1.5 \
       --system-reserved-memory=5368709120


If you are using the :doc:`Ray Cluster Launcher </cluster/vms/user-guides/launching-clusters/on-premises>`, you must add the resource isolation flags into the 
``head_start_ray_commands`` and ``worker_start_ray_commands``.


Enable Resource Isolation with the SDK
--------------------------------------

.. code-block:: python

   import ray

   # Example of enabling resource isolation overriding reserved resources:
   # - /sys/fs/cgroup/ray is used as the base cgroup.
   # - 1.5 CPU cores reserved for system processes.
   # - 5GB memory reserved for system processes.
   ray.init(
       enable_resource_isolation=True,
       cgroup_path="/sys/fs/cgroup/ray",
       system_reserved_cpu=1.5,
       system_reserved_memory=5368709120,
   )

API Reference
-------------

.. list-table::
   :header-rows: 1
   :widths: 20 10 15 55

   * - Option
     - Type
     - Default
     - Description
   * - ``enable-resource-isolation``
     - boolean
     - ``false``
     - Enables resource isolation.
   * - ``cgroup-path``
     - string
     - ``"/sys/fs/cgroup"``
     - Controls which cgroup Ray uses as its base cgroup. If set without ``enable-resource-isolation``, raises ``ValueError``.
   * - ``system-reserved-cpu``
     - float
     - See :ref:`defaults <resource-isolation-defaults>`
     - CPU cores reserved for system processes. If set without ``enable-resource-isolation``, raises ``ValueError``.
   * - ``system-reserved-memory``
     - integer
     - See :ref:`defaults <resource-isolation-defaults>`
     - Memory bytes reserved for system processes. If set without ``enable-resource-isolation``, raises ``ValueError``. Does not include ``object_store_memory``, but Ray guarantees that system processes have ``system-reserved-memory + object-store-memory`` for the system cgroup.

.. note::

   If any subset of the options is specified, Ray will use default values for the rest. For example, you can specify only ``--system-reserved-memory``.

.. _resource-isolation-defaults:

Default Values for CPU and Memory Reservations
==============================================

If you enable resource isolation but don't specify ``system-reserved-cpu`` or ``system-reserved-memory``, Ray assigns default values. The algorithm uses the following default parameters:

.. code-block:: python

   # CPU
   RAY_DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION = 0.05
   RAY_DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES = 1.0
   RAY_DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES = 3.0

   # Memory
   RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION = 0.10
   RAY_DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES = 0.5 * 1024**3 #500MiB
   RAY_DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES = 10 * 1024**3 #10GiB

You can override these default parameters using environment variables.

Calculation Logic
-----------------

Ray uses the following logic to make sure that default reservations make sense for clusters of all sizes:

1. Calculate value as a proportion of available resources (e.g., ``RAY_DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION * total_cpu_cores``)
2. If the value is less than the minimum, use the minimum
3. If the value is greater than the maximum, use the maximum

Example
-------

For a worker node with 32 CPU cores and 64 GB of RAM:

.. code-block:: bash

   # Calculated default values:
   #   object_store_memory = MIN(0.3 * 64GB, 200GB) = 19.2GB
   #   system_reserved_memory = MIN(10GB, MAX(0.10 * 64GB, 0.5GB)) = 6.4GB
   #   system_reserved_cpu = MIN(3.0, MAX(0.05 * 32, 1.0)) = 1.6
   #
   # The total system_reserved_memory (including object_store_memory) will be 19.2GB + 6.4GB = 25.6GB

   ray start --enable-resource-isolation

.. _enable-cgroupv2:

How to Enable Cgroup v2 for Resource Isolation
==============================================

For Ray Resource Isolation, you need to make sure cgroup v2 is enabled and cgroup v1 is disabled. This is the default behavior on most modern Linux distributions.

The most reliable way to test this is to look at the mount output. It should look like:

.. code-block:: console

   $ mount | grep cgroup
   cgroup2 on /sys/fs/cgroup type cgroup2 (rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot)

.. important::

   If you don't see cgroup v2 or see both cgroup v1 and cgroup v2, you will need to disable cgroup v1 and enable cgroup v2.

If your distribution uses GRUB, add ``systemd.unified_cgroup_hierarchy=1`` to ``GRUB_CMDLINE_LINUX`` under ``/etc/default/grub``, followed by ``sudo update-grub``. However, the recommended approach is to use a distribution that already enables cgroup v2 by default.

Troubleshooting
===============

To see if you've enabled resource isolation correctly, you can look at the ``raylet.out`` log file. If everything works you should see a log line that gives you detailed information about the cgroups that Ray created and the cgroup contraints it enabled.

For example:

.. code-block:: json

   {
     "asctime": "2026-01-14 13:53:13,853",
     "levelname": "I",
     "message": "Initializing CgroupManager at base cgroup at '/sys/fs/cgroup'. Ray's cgroup hierarchy will under the node cgroup at '/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04' with [memory, cpu] controllers enabled. The system cgroup at '/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/system' will have [memory] controllers enabled with [cpu.weight=666, memory.min=25482231398] constraints. The user cgroup '/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/user' will have no controllers enabled with [cpu.weight=9334] constraints. The user cgroup will contain the [/sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/user/workers, /sys/fs/cgroup/ray-node_b9e4de7636296bc3e8a75f5e345eebfc4c423bb4c99706a64196ec04/user/non-ray] cgroups.",
     "component": "raylet",
     "filename": "cgroup_manager.cc",
     "lineno": 212
   }
