.. _ray-slurm-deploy:

Deploying on Slurm
==================

Using Ray with Slurm may not feel intuitive at first.

* SLURM requires multiple copies of the same program are submitted multiple times to the same cluster to do cluster programming. This is particularly well-suited for MPI-based workloads.
* Ray, on the other hand, expects a head-worker architecture with a single point of entry; you need to start a Ray head node, multiple Ray worker nodes, and run your Ray script on the head node.

To bridge this gap, Ray 2.49 and later introduces ``ray symmetric-run`` command, which starts a Ray cluster on all nodes with given CPU and GPU resources and runs your entrypoint script **only** on the head node.

Below is a walkthrough using ``ray symmetric-run`` to run Ray on SLURM.

.. contents::
  :local:


Walkthrough using Ray with SLURM
--------------------------------

Many SLURM deployments require you to interact with slurm through ``sbatch``, which executes a batch script on SLURM.

To run a Ray job with ``sbatch``, you want to start a Ray cluster in the sbatch job with multiple ``srun`` commands (tasks), and then execute your python script that uses Ray. Each task runs on a separate node and starts/connects to a Ray runtime.

The below walkthrough does the following:

1. Set the proper headers for the ``sbatch`` script.
2. Load the proper environment/modules.
3. Fetch a list of available computing nodes and their IP addresses.
4. Launch a head ray process in one of the node (called the head node).
5. Launch Ray processes in (n-1) worker nodes and connects them to the head node by providing the head node address.
6. After the underlying ray cluster is ready, submit the user specified task.

See :ref:`slurm-basic.sh <slurm-basic>` for an end-to-end example.

.. _ray-slurm-headers:

sbatch directives
~~~~~~~~~~~~~~~~~

In your sbatch script, you want to add `directives to provide context <https://slurm.schedmd.com/sbatch.html>`__ for your job to SLURM.

.. code-block:: bash

  #!/bin/bash
  #SBATCH --job-name=my-workload

You need to tell SLURM to allocate nodes specifically for Ray. Ray then finds and manages all resources on each node.

.. code-block:: bash

  ### Modify this according to your Ray workload.
  #SBATCH --nodes=4
  #SBATCH --exclusive

Important: To ensure that each Ray worker runtime runs on a separate node, set ``tasks-per-node``.

.. code-block:: bash

  #SBATCH --tasks-per-node=1

Since this example sets `tasks-per-node = 1`, this guarantees that each Ray worker runtime obtains the
proper resources. In this example, the configuration asks for at least 5 CPUs and 5 GB of memory per node.

.. code-block:: bash

  ### Modify this according to your Ray workload.
  #SBATCH --cpus-per-task=5
  #SBATCH --mem-per-cpu=1GB
  ### Similarly, you can also specify the number of GPUs per node.
  ### Modify this according to your Ray workload. Sometimes this
  ### should be 'gres' instead.
  #SBATCH --gpus-per-task=1


You can also add other optional flags to your sbatch directives.


Loading your environment
~~~~~~~~~~~~~~~~~~~~~~~~

First, you often want to Load modules or your own conda environment at the beginning of the script.

Note that this is an optional step, but it's often required for enabling the right set of dependencies.

.. code-block:: bash

  # Example: module load pytorch/v1.4.0-gpu
  # Example: conda activate my-env

  conda activate my-env

Obtain the head IP address
~~~~~~~~~~~~~~~~~~~~~~~~~~

Next, obtain a hostname and a node IP address for the head node. This way, when you start worker nodes, you can properly connect to the right head node.

.. literalinclude:: /cluster/doc_code/slurm-basic.sh
   :language: bash
   :start-after: __doc_head_address_start__
   :end-before: __doc_head_address_end__

.. note:: In Ray 2.49 and later, you can use IPv6 addresses/hostnames.


Starting Ray and executing your script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: `ray symmetric-run` is available in Ray 2.49 and later. Check older versions of the documentation if you are using an older version of Ray.

Now, use `ray symmetric-run` to start Ray on all nodes with given CPU and GPU resources and run your entrypoint script **only** on the head node.

Below, you see that the example explicitly specifies the number of CPUs (``num-cpus``)
and number of GPUs (``num-gpus``) to Ray, as this prevents Ray from using
more resources than allocated. You also need to explicitly
indicate the ``address`` parameter for the head node to identify itself and other nodes to connect to:

.. literalinclude:: /cluster/doc_code/slurm-basic.sh
   :language: bash
   :start-after: __doc_symmetric_run_start__
   :end-before: __doc_symmetric_run_end__

After the training job is completed, the Ray cluster is stopped automatically.

.. note:: The -u argument tells python to print to stdout unbuffered, which is important with how slurm deals with rerouting output. If this argument isn't included, you may get strange printing behavior such as printed statements not being logged by slurm until the program has terminated.

.. _slurm-network-ray:

SLURM networking caveats
~~~~~~~~~~~~~~~~~~~~~~~~

There are two important networking aspects to keep in mind when working with
SLURM and Ray:

1. Ports binding.
2. IP binding.

One common use of a SLURM cluster is to have multiple users running concurrent
jobs on the same infrastructure. This can easily conflict with Ray due to the
way the head node communicates with its workers.


Considering 2 users, if they both schedule a SLURM job using Ray
at the same time, they're both creating a head node. In the backend, Ray
assigns some internal ports to a few services. The issue is that as soon as the
first head node is created, it binds some ports and prevents them to be
used by another head node. To prevent any conflicts, users have to manually
specify non overlapping ranges of ports. The following ports are to be
adjusted. For an explanation on ports, see :ref:`here <ray-ports>`::

    # used for all ports
    --node-manager-port
    --object-manager-port
    --min-worker-port
    --max-worker-port
    # used for the head node
    --port
    --ray-client-server-port
    --redis-shard-ports

For instance, again with 2 users, they would run the following commands. Note that this example doesn't use symmetric-run here
because it doesn't currently work in multi-tenant environments:

.. code-block:: bash

  # user 1
  ...
  srun --nodes=1 --ntasks=1 -w "$head_node" \
      ray start --head --node-ip-address="$head_node_ip" \
          --port=6379 \
          --node-manager-port=6700 \
          --object-manager-port=6701 \
          --ray-client-server-port=10001 \
          --redis-shard-ports=6702 \
          --min-worker-port=10002 \
          --max-worker-port=19999 \
          --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" --block &

    python -u your_script.py

  # user 2
  ...
  srun --nodes=1 --ntasks=1 -w "$head_node" \
      ray start --head --node-ip-address="$head_node_ip" \
          --port=6380 \
          --node-manager-port=6800 \
          --object-manager-port=6801 \
          --ray-client-server-port=20001 \
          --redis-shard-ports=6802 \
          --min-worker-port=20002 \
          --max-worker-port=29999 \
          --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" --block &

    python -u your_script.py

As for the IP binding, on some cluster architecture the network interfaces
don't allow to use external IPs between nodes. Instead, there are internal
network interfaces (`eth0`, `eth1`, etc.). Currently, it's difficult to
set an internal IP
(see the open `issue <https://github.com/ray-project/ray/issues/22732>`_).


Python-interface SLURM scripts
------------------------------

[Contributed by @pengzhenghao] Below is a helper utility (:ref:`slurm-launch.py <slurm-launch>`) to auto-generate SLURM scripts and launch.
``slurm-launch.py`` uses an underlying template (:ref:`slurm-template.sh <slurm-template>`) and fills out placeholders given user input.

You can feel free to copy both files into your cluster for use. Feel free to also open any PRs for contributions to improve this script.

Usage example
~~~~~~~~~~~~~

If you want to utilize a multi-node cluster in slurm:

.. code-block:: bash

    python slurm-launch.py --exp-name test --command "python your_file.py" --num-nodes 3

If you want to specify the computing nodes, just use the same node names in the same format of the output of ``sinfo`` command:

.. code-block:: bash

    python slurm-launch.py --exp-name test --command "python your_file.py" --num-nodes 3 --node NODE_NAMES


There are other options you can use when calling ``python slurm-launch.py``:

* ``--exp-name``: The experiment name. Will generate ``{exp-name}_{date}-{time}.sh`` and  ``{exp-name}_{date}-{time}.log``.
* ``--command``: The command you wish to run. For example: ``rllib train XXX`` or ``python XXX.py``.
* ``--num-gpus``: The number of GPUs you wish to use in each computing node. Default: 0.
* ``--node`` (``-w``): The specific nodes you wish to use, in the same form as the output of ``sinfo``. Nodes are automatically assigned if not specified.
* ``--num-nodes`` (``-n``): The number of nodes you wish to use. Default: 1.
* ``--partition`` (``-p``): The partition you wish to use. Default: "", uses user's default partition.
* ``--load-env``: The command to setup your environment. For example: ``module load cuda/10.1``. Default: "".

Note that the :ref:`slurm-template.sh <slurm-template>` is compatible with both IPV4 and IPV6 ip address of the computing nodes.

Implementation
~~~~~~~~~~~~~~

Concretely, the (:ref:`slurm-launch.py <slurm-launch>`) does the following things:

1. It automatically writes your requirements, for example, number of CPUs, GPUs per node, the number of nodes and so on, to a sbatch script name ``{exp-name}_{date}-{time}.sh``. Your command (``--command``) to launch your own job is also written into the sbatch script.
2. Then it submits the sbatch script to slurm manager through a new process.
3. Finally, the python process terminates itself and leaves a log file named ``{exp-name}_{date}-{time}.log`` to record the progress of your submitted command. At the mean time, the ray cluster and your job is running in the slurm cluster.


Examples and templates
----------------------

Here are some community-contributed templates for using SLURM with Ray:

- `Ray sbatch submission scripts`_ used at `NERSC <https://www.nersc.gov/>`_, a national lab.
- `YASPI`_ (yet another slurm python interface) by @albanie. The goal of yaspi is to provide an interface to submitting slurm jobs, thereby obviating the joys of sbatch files. It does so through recipes - these are collections of templates and rules for generating sbatch scripts. Supports job submissions for Ray.

- `Convenient python interface`_ to launch ray cluster and submit task by @pengzhenghao

.. _`Ray sbatch submission scripts`: https://github.com/NERSC/slurm-ray-cluster

.. _`YASPI`: https://github.com/albanie/yaspi

.. _`Convenient python interface`: https://github.com/pengzhenghao/use-ray-with-slurm
