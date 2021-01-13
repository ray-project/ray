.. _ray-slurm-deploy:

Deploying on Slurm
==================

Slurm usage with Ray can be a little bit unintuitive.

* SLURM requires multiple copies of the same program are submitted multiple times to the same cluster to do cluster programming. This is particularly well-suited for MPI-based workloads.
* Ray, on the other hand, expects a head-worker architecture with a single point of entry. That is, you'll need to start a Ray head node, multiple Ray worker nodes, and run your Ray script on the head node.

This document aims to clarify how to run Ray on SLURM.

.. contents::
  :local:


Walkthrough using Ray with SLURM
--------------------------------

Many SLURM deployments require you to interact with slurm via ``sbatch``, which executes a batch script on SLURM.

To run a Ray job with ``sbatch``, you will want to start a Ray cluster in the sbatch job with multiple ``srun`` commands (tasks), and then execute your python script that uses Ray. Each task will run on a separate node and start/connect to a Ray runtime.

The below walkthrough will do the following:

1. Set the proper headers for the ``sbatch`` script.
2. Load the proper environment/modules.
3. Fetch a list of available computing nodes and their IP addresses.
4. Launch a head ray process in one of the node (called the head node).
5. Launch Ray processes in (n-1) worker nodes and connects them to the head node by providing the head node address.
6. After the underlying ray cluster is ready, submit the user specified task.

See :ref:`slurm-basic.sh <slurm-basic>` for an end-to-end example.

.. _ray-slurm-headers:

sbatch headers
~~~~~~~~~~~~~~

In your sbatch script, you'll want to add headers to provide context for your job to SLURM.

.. code-block:: bash

  #!/bin/bash
  #SBATCH --job-name=my-workload

You'll need to tell SLURM to allocate nodes specifically for Ray. Ray will then find and manage all resources on each node.

.. code-block:: bash

  ### Modify this according to your Ray workload.
  #SBATCH --nodes=4
  #SBATCH --exclusive

Important: To ensure that each Ray worker runtime will run on a separate node, set ``tasks-per-node``.

.. code-block:: bash

  #SBATCH --tasks-per-node=1

Since we've set `tasks-per-node = 1`, this will be used to guarantee that each Ray worker runtime will obtain the
proper resources. In this example, we ask for at least 5 CPUs and 5 GB of memory per node.

.. code-block:: bash

  ### Modify this according to your Ray workload.
  #SBATCH --cpus-per-task=5
  #SBATCH --mem-per-cpu=1GB

  ### Similarly, you can also specify the number of GPUs per node.
  ### Modify this according to your Ray workload.
  #SBATCH --gpus-per-task=1


You can also add other optional flags to your sbatch headers.


Loading your environment
~~~~~~~~~~~~~~~~~~~~~~~~

First, you'll often want to Load modules or your own conda environment at the beginning of the script.

Note that this is an optional step, but it is often required for enabling the right set of dependencies.

.. code-block:: bash

  # Example: module load pytorch/v1.4.0-gpu
  # Example: conda activate my-env

  conda activate my-env

Obtain the head IP address
~~~~~~~~~~~~~~~~~~~~~~~~~~

Next, we'll want to obtain a hostname and a node IP address for the head node. This way, when we start worker nodes, we'll be able to properly connect to the right head node.

.. code-block:: bash

  # Getting the node names
  nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST)
  nodes_array=($nodes)

  head_node=${nodes_array[0]}
  head_node_ip=$(srun --nodes=1 --ntasks=1 -w $head_node hostname --ip-address)

  # if we detect a space character in the head node IP, we'll
  # convert it to an ipv4 address. This step is optional.
  if [[ $head_node_ip == *" "* ]]; then
    IFS=' ' read -ra ADDR <<<"$head_node_ip"
    if [[ ${#ADDR[0]} -gt 16 ]]; then
      head_node_ip=${ADDR[1]}
    else
      head_node_ip=${ADDR[0]}
    fi
    echo "IPV6 address detected. We split the IPV4 address as $head_node_ip"
  fi


Starting the Ray head node
~~~~~~~~~~~~~~~~~~~~~~~~~~

After detecting the head node hostname and head node IP, we'll want to create
a Ray head node runtime. We'll do this by using ``srun`` as a background task
as a single task/node (recall that ``tasks-per-node=1``).

Below, you'll see that we explicitly specify the number of CPUs (``num-cpus``)
and number of GPUs (``num-gpus``) to Ray, as this will prevent Ray from using
more resources than allocated. We also need to explictly
indicate the ``node-ip-address`` for the Ray head runtime:

.. code-block:: bash

  port=6379
  ip_head=$head_node_ip:$port
  export ip_head
  echo "IP Head: $ip_head"

  echo "Starting HEAD at $head_node"
  srun --nodes=1 --ntasks=1 -w $head_node \
    ray start --head --node-ip-address=$head_node_ip --port=$port \
    --num-cpus ${SLURM_CPUS_PER_TASK} --num-gpus ${SLURM_GPUS_PER_TASK} --block &

By backgrounding the above srun task, we can proceed to start the Ray worker runtimes.

Starting the Ray worker nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below, we do the same thing, but for each worker. Make sure the Ray head and Ray worker processes are not started on the same node.

.. code-block:: bash

  # optional, though may be useful in certain versions of Ray < 1.0.
  sleep 10

  # number of nodes other than the head node
  worker_num=$(($SLURM_JOB_NUM_NODES - 1))

  for ((i = 1; i <= $worker_num; i++)); do
    node_i=${nodes_array[$i]}
    echo "Starting WORKER $i at $node_i"
    srun --nodes=1 --ntasks=1 -w $node_i \
      ray start --address $ip_head --block \
      --num-cpus ${SLURM_CPUS_PER_TASK} --num-gpus ${SLURM_GPUS_PER_TASK} &
    sleep 5
  done

Submitting your script
~~~~~~~~~~~~~~~~~~~~~~

Finally, you can invoke your Python script:

.. code-block:: bash

  python -u your-script.py


Python-interface SLURM scripts
------------------------------

[Contributed by @pengzhenghao] Below, we provide a helper utility (:ref:`slurm-launch.py <slurm-launch>`) to auto-generate SLURM scripts and launch.
``slurm-launch.py`` uses an underlying template (:ref:`slurm-template.txt <slurm-template>`) and fills out placeholders given user input.

You can feel free to copy both files into your cluster for use. Feel free to also open any PRs for contributions to improve this script!

Usage example
~~~~~~~~~~~~~

If you want to utilize a multi-node cluster in slurm:

.. code-block:: bash

    python slurm-launch.py --exp-name test --command "python your_file.py" --num-nodes 3

If you want to specify the computing nodes, just use the same node name in ``sinfo`` command:

.. code-block:: bash

    python slurm-launch.py --exp-name test --command "python your_file.py" --num-nodes 3 --node chpc-cn[003-005]


There are other options you can use when calling ``python slurm-launch.py``:

* ``--exp-name``: The experiment name. Will generate ``{exp-name}_{date}-{time}.sh`` and  ``{exp-name}_{date}-{time}.log``.
* ``--command``: The command you wish to run. For example: ``rllib train XXX`` or ``python XXX.py``.
* ``--num-gpus``: The number of GPUs you wish to use in each computing node. Default: 0.
* ``--node`` (``-w``): The specific nodes you wish to use, in the same form as the output of ``sinfo``. Nodes are automatically assigned if not specified.
* ``--num-nodes`` (``-n``): The number of nodes you wish to use. Default: 1.
* ``--partition`` (``-p``): The partition you wish to use. Default: "", will use user's default partition.
* ``--load-env``: The command to setup your environment. For example: ``module load cuda/10.1``. Default: "".

Note that the :ref:`slurm-template.txt <slurm-template>` is compatible with both IPV4 and IPV6 ip address of the computing nodes.

Implementation
~~~~~~~~~~~~~~

Concretely, the (:ref:`slurm-launch.py <slurm-launch>`) does the following things:

1. It automatically writes your requirements, e.g. number of CPUs, GPUs per node, the number of nodes and so on, to a sbatch script name ``{exp-name}_{date}-{time}.sh``. Your command (``--command``) to launch your own job is also written into the sbatch script.
2. Then it will submit the sbatch script to slurm manager via a new process.
3. Finally, the python process will terminate itself and leaves a log file named ``{exp-name}_{date}-{time}.log`` to record the progress of your submitted command. At the mean time, the ray cluster and your job is running in the slurm cluster.


Examples and templates
----------------------

Here are some community-contributed templates for using SLURM with Ray:

- `Ray sbatch submission scripts`_ used at `NERSC <https://www.nersc.gov/>`_, a US national lab.
- `YASPI`_ (yet another slurm python interface) by @albanie. The goal of yaspi is to provide an interface to submitting slurm jobs, thereby obviating the joys of sbatch files. It does so through recipes - these are collections of templates and rules for generating sbatch scripts. Supports job submissions for Ray.

- `Convenient python interface`_ to launch ray cluster and submit task by @pengzhenghao

.. _`Ray sbatch submission scripts`: https://github.com/NERSC/slurm-ray-cluster

.. _`YASPI`: https://github.com/albanie/yaspi

.. _`Convenient python interface`: https://github.com/pengzhenghao/use-ray-with-slurm



