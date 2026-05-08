.. _ray-slurm-deploy:

Deploying on Slurm
==================

Slurm usage with Ray can be a little bit unintuitive.

* SLURM requires multiple copies of the same program are submitted multiple times to the same cluster to do cluster programming. This is particularly well-suited for MPI-based workloads.
* Ray, on the other hand, expects a head-worker architecture with a single point of entry. That is, you'll need to start a Ray head node, multiple Ray worker nodes, and run your Ray script on the head node.

To bridge this gap, Ray 2.49 and above introduces ``ray symmetric-run`` command, which will start a Ray cluster on all nodes with given CPU and GPU resources and run your entrypoint script ONLY the head node.

Below, we provide a walkthrough using ``ray symmetric-run`` to run Ray on SLURM.

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

sbatch directives
~~~~~~~~~~~~~~~~~

In your sbatch script, you'll want to add `directives to provide context <https://slurm.schedmd.com/sbatch.html>`__ for your job to SLURM.

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
  ### Modify this according to your Ray workload. Sometimes this
  ### should be 'gres' instead.
  #SBATCH --gpus-per-task=1


You can also add other optional flags to your sbatch directives.


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

.. literalinclude:: /cluster/doc_code/slurm-basic.sh
   :language: bash
   :start-after: __doc_head_address_start__
   :end-before: __doc_head_address_end__

.. note:: In Ray 2.49 and above, you can use IPv6 addresses/hostnames.


Starting Ray and executing your script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: `ray symmetric-run` is available in Ray 2.49 and above. Check older versions of the documentation if you are using an older version of Ray.

Now, we'll use `ray symmetric-run` to start Ray on all nodes with given CPU and GPU resources and run your entrypoint script ONLY the head node.

Below, you'll see that we explicitly specify the number of CPUs (``num-cpus``)
and number of GPUs (``num-gpus``) to Ray, as this will prevent Ray from using
more resources than allocated. We also need to explicitly
indicate the ``address`` parameter for the head node to identify itself and other nodes to connect to:

.. literalinclude:: /cluster/doc_code/slurm-basic.sh
   :language: bash
   :start-after: __doc_symmetric_run_start__
   :end-before: __doc_symmetric_run_end__

After the training job is completed, the Ray cluster will be stopped automatically.

.. note:: The -u argument tells python to print to stdout unbuffered, which is important with how slurm deals with rerouting output. If this argument is not included, you may get strange printing behavior such as printed statements not being logged by slurm until the program has terminated.

.. _ray-slurm-docker-init:

Running inside Docker containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your SLURM compute nodes run the job inside a Docker container, make
sure PID 1 inside the container is a real init process — ``tini``,
``dumb-init``, or whatever Docker injects when you pass ``--init``
(currently ``tini``). Otherwise the Ray processes that ``ray stop``
terminates remain as zombies in the kernel's process table, and
``ray stop`` reports ``Stopped only 0 out of N`` with every remaining
process showing ``status='zombie'``.

Symptom
^^^^^^^

When the container's PID 1 is not a reaper, ``ray symmetric-run`` (or a
manual ``ray stop``) prints a warning like this at teardown:

.. code-block:: text

   WARN scripts.py:1392 -- Stopped only 0 out of 6 Ray processes within the grace period 16 seconds. Set `-v` to see more details. Remaining processes [psutil.Process(pid=2226, name='raylet', status='zombie'), psutil.Process(pid=2225, name='python3.12', status='zombie'), psutil.Process(pid=1761, name='python3.12', status='zombie'), psutil.Process(pid=1759, name='python3.12', status='zombie'), psutil.Process(pid=1760, name='python3.12', status='zombie'), psutil.Process(pid=1709, name='gcs_server', status='zombie')] will be forcefully terminated.
   WARN scripts.py:1399 -- You can also use `--force` to forcefully terminate processes or set higher `--grace-period` to wait longer time for proper termination.

What this actually means:

* The Ray processes already exited in response to the ``SIGTERM`` that
  ``ray stop`` sent — they are zombies, not still running. Per Linux
  `wait(2) <https://man7.org/linux/man-pages/man2/wait.2.html>`__,
  *"a child that terminates, but has not been waited for, becomes a
  zombie"* and *"as long as a zombie is not removed from the system via a
  wait, it will consume a slot in the kernel process table"*. Because the
  container's PID 1 (e.g. ``slurmd``) never calls ``waitpid(2)``, the
  zombies stay in that table.
* ``ray stop`` waits for termination via
  `psutil.wait_procs <https://github.com/giampaolo/psutil/blob/master/psutil/__init__.py>`__,
  which on POSIX uses
  `psutil._psposix.wait_pid_posix
  <https://github.com/giampaolo/psutil/blob/master/psutil/_psposix.py>`__.
  For PIDs that aren't children of the caller it falls back to polling
  whether ``/proc/<pid>`` still exists. ``/proc/<pid>`` exists for a
  zombie, so the wait times out and the zombies land in the ``alive``
  list. ``ray stop`` then counts them as "not stopped".
* The "forcefully terminated" line in the warning refers to ``ray stop``
  sending ``SIGKILL`` after the grace period — see the ``proc.kill()``
  loop in
  `python/ray/scripts/scripts.py <https://github.com/ray-project/ray/blob/master/python/ray/scripts/scripts.py>`__
  (``ray stop`` implementation). This does not change a zombie's state:
  a zombie has no executing context, and only ``waitpid`` by its parent
  removes it from the process table (``wait(2)``).

Why this matters
^^^^^^^^^^^^^^^^

When ``ray symmetric-run`` finishes, it calls ``ray stop``, which sends
``SIGTERM`` to each Ray process and then waits for them to exit. After a
process exits, the kernel marks it as a zombie and delivers ``SIGCHLD``
to its current parent (see
`signal(7) <https://man7.org/linux/man-pages/man7/signal.7.html>`__:
*"Child stopped, terminated, or continued"*). The parent must then call
``waitpid`` (`wait(2) <https://man7.org/linux/man-pages/man2/wait.2.html>`__)
to reap the zombie.

* On a typical modern Linux distribution, PID 1 is ``systemd``, which
  reaps orphaned children. Zombies disappear immediately and
  ``psutil.wait_procs`` reports them as ``gone``.
* Inside a containerized SLURM compute node where PID 1 is ``slurmd``,
  ``slurmd`` registers handlers for ``SIGINT``, ``SIGTERM``, ``SIGQUIT``,
  ``SIGHUP``, ``SIGUSR2``, ``SIGPIPE``, and ``SIGPROF`` — but not
  ``SIGCHLD`` — and so does not reap re-parented orphan processes. (The
  official `slurmd(8) <https://slurm.schedmd.com/slurmd.html>`__ SIGNALS
  section likewise omits ``SIGCHLD``; ``slurmd.c`` source references are
  linked in the discussion comment cited under "References" below.) The
  dead Ray processes stay in the process table with ``status='zombie'``,
  ``psutil.wait_procs`` returns them in the ``alive`` list, and
  ``ray stop`` reports ``Stopped only 0 out of N``.

This is a deployment-layer issue (a container without a real init), not
a Ray bug and not a SLURM bug.

How to fix it
^^^^^^^^^^^^^

Give the container a real init that reaps zombies (calls ``waitpid`` on
exit). Pick the option that matches how you launch the container:

* **Plain Docker** — pass ``--init`` to ``docker run``. Docker injects
  ``tini`` as PID 1 for you. See
  `docker run --init <https://docs.docker.com/reference/cli/docker/container/run/#init>`__.
* **Docker Compose** — set ``init: true`` on the service in your
  ``docker-compose.yaml``. Same effect as ``docker run --init``. See
  `Compose: init <https://docs.docker.com/reference/compose-file/services/#init>`__.
* **Bake it into the image** — install
  `tini <https://github.com/krallin/tini>`__ (or
  `dumb-init <https://github.com/Yelp/dumb-init>`__) in the
  ``Dockerfile`` and use it as the ``ENTRYPOINT``. ``tini`` exists, in
  its own words, *"to protect you from software that accidentally
  creates zombie processes, which can (over time!) starve your entire
  system for PIDs"* — by reaping them. ``dumb-init`` does the same and
  additionally addresses Linux's special signal-handling rules for
  PID 1. This is the path Docker recommends for containers running
  multiple processes: see
  `Run multiple services in a container <https://docs.docker.com/engine/containers/multi-service_container/>`__.

Example — a ``docker-compose.yaml`` snippet for a containerized SLURM
compute node. The line that fixes the zombie problem is ``init: true``:

.. code-block:: yaml
   :caption: docker-compose.yaml

   services:
     c1:
       image: slurm-docker-cluster:25.11.2
       init: true            # PID 1 becomes tini, which reaps zombies
       command: ["slurmd"]

After this change, the Ray processes are properly reaped on teardown,
so ``psutil.wait_procs`` no longer classifies them as alive and
``ray stop`` prints its success message instead of the warning above:

.. code-block:: text

   SUCCESS scripts.py:1488 -- Stopped all 6 Ray processes.

References
^^^^^^^^^^

Linux process and signal semantics:

* `wait(2) <https://man7.org/linux/man-pages/man2/wait.2.html>`__ —
  zombie definition, ``waitpid``, and PID 1 reaping orphans.
* `signal(7) <https://man7.org/linux/man-pages/man7/signal.7.html>`__ —
  ``SIGCHLD`` semantics.

Container init runtimes:

* `tini <https://github.com/krallin/tini>`__ — the minimal init that
  Docker bundles for ``--init``.
* `dumb-init <https://github.com/Yelp/dumb-init>`__ — alternative
  minimal init, same purpose.
* `Docker: Run multiple services in a container
  <https://docs.docker.com/engine/containers/multi-service_container/>`__
  — Docker's guidance on running an init in the container.

Tooling used by ``ray stop``:

* `psutil.wait_procs source
  <https://github.com/giampaolo/psutil/blob/master/psutil/__init__.py>`__
  — the function ``ray stop`` uses to wait for termination.
* `python/ray/scripts/scripts.py
  <https://github.com/ray-project/ray/blob/master/python/ray/scripts/scripts.py>`__
  — ``ray stop`` implementation, including the warning text and the
  post-grace-period ``SIGKILL`` escalation.

Discussion specific to this issue:

* `ray-project/ray#62591 (root-cause analysis)
  <https://github.com/ray-project/ray/pull/62591#issuecomment-4396615458>`__
  — comparison of ``systemd`` vs. ``slurmd`` PID 1 behavior, with linked
  ``slurmd.c`` source references.
* `ray-project/ray#62591 (init fix confirmation)
  <https://github.com/ray-project/ray/pull/62591#issuecomment-4403602546>`__
  — confirms ``init: true`` resolves the issue.

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
at the same time, they are both creating a head node. In the backend, Ray will
assign some internal ports to a few services. The issue is that as soon as the
first head node is created, it will bind some ports and prevent them to be
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

For instance, again with 2 users, they would run the following commands. Note that we don't use symmetric-run here
because it does not currently work in multi-tenant environments:

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
do not allow to use external IPs between nodes. Instead, there are internal
network interfaces (`eth0`, `eth1`, etc.). Currently, it's difficult to
set an internal IP
(see the open `issue <https://github.com/ray-project/ray/issues/22732>`_).


Python-interface SLURM scripts
------------------------------

[Contributed by @pengzhenghao] Below, we provide a helper utility (:ref:`slurm-launch.py <slurm-launch>`) to auto-generate SLURM scripts and launch.
``slurm-launch.py`` uses an underlying template (:ref:`slurm-template.sh <slurm-template>`) and fills out placeholders given user input.

You can feel free to copy both files into your cluster for use. Feel free to also open any PRs for contributions to improve this script!

Usage example
~~~~~~~~~~~~~

If you want to utilize a multi-node cluster in slurm:

.. code-block:: bash

    python slurm-launch.py --exp-name test --command "python your_file.py" --num-nodes 3

If you want to specify the computing node(s), just use the same node name(s) in the same format of the output of ``sinfo`` command:

.. code-block:: bash

    python slurm-launch.py --exp-name test --command "python your_file.py" --num-nodes 3 --node NODE_NAMES


There are other options you can use when calling ``python slurm-launch.py``:

* ``--exp-name``: The experiment name. Will generate ``{exp-name}_{date}-{time}.sh`` and  ``{exp-name}_{date}-{time}.log``.
* ``--command``: The command you wish to run. For example: ``rllib train XXX`` or ``python XXX.py``.
* ``--num-gpus``: The number of GPUs you wish to use in each computing node. Default: 0.
* ``--node`` (``-w``): The specific nodes you wish to use, in the same form as the output of ``sinfo``. Nodes are automatically assigned if not specified.
* ``--num-nodes`` (``-n``): The number of nodes you wish to use. Default: 1.
* ``--partition`` (``-p``): The partition you wish to use. Default: "", will use user's default partition.
* ``--load-env``: The command to setup your environment. For example: ``module load cuda/10.1``. Default: "".

Note that the :ref:`slurm-template.sh <slurm-template>` is compatible with both IPV4 and IPV6 ip address of the computing nodes.

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
