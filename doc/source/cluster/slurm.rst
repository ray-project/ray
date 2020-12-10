.. _ray-slurm-deploy:

Deploying on Slurm
==================

Clusters managed by Slurm may require that Ray is initialized as a part of the submitted job. This can be done by using ``srun`` within the submitted script.

Examples and templates
----------------------

Here are some community-contributed templates for using SLURM with Ray:

- `Ray sbatch submission scripts`_ used at `NERSC <https://www.nersc.gov/>`_, a US national lab.
- `YASPI`_ (yet another slurm python interface) by @albanie. The goal of yaspi is to provide an interface to submitting slurm jobs, thereby obviating the joys of sbatch files. It does so through recipes - these are collections of templates and rules for generating sbatch scripts. Supports job submissions for Ray.

- `Template script`_ by @pengzhenghao

.. _`Ray sbatch submission scripts`: https://github.com/NERSC/slurm-ray-cluster

.. _`YASPI`: https://github.com/albanie/yaspi

.. _`Template script`: https://gist.github.com/pengzhenghao/b348db1075101a9b986c4cdfea13dcd6


Starter SLURM script
--------------------

.. code-block:: bash

  #!/bin/bash
  #SBATCH --job-name=test
  #SBATCH --cpus-per-task=5
  #SBATCH --mem-per-cpu=1GB
  #SBATCH --nodes=4
  #SBATCH --tasks-per-node=1
  #SBATCH --time=00:30:00
  #SBATCH --reservation=test

  let "worker_num=(${SLURM_NTASKS} - 1)"

  # Define the total number of CPU cores available to ray
  let "total_cores=${worker_num} * ${SLURM_CPUS_PER_TASK}"

  suffix='6379'
  ip_head=`hostname`:$suffix
  export ip_head # Exporting for latter access by trainer.py

  # Start the ray head node on the node that executes this script by specifying --nodes=1 and --nodelist=`hostname`
  # We are using 1 task on this node and 5 CPUs (Threads). Have the dashboard listen to 0.0.0.0 to bind it to all
  # network interfaces. This allows to access the dashboard through port-forwarding:
  # Let's say the hostname=cluster-node-500 To view the dashboard on localhost:8265, set up an ssh-tunnel like this: (assuming the firewall allows it)
  # $  ssh -N -f -L 8265:cluster-node-500:8265 user@big-cluster
  srun --nodes=1 --ntasks=1 --cpus-per-task=${SLURM_CPUS_PER_TASK} --nodelist=`hostname` ray start --head --block --dashboard-host 0.0.0.0 --port=6379 --num-cpus ${SLURM_CPUS_PER_TASK} &
  sleep 5
  # Make sure the head successfully starts before any worker does, otherwise
  # the worker will not be able to connect to redis. In case of longer delay,
  # adjust the sleeptime above to ensure proper order.

  # Now we execute worker_num worker nodes on all nodes in the allocation except hostname by
  # specifying --nodes=${worker_num} and --exclude=`hostname`. Use 1 task per node, so worker_num tasks in total
  # (--ntasks=${worker_num}) and 5 CPUs per task (--cps-per-task=${SLURM_CPUS_PER_TASK}).
  srun --nodes=${worker_num} --ntasks=${worker_num} --cpus-per-task=${SLURM_CPUS_PER_TASK} --exclude=`hostname` ray start --address $ip_head --block --num-cpus ${SLURM_CPUS_PER_TASK} &
  sleep 5

  python -u trainer.py ${total_cores} # Pass the total number of allocated CPUs

.. code-block:: python

  # trainer.py
  from collections import Counter
  import os
  import sys
  import time
  import ray

  num_cpus = int(sys.argv[1])

  ray.init(address=os.environ["ip_head"])

  print("Nodes in the Ray cluster:")
  print(ray.nodes())

  @ray.remote
  def f():
      time.sleep(1)
      return ray.services.get_node_ip_address()

  # The following takes one second (assuming that ray was able to access all of the allocated nodes).
  for i in range(60):
      start = time.time()
      ip_addresses = ray.get([f.remote() for _ in range(num_cpus)])
      print(Counter(ip_addresses))
      end = time.time()
      print(end - start)


Fully functional SLURM sctips
------------------------------

Extending the starter script above, we can formalize a fully functional SLURM scripts with helpful python interface.
In the following content, we will introduce a python script that can generate a functional SBATCH script and launch the
script automatically.

The sbatch script does the following things:

1. It fetches the list of computing nodes and their IP addresses.
2. It launches a head ray process in one of the node, and get the address of the head node.
3. It launches ray processes in (n-1) worker nodes and connects them to the head node by providing the head node address.
4. It submit the user specified task to ray.



.. code-block:: python

    # Suppose this file is called: launch.py
    # Usage: python launch.py --exp-name test --command "rllib train --run PPO --env CartPole-v0"
    import argparse
    import os.path as osp
    import subprocess
    import sys
    import time

    template_file = osp.join(osp.dirname(__file__), "sbatch_template.sh")
    JOB_NAME = "{{JOB_NAME}}"
    NUM_NODES = "{{NUM_NODES}}"
    NUM_CPUS_PER_NODE = "{{NUM_CPUS_PER_NODE}}"
    NUM_GPUS_PER_NODE = "{{NUM_GPUS_PER_NODE}}"
    PARTITION_NAME = "{{PARTITION_NAME}}"
    COMMAND_PLACEHOLDER = "{{COMMAND_PLACEHOLDER}}"
    GIVEN_NODE = "{{GIVEN_NODE}}"
    COMMAND_SUFFIX = "{{COMMAND_SUFFIX}}"
    LOAD_ENV = "{{LOAD_ENV}}"

    if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--exp-name", type=str, required=True, help="The job name and path to logging file (exp_name.log)."
        )
        parser.add_argument("--num-nodes", "-n", type=int, default=1, help="Number of nodes to use.")
        parser.add_argument("--node", "-w", type=str, default="", help="A specify node to use")
        parser.add_argument("--num-cpus", type=int, default=64,
                            help="Deprecated. Number of CPUs to use in each node. "
                                 "(Default: 64) Slurm will ignore this setting.")
        parser.add_argument("--num-gpus", type=int, default=0, help="Number of GPUs to use in each node. (Default: 8)")
        parser.add_argument(
            "--partition",
            "-p",
            type=str,
            default="chpc",
        )
        parser.add_argument("--load-env", type=str, default="",
                            help="The script to load your environment, e.g. 'module load cuda/10.1'")
        parser.add_argument(
            "--command",
            type=str,
            required=True,
            help="The command you wish to execute. For example: --command 'python "
                 "test.py' Note that the command must be a string."
        )
        args = parser.parse_args()

        if args.node:
            # assert args.num_nodes == 1
            node_info = "#SBATCH -w {}".format(args.node)
        else:
            node_info = ""

        job_name = "{}_{}".format(args.exp_name, time.strftime("%m%d-%H%M", time.localtime()))

        # ===== Modified the template script =====
        with open(template_file, "r") as f:
            text = f.read()
        text = text.replace(JOB_NAME, job_name)
        text = text.replace(NUM_NODES, str(args.num_nodes))
        text = text.replace(NUM_CPUS_PER_NODE, str(args.num_cpus))
        text = text.replace(NUM_GPUS_PER_NODE, str(args.num_gpus))
        text = text.replace(PARTITION_NAME, str(args.partition))
        text = text.replace(COMMAND_PLACEHOLDER, str(args.command))
        text = text.replace(LOAD_ENV, str(args.load_env))
        text = text.replace(GIVEN_NODE, node_info)
        text = text.replace(COMMAND_SUFFIX, "")
        text = text.replace(
            "# THIS FILE IS A TEMPLATE AND IT SHOULD NOT BE DEPLOYED TO "
            "PRODUCTION!", "# THIS FILE IS MODIFIED AUTOMATICALLY FROM TEMPLATE AND SHOULD BE "
                           "RUNNABLE!"
        )

        # ===== Save the script =====
        script_file = "{}.sh".format(job_name)
        with open(script_file, "w") as f:
            f.write(text)

        # ===== Submit the job =====
        print("Start to submit job!")
        subprocess.Popen(["sbatch", script_file])
        print("Job submitted! Script file is at: <{}>. Log file is at: <{}>".format(script_file, "{}.log".format(job_name)))
        sys.exit(0)

