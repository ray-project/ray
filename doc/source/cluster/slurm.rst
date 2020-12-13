.. _ray-slurm-deploy:

Deploying on Slurm
==================

Clusters managed by Slurm may require that Ray is initialized as a part of the submitted job. This can be done by using ``srun`` within the submitted script.

Examples and templates
----------------------

Here are some community-contributed templates for using SLURM with Ray:

- `Ray sbatch submission scripts`_ used at `NERSC <https://www.nersc.gov/>`_, a US national lab.
- `YASPI`_ (yet another slurm python interface) by @albanie. The goal of yaspi is to provide an interface to submitting slurm jobs, thereby obviating the joys of sbatch files. It does so through recipes - these are collections of templates and rules for generating sbatch scripts. Supports job submissions for Ray.

- `Convenient python interface`_ to launch ray cluster and submit task by @pengzhenghao

.. _`Ray sbatch submission scripts`: https://github.com/NERSC/slurm-ray-cluster

.. _`YASPI`: https://github.com/albanie/yaspi

.. _`Convenient python interface`: https://github.com/pengzhenghao/use-ray-with-slurm


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


Fully functional SLURM scripts
------------------------------

The starter SBATCH script does the following things:

1. It fetches the list of computing nodes and their IP addresses.
2. It launches a head ray process in one of the node (called the head node), and get the address of the head node.
3. It launches ray processes in (n-1) worker nodes and connects them to the head node by providing the head node address.
4. After the underlying ray cluster is ready, it submits the user specified task.

Extending the starter script above, we can formalize a fully functional SLURM scripts with helpful python interface.
First, we rewrite the starter SBATCH script and add some placeholders so that we can use a python script to fulfill them.
Then, we can write a python interface to take user's specification as argument and replace the placeholders in the SBATCH scripts.
The python script can also be used to launch the Ray job to the ray cluster which is hosted inside the slurm cluster.

The following `launch.py` and `sbatch_template.sh` implement this idea and you can copy them to your own project as helper files.
Concretely, the `launch.py` does the following things:

1. It automatically writes your requirements, e.g. number of CPUs, GPUs per node, the number of nodes and so on, to a sbatch script name `{exp-name}_{date}-{time}.sh`. Your command (`--command`) to launch your own job is also written into the sbatch script.
2. Then it will submit the sbatch script to slurm manager via a new process.
3. Finally, the python process will terminate itself and leaves a log file named `{exp-name}_{date}-{time}.log` to record the progress of your submitted command. At the mean time, the ray cluster and your job is running in the slurm cluster.


If you want to utilize multiple computing node in slurm and let ray recognizes them, please use:

.. code-block:: bash
    python launch.py --exp-name test --command "python your_file.py" --num-nodes 3


If you want to specify the computing nodes, just use the same node name in `sinfo` command:

.. code-block:: bash
    python launch.py --exp-name test --command "python your_file.py" --num-nodes 3 --node chpc-cn[003-005]


There are other options you can use when calling `python launch.py`:

* `--exp-name`: The experiment name. Will generate `{exp-name}_{date}-{time}.sh` and  `{exp-name}_{date}-{time}.log`.
* `--command`: The command you wish to run. For example: `rllib train XXX` or `python XXX.py`.
* `--num-gpus`: The number of GPUs you wish to use in each computing node. Default: 0.
* `--node` (`-w`): The specify nodes you wish to use, in the same form of the return of `sinfo`. Automatically assign if not specify.
* `--num-nodes` (`-n`): The number of nodes you wish to use. Default: 1.
* `--partition` (`-p`): The partition you wish to use. Default: "chpc" (CUHK cluster partition name, change to yours!)
* `--load-env`: The command to setup your environment. For example: `module load cuda/10.1`. Default: "".

Note that the `sbatch_template.sh` is compatible with both IPV4 and IPV6 ip address of the computing nodes.

The python interface `launch.py`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # launch.py
    # Usage: python launch.py --exp-name test --command "rllib train --run PPO --env CartPole-v0"

    import argparse
    import subprocess
    import sys
    import time

    from pathlib import Path

    template_file = Path(__file__) / 'sbatch_template.sh'
    JOB_NAME = "{{JOB_NAME}}"
    NUM_NODES = "{{NUM_NODES}}"
    NUM_GPUS_PER_NODE = "{{NUM_GPUS_PER_NODE}}"
    PARTITION_NAME = "{{PARTITION_NAME}}"
    COMMAND_PLACEHOLDER = "{{COMMAND_PLACEHOLDER}}"
    GIVEN_NODE = "{{GIVEN_NODE}}"
    COMMAND_SUFFIX = "{{COMMAND_SUFFIX}}"
    LOAD_ENV = "{{LOAD_ENV}}"

    if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--exp-name", type=str, required=True,
            help="The job name and path to logging file (exp_name.log)."
        )
        parser.add_argument(
            "--num-nodes", "-n", type=int, default=1,
            help="Number of nodes to use."
        )
        parser.add_argument(
            "--node", "-w", type=str, default="",
            help="The specified nodes to use. Same format as the return of 'sinfo'. Default: ''."
        )
        parser.add_argument(
            "--num-gpus", type=int, default=0,
            help="Number of GPUs to use in each node. (Default: 0)"
        )
        parser.add_argument(
            "--partition", "-p", type=str, default="chpc",
        )
        parser.add_argument(
            "--load-env", type=str, default="",
            help="The script to load your environment, e.g. 'module load cuda/10.1'"
        )
        parser.add_argument(
            "--command", type=str, required=True,
            help="The command you wish to execute. For example: --command 'python "
                 "test.py' Note that the command must be a string."
        )
        args = parser.parse_args()

        if args.node:
            # assert args.num_nodes == 1
            node_info = "#SBATCH -w {}".format(args.node)
        else:
            node_info = ""

        job_name = "{}_{}".format(
            args.exp_name,
            time.strftime("%m%d-%H%M", time.localtime())
        )

        # ===== Modified the template script =====
        with open(template_file, "r") as f:
            text = f.read()
        text = text.replace(JOB_NAME, job_name)
        text = text.replace(NUM_NODES, str(args.num_nodes))
        text = text.replace(NUM_GPUS_PER_NODE, str(args.num_gpus))
        text = text.replace(PARTITION_NAME, str(args.partition))
        text = text.replace(COMMAND_PLACEHOLDER, str(args.command))
        text = text.replace(LOAD_ENV, str(args.load_env))
        text = text.replace(GIVEN_NODE, node_info)
        text = text.replace(COMMAND_SUFFIX, "")
        text = text.replace(
            "# THIS FILE IS A TEMPLATE AND IT SHOULD NOT BE DEPLOYED TO "
            "PRODUCTION!",
            "# THIS FILE IS MODIFIED AUTOMATICALLY FROM TEMPLATE AND SHOULD BE "
            "RUNNABLE!"
        )

        # ===== Save the script =====
        script_file = "{}.sh".format(job_name)
        with open(script_file, "w") as f:
            f.write(text)

        # ===== Submit the job =====
        print("Start to submit job!")
        subprocess.Popen(["sbatch", script_file])
        print(
            "Job submitted! Script file is at: <{}>. Log file is at: <{}>".format(
                script_file, "{}.log".format(job_name))
        )
        sys.exit(0)



The advanced SBATCH template `sbatch_template.sh`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    #!/bin/bash

    # THIS FILE IS GENERATED BY AUTOMATION SCRIPT! PLEASE REFER TO ORIGINAL SCRIPT!
    # THIS FILE IS A TEMPLATE AND IT SHOULD NOT BE DEPLOYED TO PRODUCTION!

    #SBATCH --partition={{PARTITION_NAME}}
    #SBATCH --job-name={{JOB_NAME}}
    #SBATCH --output={{JOB_NAME}}.log
    {{GIVEN_NODE}}

    ### This script works for any number of nodes, Ray will find and manage all resources
    #SBATCH --nodes={{NUM_NODES}}
    #SBATCH --exclusive

    ### Give all resources to a single Ray task, ray can manage the resources internally
    #SBATCH --ntasks-per-node=1
    #SBATCH --gpus-per-task={{NUM_GPUS_PER_NODE}}

    # Load modules or your own conda environment here
    # module load pytorch/v1.4.0-gpu
    # conda activate {{CONDA_ENV}}
    {{LOAD_ENV}}

    # ===== DON NOT CHANGE THINGS HERE UNLESS YOU KNOW WHAT YOU ARE DOING =====
    # This script is a modification to the implementation suggest by gregSchwartz18 here:
    # https://github.com/ray-project/ray/issues/826#issuecomment-522116599
    redis_password=$(uuidgen)
    export redis_password

    nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
    nodes_array=($nodes)

    node_1=${nodes_array[0]}
    ip=$(srun --nodes=1 --ntasks=1 -w $node_1 hostname --ip-address) # making redis-address

    if [[ $ip == *" "* ]]; then
      IFS=' ' read -ra ADDR <<<"$ip"
      if [[ ${#ADDR[0]} > 16 ]]; then
        ip=${ADDR[1]}
      else
        ip=${ADDR[0]}
      fi
      echo "We detect space in ip! You are using IPV6 address. We split the IPV4 address as $ip"
    fi

    port=6379
    ip_head=$ip:$port
    export ip_head
    echo "IP Head: $ip_head"

    echo "STARTING HEAD at $node_1"
    # srun --nodes=1 --ntasks=1 -w $node_1 start-head.sh $ip $redis_password &
    srun --nodes=1 --ntasks=1 -w $node_1 \
      ray start --head --node-ip-address=$ip --port=6379 --redis-password=$redis_password --block &
    sleep 30

    worker_num=$(($SLURM_JOB_NUM_NODES - 1)) #number of nodes other than the head node
    for ((i = 1; i <= $worker_num; i++)); do
      node_i=${nodes_array[$i]}
      echo "STARTING WORKER $i at $node_i"
      srun --nodes=1 --ntasks=1 -w $node_i ray start --address $ip_head --redis-password=$redis_password --block &
      sleep 5
    done

    # ===== Call your code below =====
    {{COMMAND_PLACEHOLDER}} {{COMMAND_SUFFIX}}
