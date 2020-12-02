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
