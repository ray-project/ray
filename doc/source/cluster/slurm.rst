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
  #SBATCH --nodes=3
  #SBATCH --tasks-per-node 1

  worker_num=2 # Must be one less that the total number of nodes

  # module load Langs/Python/3.6.4 # This will vary depending on your environment
  # source venv/bin/activate

  nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
  nodes_array=( $nodes )

  node1=${nodes_array[0]}

  ip_prefix=$(srun --nodes=1 --ntasks=1 -w $node1 hostname --ip-address) # Making address
  suffix=':6379'
  ip_head=$ip_prefix$suffix
  redis_password=$(uuidgen)

  export ip_head # Exporting for latter access by trainer.py

  srun --nodes=1 --ntasks=1 -w $node1 ray start --block --head --redis-port=6379 --redis-password=$redis_password & # Starting the head
  sleep 5
  # Make sure the head successfully starts before any worker does, otherwise
  # the worker will not be able to connect to redis. In case of longer delay,
  # adjust the sleeptime above to ensure proper order.

  # Now we execute worker_num worker nodes on all nodes in the allocation except hostname by
  # specifying --nodes=${worker_num} and --exclude=`hostname`. Use 1 task per node, so worker_num tasks in total
  # (--ntasks=${worker_num}) and 5 CPUs per task (--cps-per-task=${SLURM_CPUS_PER_TASK}).
  srun --nodes=${worker_num} --ntasks=${worker_num} --cpus-per-task=${SLURM_CPUS_PER_TASK} --exclude=`hostname` ray start --address $ip_head --block --num-cpus ${SLURM_CPUS_PER_TASK} &
  sleep 5

  for ((  i=1; i<=$worker_num; i++ ))
  do
    node2=${nodes_array[$i]}
    srun --nodes=1 --ntasks=1 -w $node2 ray start --block --address=$ip_head --redis-password=$redis_password & # Starting the workers
    # Flag --block will keep ray process alive on each compute node.
    sleep 5
  done

  python -u trainer.py $redis_password 15 # Pass the total number of allocated CPUs

.. code-block:: python

  # trainer.py
  from collections import Counter
  import os
  import sys
  import time
  import ray

  redis_password = sys.argv[1]
  num_cpus = int(sys.argv[2])

  ray.init(address=os.environ["ip_head"], _redis_password=redis_password)

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
