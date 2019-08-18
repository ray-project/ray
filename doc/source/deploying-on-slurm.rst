Deploying on Slurm
==================

Clusters managed by Slurm may require that Ray is initialized as a part of the submitted job. This can be done by using ``srun`` within the submitted script. For example:

.. code-block:: bash

  #!/bin/bash

  #SBATCH --job-name=test
  #SBATCH --cpus-per-task=20
  #SBATCH --mem-per-cpu=1GB
  #SBATCH --nodes=5
  #SBATCH --tasks-per-node 1

  worker_num=4 # Must be one less that the total number of nodes

  module load Langs/Python/3.6.4 # This will vary depending on your environment
  source venv/bin/activate

  nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
  nodes_array=( $nodes )

  node1=${nodes_array[0]}

  ip_prefix=$(srun --nodes=1 --ntasks=1 -w $node1 hostname --ip-address) # Making redis-address
  suffix=':6379'
  ip_head=$ip_prefix$suffix

  export ip_head # Exporting for latter access by trainer.py

  srun --nodes=1 --ntasks=1 -w $node1 ray start --block --head --redis-port=6379 & # Starting the head
  sleep 5

  for ((  i=1; i<=$worker_num; i++ ))
  do
    node2=${nodes_array[$i]}
    srun --nodes=1 --ntasks=1 -w $node2 ray start --block --redis-address=$ip_head & # Starting the workers
    sleep 5
  done

  python trainer.py 100 # Pass the total number of allocated CPUs

.. code-block:: python

  # trainer.py
  import os
  import sys
  import time
  import ray
  
  ray.init(redis_address=os.environ["ip_head"])
 
  @ray.remote
  def f():
    time.sleep(1)

  # The following takes one second (assuming that ray was able to access all of the allocated nodes).
  start = time.time()
  num_cpus = int(sys.argv[1])
  ray.get([f.remote() for _ in range(num_cpus)])  
  end = time.time()
  print(end - start)
