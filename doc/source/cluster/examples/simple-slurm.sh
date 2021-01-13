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

