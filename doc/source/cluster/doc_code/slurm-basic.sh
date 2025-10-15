#!/bin/bash
# shellcheck disable=SC2206
#SBATCH --job-name=test
#SBATCH --cpus-per-task=5
#SBATCH --mem-per-cpu=1GB
#SBATCH --nodes=4
#SBATCH --tasks-per-node=1
#SBATCH --time=00:30:00

set -x

# __doc_head_address_start__

# Getting the node names
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

head_node=${nodes_array[0]}

port=6379
ip_head=$head_node:$port
export ip_head
echo "IP Head: $ip_head"
# __doc_head_address_end__

# __doc_symmetric_run_start__
# Start Ray cluster using symmetric_run.py on all nodes.
# Symmetric run will automatically start Ray on all nodes and run the script ONLY the head node.
# Use the '--' separator to separate Ray arguments and the entrypoint command.
# The --min-nodes argument ensures all nodes join before running the script.

# All nodes (including head and workers) will execute this block.
# The entrypoint (simple-trainer.py) will only run on the head node.
srun --nodes="$SLURM_JOB_NUM_NODES" --ntasks="$SLURM_JOB_NUM_NODES" \
    ray symmetric-run \
    --address "$ip_head" \
    --min-nodes "$SLURM_JOB_NUM_NODES" \
    --num-cpus="${SLURM_CPUS_PER_TASK}" \
    --num-gpus="${SLURM_GPUS_PER_TASK}" \
    -- \
    python -u simple-trainer.py "$SLURM_CPUS_PER_TASK"
# __doc_symmetric_run_end__

# __doc_script_start__
# The entrypoint script (simple-trainer.py) will be run on the head node by symmetric_run.
