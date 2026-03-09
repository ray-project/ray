#!/bin/bash
# ==========================================================================
#  Ray Rust Backend — GPU NCCL Direct Transfer Experiment
# ==========================================================================
#
# Two-node AWS cluster:
#   Head node:  t3.large (Ubuntu 22.04)        — runs GCS, Raylet, Driver
#   GPU node:   g4dn.12xlarge (Deep Learning AMI) — runs Raylet, 4 GPU workers
#
# This script is a reference guide. Run each section manually on the
# appropriate node (head or GPU).
# ==========================================================================

set -e

cat <<'GUIDE'
==========================================================================
  STEP-BY-STEP EXPERIMENT GUIDE
==========================================================================

--- AWS Setup ---

1. Launch TWO EC2 instances in the same VPC/subnet:

   HEAD NODE:
     Instance type: t3.large
     AMI: Ubuntu 22.04 LTS (x86_64)
     Security group: allow all TCP from VPC CIDR + SSH from your IP

   GPU NODE:
     Instance type: g4dn.12xlarge (4x NVIDIA T4, 48 vCPUs, 192 GB RAM)
     AMI: Deep Learning OSS Nvidia Driver AMI GPU PyTorch 2.x (Ubuntu 22.04)
     Security group: same as head node

2. Note private IPs (e.g., HEAD_IP=172.31.x.x, GPU_IP=172.31.y.y)
   Ensure they can reach each other on all TCP ports.

--- Head Node Setup ---

3. SSH to head node, clone the Ray repo, and build:

   git clone <ray-repo-url> ~/ray
   cd ~/ray/rust/examples/python/gpu-test
   bash setup_head_node.sh

4. Deploy wheel and scripts to GPU node:

   WHEEL=$(ls ~/ray/rust/target/wheels/*.whl | head -1)
   GPU_IP=<gpu_node_private_ip>

   scp $WHEEL ubuntu@$GPU_IP:~/
   scp gpu_worker.py start_gpu_workers.sh setup_gpu_node.sh ubuntu@$GPU_IP:~/

--- GPU Node Setup ---

5. SSH to GPU node and run setup:

   bash ~/setup_gpu_node.sh ~/ray_rust_raylet-*.whl

--- Start Services ---

6. On HEAD NODE — start GCS server:

   HEAD_IP=$(hostname -I | awk '{print $1}')
   ~/ray/rust/target/release/gcs_server \
     --gcs-server-port 6379 \
     --node-ip-address $HEAD_IP \
     --session-name gpu-experiment

7. On HEAD NODE — start raylet:

   ~/ray/rust/target/release/raylet \
     --node-ip-address $HEAD_IP \
     --port 0 \
     --gcs-address $HEAD_IP:6379 \
     --resources CPU:2 \
     --session-name gpu-experiment

8. On GPU NODE — start raylet:

   GPU_IP=$(hostname -I | awk '{print $1}')
   HEAD_IP=<head_node_private_ip>

   # If raylet binary is not on GPU node, scp it first:
   # scp ubuntu@$HEAD_IP:~/ray/rust/target/release/raylet ~/

   ~/raylet \
     --node-ip-address $GPU_IP \
     --port 0 \
     --gcs-address $HEAD_IP:6379 \
     --resources CPU:48,GPU:4 \
     --session-name gpu-experiment

9. On GPU NODE — start 4 GPU workers (one per GPU):

   bash ~/start_gpu_workers.sh $HEAD_IP:6379 tcp://$GPU_IP:29500 4

   Wait for all 4 workers to print CONNECT_STRING lines.
   Collect the connect strings from /tmp/gpu_worker_*.log:

   for i in 0 1 2 3; do grep CONNECT_STRING /tmp/gpu_worker_${i}.log; done

--- Run the Experiment ---

10. On HEAD NODE — run the driver:

    cd ~/ray/rust/examples/python/gpu-test

    python3 gpu_driver.py \
      --gcs-address $HEAD_IP:6379 \
      --worker <connect_string_0> \
      --worker <connect_string_1> \
      --worker <connect_string_2> \
      --worker <connect_string_3>

    (Replace <connect_string_N> with the CONNECT_STRING values
     from step 9, format: ip:port:worker_id_hex)

--- Collect Results ---

11. The driver prints a full test report to stdout.
    Save it and update EXPERIMENT_REPORT.md with actual values.

==========================================================================
GUIDE
