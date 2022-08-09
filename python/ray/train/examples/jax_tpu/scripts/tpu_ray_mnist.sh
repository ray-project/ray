#!/bin/bash
folder=$(dirname "$0")

gcloud alpha compute tpus tpu-vm scp --recurse "$folder/_tpu_ray_mnist.sh" jax-trainer-mnist-tpu-pod: --zone=us-central1-a --worker all
# this one is not run SPMD on TPUs!!
gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "bash _tpu_ray_mnist.sh" --worker 0
