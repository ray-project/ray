#!/bin/bash

gcloud alpha compute tpus tpu-vm delete jax-trainer-mnist-tpu-pod \
  --zone=us-central1-a 
