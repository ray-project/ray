#!/bin/bash

gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo python3 -c \"import jax; print(jax.device_count(), jax.local_device_count())\"" --worker all
