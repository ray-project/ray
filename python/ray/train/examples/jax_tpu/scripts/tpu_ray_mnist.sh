
# this one is not run SPMD on TPUs!!
gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "bash ray_jax_trainer_setup.sh" --worker 0