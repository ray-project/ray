head_ip=`gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo python3 -c \"import ray; print(ray._private.services.get_node_ip_address())\"" --worker 0`
echo "head node ip: "$head_ip

gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo ray stop && sudo env LD_LIBRARY_PATH=/usr/local/lib RAY_TPU_DEV=1 ray start --head --port=6379 --resources='{\"TPU\":1}'" --worker=0

for i in 1 2 3 
do 
    gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo ray stop && sudo env LD_LIBRARY_PATH=/usr/local/lib RAY_TPU_DEV=1 ray start --address='${head_ip}:6379' --resources='{\"TPU\":1}'" --worker=$i
done 
