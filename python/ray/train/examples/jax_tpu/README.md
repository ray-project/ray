# Distributed Jax Training On Ray Cluster on Tpu Pod 
A ray cluster is set up on the tpu pod and a jax trainer is trained in the ray cluster
# How to run 

- set up the tpu pod 

launch the tpu pod on the gcloud

```python
bash scripts/tpu_launcher.sh
```

- test the tpu 

test whether the tpu pod is successfully launched
```python
gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo python3 -c \"import jax; print(jax.device_count(), jax.local_device_count())\"" --worker all
```

- set up the ray cluster

build the ray cluster across the tpu pod

```python
bash scripts/tpu_ray_cluster.sh
```

- Train a mnist classifier

A neural network is training using Jax on the Ray cluster


```python
bash scripts/tpu_ray_mnist.sh
```
