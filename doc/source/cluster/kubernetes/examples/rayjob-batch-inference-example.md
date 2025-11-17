(kuberay-batch-inference-example)=

# RayJob Batch Inference Example

This example demonstrates how to use the RayJob custom resource to run a batch inference job for an image classification workload on a Ray cluster.
See [Image Classification Batch Inference with HuggingFace Vision Transformer](https://docs.ray.io/en/latest/data/examples/huggingface_vit_batch_prediction.html) for a full explanation of the code.

## Prerequisites

You must have a Kubernetes cluster running,`kubectl` configured to use it, and GPUs available. This example provides a brief tutorial for setting up the necessary GPUs on Google Kubernetes Engine (GKE), but you can use any Kubernetes cluster with GPUs.

## Step 0: Create a Kubernetes cluster on GKE (Optional)

If you already have a Kubernetes cluster with GPUs, you can skip this step.


Otherwise, follow [this tutorial](kuberay-gke-gpu-cluster-setup), but substitute the following GPU node pool creation command to create a Kubernetes cluster on GKE with four NVIDIA T4 GPUs:

```sh
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-tesla-t4,count=4,gpu-driver-version=default \
  --zone us-west1-b \
  --cluster kuberay-gpu-cluster \
  --num-nodes 1 \
  --min-nodes 0 \
  --max-nodes 1 \
  --enable-autoscaling \
  --machine-type n1-standard-64
```

This example uses four [NVIDIA T4](https://cloud.google.com/compute/docs/gpus#nvidia_t4_gpus) GPUs. The machine type is `n1-standard-64`, which has [64 vCPUs and 240 GB RAM](https://cloud.google.com/compute/docs/general-purpose-machines#n1_machine_types).

## Step 1: Install the KubeRay Operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
The KubeRay operator Pod must be on the CPU node if you have set up the taint for the GPU node pool correctly.

## Step 2: Submit the RayJob

Create the RayJob custom resource with [ray-job.batch-inference.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-job.batch-inference.yaml).

Download the file with `curl`:

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-job.batch-inference.yaml
```

Note that the `RayJob` spec contains a spec for the `RayCluster`. This tutorial uses a single-node cluster with 4 GPUs. For production use cases, use a multi-node cluster where the head node doesn't have GPUs, so that Ray can automatically schedule GPU workloads on worker nodes which won't interfere with critical Ray processes on the head node.

Note the following fields in the `RayJob` spec, which specify the Ray image and the GPU resources for the Ray node:

```yaml
        spec:
          containers:
            - name: ray-head
              image: rayproject/ray-ml:2.6.3-gpu
              resources:
                limits:
                  nvidia.com/gpu: "4"
                  cpu: "54"
                  memory: "54Gi"
                requests:
                  nvidia.com/gpu: "4"
                  cpu: "54"
                  memory: "54Gi"
              volumeMounts:
                - mountPath: /home/ray/samples
                  name: code-sample
          nodeSelector:
            cloud.google.com/gke-accelerator: nvidia-tesla-t4 # This is the GPU type we used in the GPU node pool.
```

To submit the job, run the following command:

```bash
kubectl apply -f ray-job.batch-inference.yaml
```

Check the status with `kubectl describe rayjob rayjob-sample`.

Sample output:

```
[...]
Status:
  Dashboard URL:          rayjob-sample-raycluster-j6t8n-head-svc.default.svc.cluster.local:8265
  End Time:               ...
  Job Deployment Status:  Complete
  Job Id:                 rayjob-sample-ft8lh
  Job Status:             SUCCEEDED
  Message:                Job finished successfully.
  Observed Generation:    2
  ...
```

To view the logs, first find the name of the pod running the job with `kubectl get pods`.

Sample output:

```bash
NAME                                        READY   STATUS      RESTARTS   AGE
kuberay-operator-8b86754c-r4rc2             1/1     Running     0          25h
rayjob-sample-raycluster-j6t8n-head-kx2gz   1/1     Running     0          35m
rayjob-sample-w98c7                         0/1     Completed   0          30m
```

The Ray cluster is still running because `shutdownAfterJobFinishes` isn't set in the `RayJob` spec. If you set `shutdownAfterJobFinishes` to `true`, the cluster is shut down after the job finishes.

Next, run:

```text
kubectl logs rayjob-sample-w98c7
```

to get the standard output of the `entrypoint` command for the `RayJob`.  Sample output:

```text
[...]
Running: 62.0/64.0 CPU, 4.0/4.0 GPU, 955.57 MiB/12.83 GiB object_store_memory:   0%|          | 0/200 [00:05<?, ?it/s]
Running: 61.0/64.0 CPU, 4.0/4.0 GPU, 999.41 MiB/12.83 GiB object_store_memory:   0%|          | 0/200 [00:05<?, ?it/s]
Running: 61.0/64.0 CPU, 4.0/4.0 GPU, 999.41 MiB/12.83 GiB object_store_memory:   0%|          | 1/200 [00:05<17:04,  5.15s/it]
Running: 61.0/64.0 CPU, 4.0/4.0 GPU, 1008.68 MiB/12.83 GiB object_store_memory:   0%|          | 1/200 [00:05<17:04,  5.15s/it]
Running: 61.0/64.0 CPU, 4.0/4.0 GPU, 1008.68 MiB/12.83 GiB object_store_memory: 100%|██████████| 1/1 [00:05<00:00,  5.15s/it]

2023-08-22 15:48:33,905 WARNING actor_pool_map_operator.py:267 -- To ensure full parallelization across an actor pool of size 4, the specified batch size should be at most 5. Your configured batch size for this operator was 16.
<PIL.Image.Image image mode=RGB size=500x375 at 0x7B37546CF7F0>
Label:  tench, Tinca tinca
<PIL.Image.Image image mode=RGB size=500x375 at 0x7B37546AE430>
Label:  tench, Tinca tinca
<PIL.Image.Image image mode=RGB size=500x375 at 0x7B37546CF430>
Label:  tench, Tinca tinca
<PIL.Image.Image image mode=RGB size=500x375 at 0x7B37546AE430>
Label:  tench, Tinca tinca
<PIL.Image.Image image mode=RGB size=500x375 at 0x7B37546CF7F0>
Label:  tench, Tinca tinca
2023-08-22 15:48:36,522 SUCC cli.py:33 -- -----------------------------------
2023-08-22 15:48:36,522 SUCC cli.py:34 -- Job 'rayjob-sample-ft8lh' succeeded
2023-08-22 15:48:36,522 SUCC cli.py:35 -- -----------------------------------
```
