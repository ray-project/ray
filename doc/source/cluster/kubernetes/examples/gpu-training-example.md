(kuberay-gpu-training-example)=

# ML training with GPUs on Kubernetes
In this guide, we show you how to run a sample Ray machine learning training workload with GPU on Kubernetes infrastructure. We will run Ray's {ref}`PyTorch image training benchmark <pytorch_gpu_training_benchmark>` with a 1 gigabyte training set.

:::{note}
To learn the basics of Ray on Kubernetes, we recommend taking a look
at the {ref}`introductory guide <kuberay-quickstart>` first.
:::

Note that a version of at least 1.19 is required for Kubernetes and Kubectl.

## The end-to-end workflow
The following script summarizes the end-to-end workflow for GPU training. These instructions are for GCP, but a similar setup would work for any major cloud provider. The following script consists of:
- Step 1: Set up a Kubernetes cluster on GCP.
- Step 2: Deploy a Ray cluster on Kubernetes with the KubeRay operator.
- Step 3: Run the PyTorch image training benchmark.
```shell
# Step 1: Set up a Kubernetes cluster on GCP
# Create a node-pool for a CPU-only head node
# e2-standard-8 => 8 vCPU; 32 GB RAM
gcloud container clusters create gpu-cluster-1 \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-central1-c --machine-type e2-standard-8

# Create a node-pool for GPU. The node is for a GPU Ray worker node.
# n1-standard-8 => 8 vCPU; 30 GB RAM
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-tesla-t4,count=1 \
  --zone us-central1-c --cluster gpu-cluster-1 \
  --num-nodes 1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
  --machine-type n1-standard-8

# Install NVIDIA GPU device driver
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded.yaml

# Step 2: Deploy a Ray cluster on Kubernetes with the KubeRay operator.
# Please make sure you are connected to your Kubernetes cluster. For GCP, you can do so by:
#   (Method 1) Copy the connection command from the GKE console
#   (Method 2) "gcloud container clusters get-credentials <your-cluster-name> --region <your-region> --project <your-project>"
#   (Method 3) "kubectl config use-context ..."

# Create the KubeRay operator
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.4.0&timeout=90s"

# Create a Ray cluster
kubectl apply -f https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/ray-cluster.gpu.yaml

# Set up port-forwarding
kubectl port-forward services/raycluster-head-svc 8265:8265

# Test the cluster
ray job submit --address http://localhost:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())"

# Step 3: Run the PyTorch image training benchmark.
pip3 install -U "ray[default]"

# Download the Python script
curl https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/doc_code/pytorch_training_e2e_submit.py -o pytorch_training_e2e_submit.py

# Submit the training job to your ray cluster
python3 pytorch_training_e2e_submit.py

# Use the following command to follow this Job's logs:
# Substitute the Ray Job's submission id.
ray job logs 'raysubmit_xxxxxxxxxxxxxxxx' --follow
```
In the rest of this document, we present a more detailed breakdown of the above workflow.

## Step 1: Set up a Kubernetes cluster on GCP.
In this section, we set up a Kubernetes cluster with CPU and GPU node pools. These instructions are for GCP, but a similar setup would work for any major cloud provider. If you have an existing Kubernetes cluster with GPU, you can ignore this step.

If you are new to Kubernetes and you are planning to deploy Ray workloads on a managed
Kubernetes service, we recommend taking a look at this {ref}`introductory guide <kuberay-k8s-setup>` first.

It is not necessary to run this example with a cluster having that much RAM (>30GB per node in the following commands). Feel free to update
the option `machine-type` and the resource requirements in `ray-cluster.gpu.yaml`.

In the first command, we create a Kubernetes cluster `gpu-cluster-1` with one CPU node (`e2-standard-8`: 8 vCPU; 32 GB RAM). In the second command,
we add a new node (`n1-standard-8`: 8 vCPU; 30 GB RAM) with a GPU (`nvidia-tesla-t4`) to the cluster.

```shell
# Step 1: Set up a Kubernetes cluster on GCP.
# e2-standard-8 => 8 vCPU; 32 GB RAM
gcloud container clusters create gpu-cluster-1 \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-central1-c --machine-type e2-standard-8

# Create a node-pool for GPU
# n1-standard-8 => 8 vCPU; 30 GB RAM
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-tesla-t4,count=1 \
  --zone us-central1-c --cluster gpu-cluster-1 \
  --num-nodes 1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
  --machine-type n1-standard-8

# Install NVIDIA GPU device driver
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded.yaml
```

## Step 2: Deploy a Ray cluster on Kubernetes with the KubeRay operator.

To execute the following steps, please make sure you are connected to your Kubernetes cluster. For GCP, you can do so by:
* Copy the connection command from the GKE console
* `gcloud container clusters get-credentials <your-cluster-name> --region <your-region> --project <your-project>` ([Link](https://cloud.google.com/sdk/gcloud/reference/container/clusters/get-credentials))
* `kubectl config use-context` ([Link](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/))

The first command will deploy KubeRay (ray-operator) to your Kubernetes cluster. The second command will create a ray cluster with the help of KubeRay.

The third command is used to map port 8265 of the `ray-head` pod to **127.0.0.1:8265**. You can check
**127.0.0.1:8265** to see the dashboard. The last command is used to test your Ray cluster by submitting a simple job.
It is optional.

```shell
# Step 2: Deploy a Ray cluster on Kubernetes with the KubeRay operator.
# Create the KubeRay operator
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.4.0&timeout=90s"

# Create a Ray cluster
kubectl apply -f https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/ray-cluster.gpu.yaml

# port forwarding
kubectl port-forward services/raycluster-head-svc 8265:8265

# Test cluster (optional)
ray job submit --address http://localhost:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

## Step 3: Run the PyTorch image training benchmark.
We will use the [Ray Job Python SDK](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/sdk.html#ray-job-sdk) to submit the PyTorch workload.

```python
from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient("http://127.0.0.1:8265")

kick_off_pytorch_benchmark = (
    # Clone ray. If ray is already present, don't clone again.
    "git clone https://github.com/ray-project/ray || true;"
    # Run the benchmark.
    "python ray/release/air_tests/air_benchmarks/workloads/pytorch_training_e2e.py"
    " --data-size-gb=1 --num-epochs=2 --num-workers=1"
)


submission_id = client.submit_job(
    entrypoint=kick_off_pytorch_benchmark,
)

print("Use the following command to follow this Job's logs:")
print(f"ray job logs '{submission_id}' --follow")
```

To submit the workload, run the above Python script. The script is available in the [Ray repository](https://github.com/ray-project/ray/tree/master/doc/source/cluster/doc_code/pytorch_training_e2e_submit.py)
```shell
# Step 3: Run the PyTorch image training benchmark.
# Install Ray if needed
pip3 install -U "ray[default]"

# Download the Python script
curl https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/doc_code/pytorch_training_e2e_submit.py -o pytorch_training_e2e_submit.py

# Submit the training job to your ray cluster
python3 pytorch_training_e2e_submit.py
# Example STDOUT:
# Use the following command to follow this Job's logs:
# ray job logs 'raysubmit_jNQxy92MJ4zinaDX' --follow

# Track job status
# Substitute the Ray Job's submission id.
ray job logs 'raysubmit_xxxxxxxxxxxxxxxx' --follow
```

## Clean-up
Delete your Ray cluster and KubeRay with the following commands:
```shell
kubectl delete raycluster raycluster

# Please make sure the ray cluster has already been removed before delete the operator.
kubectl delete -k "http://github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.4.0&timeout=90s"
```
If you're on a public cloud, don't forget to clean up the underlying
node group and/or Kubernetes cluster.
