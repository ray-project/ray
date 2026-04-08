(kuberay-serve-high-throughput)=
# Enable High Throughput on Ray Serve with KubeRay

Take advantage of major upgrades to Ray Serve, delivering online inference with
88% lower latency and 11.1x higher throughput.

## Prerequisites

- Ray 2.55 or later

## Enabling high throughput mode


With Ray 2.55 and later, high throughput config options are available for Ray
Serve by setting  the environment variables `RAY_SERVE_ENABLE_HA_PROXY` and
`RAY_SERVE_THROUGHPUT_OPTIMIZED`.

With the new proxy enabled, each Ray pod's proxy ingress (serving on port 8000
by default) is now HAProxy ingress, a highly optimized, battle-tested
open-source load balancer written in C.

The throughput optimized variable enables multiple high throughput serving optimizations, including direct gRPC data-plane communications
between Ray Serve replicas, improving the performance of inter-deployment
traffic.

## Example: Serving Qwen on GKE

The following example demonstrates how to deploy Qwen 3.5 on four replicas
with NVIDIA L4 GPUs using KubeRay on Google Kubernetes Engine (GKE).

### 1. Configure the environment

Set the following environment variables for your project:

```bash
CLUSTER=serve-qwen-optimized
PROJECT=$(gcloud config get-value project)
LOCATION=us-central1-b
REGION=us-central1
HUGGING_FACE_TOKEN=<your-token>
```

### 2. Create the GKE cluster with KubeRay

Create a GKE cluster:

```bash
gcloud container clusters create $CLUSTER \
  --project $PROJECT \
  --location $LOCATION \
  --machine-type=e2-standard-16 \
  --num-nodes=1
```

### 3. Install the KubeRay operator

Install the most recent stable KubeRay operator from the Helm repository by following [Deploy a KubeRay operator](../getting-started/kuberay-operator-installation.md). The Kubernetes `NoSchedule` taint in the example config prevents the KubeRay operator pod from running on a GPU node.

### 4. Configure the GPU node pool

Create a node pool with NVIDIA L4 GPUs:

```bash
gcloud container node-pools create gpu-pool \
  --cluster=$CLUSTER \
  --location=$LOCATION \
  --accelerator="type=nvidia-l4,count=1,gpu-driver-version=latest" \
  --machine-type=g2-standard-8 \
  --num-nodes=4
```

### 5. Set up Hugging Face access

Create a Kubernetes secret with your Hugging Face API token:

```bash
kubectl create secret generic hf-secret \
    --from-literal=hf_api_token=${HUGGING_FACE_TOKEN?}
```

### 6. Deploy the RayService

Deploy the example high-throughput LLM service:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-throughput-llm.yaml
```

To enable these optimizations on your own Ray Service, add the environment
variables `RAY_SERVE_ENABLE_HA_PROXY` and `RAY_SERVE_THROUGHPUT_OPTIMIZED` to
both the head and worker group specifications:

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: my-ray-service
spec:
  serveConfigV2: ...
  rayClusterConfig:
    headGroupSpec:
      template:
        spec:
          containers:
            - name: ray-head
              env:
                - name: RAY_SERVE_ENABLE_HA_PROXY
                  value: "1"
                - name: RAY_SERVE_THROUGHPUT_OPTIMIZED
                  value: "1"
    workerGroupSpecs:
      - template:
          spec:
            containers:
              - name: worker
                env:
                  - name: RAY_SERVE_ENABLE_HA_PROXY
                    value: "1"
                  - name: RAY_SERVE_THROUGHPUT_OPTIMIZED
                    value: "1"
```


### 7. Verify HAProxy status

Identify the head and worker pod names, then verify that HAProxy is running on each:

```bash
# Get pod names
HEAD_POD=$(kubectl get pods -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
WORKER_POD=$(kubectl get pods -l ray.io/node-type=worker -o jsonpath='{.items[0].metadata.name}')

# Check for haproxy process
kubectl exec $HEAD_POD -- pgrep haproxy
kubectl exec $WORKER_POD -- pgrep haproxy
```

If the commands return a process ID, HAProxy is running successfully. If the
command doesn't print an ID, double-check your configuration.

## Expected performance

See [the announcement blog post for detailed performance
numbers](https://www.anyscale.com/blog/ray-serve-inference-lower-latency-higher-throughput-haproxy).

The optimizations work best with deployments with high load, where the serve app
handles 50+ requests per second, concurrent connections per replica is greater
than 250, and bursty traffic.

Performance gains scale with the size of the deployment. The more replicas your
RayService is using, the greater the performance improvement compared to
versions preceding 2.55.

## Next steps

* Learn more about {ref}`Ray Serve Performance Tuning <serve-perf-tuning>`.
* Read the technical deep dive: [Ray Serve: Lower Latency and Higher Throughput with HAProxy](https://www.anyscale.com/blog/ray-serve-inference-lower-latency-higher-throughput-haproxy).
