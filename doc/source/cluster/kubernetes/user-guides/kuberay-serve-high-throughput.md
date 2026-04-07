(kuberay-serve-high-throughput)=
# Enable High Throughput on Ray Serve with KubeRay

Take advantage of major upgrades to Ray Serve, delivering online Inference with
88% lower latency and 11.1x higher throughput.

## Prerequisites

- Ray 2.54 or later

## Enable high throughput

Ray Serve optimizations for high throughput and the new HAProxy ingress are
enabled with the environment variables `RAY_SERVE_THROUGHPUT_OPTIMIZED` and
`RAY_SERVE_ENABLE_HA_PROXY`.

To enable these optimizations on your own Ray Service, add these environment
variables to both the head and worker group specifications:

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

## Example: Serving Qwen on GKE

The following example demonstrates how to deploy Qwen 3.5 4B on four replicas
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

### 2. Create the GKE cluster

Create a GKE cluster with the Ray operator and Gateway API enabled:

```bash
gcloud container clusters create $CLUSTER \
  --project $PROJECT \
  --location $LOCATION \
  --release-channel rapid \
  --enable-ray-cluster-logging \
  --enable-ray-cluster-monitoring
```

### 3. Configure the GPU node pool

Create a node pool with NVIDIA L4 GPUs:

```bash
gcloud container node-pools create gpu-pool \
  --cluster=$CLUSTER \
  --location=$LOCATION \
  --accelerator="type=nvidia-l4,count=1,gpu-driver-version=latest" \
  --machine-type=g2-standard-8 \
  --num-nodes=4
```

### 4. Set up Hugging Face access

Create a Kubernetes secret with your Hugging Face API token:

```bash
kubectl create secret generic hf-secret \
    --from-literal=hf_api_token=${HUGGING_FACE_TOKEN?}
```

### 5. Deploy the RayService

Deploy the high-throughput LLM service:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-throughput-llm.yaml
```

### 6. Verify HAProxy status

Identify the head and worker pod names, then verify that HAProxy is running on each:

```bash
# Get pod names
HEAD_POD=$(kubectl get pods -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
WORKER_POD=$(kubectl get pods -l ray.io/node-type=worker -o jsonpath='{.items[0].metadata.name}')

# Check for haproxy process
kubectl exec -it $HEAD_POD -- pgrep haproxy
kubectl exec -it $WORKER_POD -- pgrep haproxy
```

If the commands return a process ID, HAProxy is running successfully. If no ID
is returned, double-check your configuration.

## Expected performance

Performance gains scale with the size of the deployment.

The following measurements were taken using `vllm bench` with an input sequence
length (ISL) of 512 tokens, an output sequence length (OSL) of 128 tokens, and
a maximum concurrency of 300:

### Total Output Throughput (Tokens Per Second) - Higher is better

| Configuration | 2 Replicas | 4 Replicas |
| --- | --- | --- |
| Non-Optimized | 355 | 573 |
| **Optimized** | 358 | 756 |
| **Optimization Gain** | **+1%** | **+32%** |

### Median Time to First Token (ms) - Lower is better

| Configuration | 2 Replicas | 4 Replicas |
| --- | --- | --- |
| Non-Optimized | 2710 | 3721 |
| **Optimized** | 1143 | 1225 |
| **Latency Reduction** | **-58%** | **-67%** |

## Next steps

* Learn more about {ref}`Ray Serve Performance Tuning <serve-perf-tuning>`.
* Read the technical deep dive: [Ray Serve: Lower Latency and Higher Throughput with HAProxy](https://www.anyscale.com/blog/ray-serve-inference-lower-latency-higher-throughput-haproxy).
