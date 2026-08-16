---
myst:
  html_meta:
    description: "Deploy Ray Sandboxes on Google Kubernetes Engine (GKE) with KubeRay for kernel-isolated, high-throughput untrusted code execution using gVisor."
---

(kuberay-sandboxing)=

# Deploying Ray Sandboxes using KubeRay

This guide covers how to deploy and orchestrate Ray Sandboxes using Ray and KubeRay. In this guide, we use Google Kubernetes Engine (GKE) as an example, but the same principles apply to other Kubernetes distributions.

Ray Sandboxes (introduced experimentally in `ray.experimental.sandbox`) allow Reinforcement Learning (RL) rollout workers and autonomous LLM agents to execute untrusted, model-generated code safely inside lightweight, kernel-isolated environments on Ray worker nodes. By utilizing [gVisor](https://github.com/google/gvisor) (`runsc`) directly inside Ray worker Pods, Ray delivers sub-100ms startup latencies and dense bin packing of hundreds of concurrent sandboxes per node without the multi-second overhead of provisioning separate Kubernetes Pods.

:::{warning}
Ray Sandboxes is an **experimental** feature (`ray.experimental.sandbox`). Experimental APIs are subject to change or removal in future releases prior to General Availability (GA) graduation.
:::

---

## Prerequisites

* `kubectl` installed and configured with access to your Kubernetes cluster.
* `gcloud` CLI installed and authenticated to your Google Cloud project.
* [Helm](https://helm.sh/) v3 installed.
* Ray 2.58.0 or newer with the `ray.experimental.sandbox` package.

---

## Step 1: Create a GKE Cluster

Create a GKE cluster with standard Linux worker nodes. Because gVisor runs inside the Ray container processes in rootless user space, you can use standard GKE node pools with the `containerd` container runtime.

```bash
gcloud container clusters create ray-sandbox-cluster \
    --region=us-central1 \
    --machine-type=e2-standard-16 \
    --num-nodes=3
```

---

## Step 2: Install the KubeRay Operator

Follow {ref}`this document <kuberay-operator-deploy>` to install the latest stable KubeRay operator using the Helm repository.

---

## Step 3: Create a RayJob

Create a RayJob which will create a RayCluster configured with `runsc` and submit a Ray job that manages Ray sandboxes:

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.sandbox.yaml
```

The RayJob is configured to do the following:
* Create a RayCluster configured to install runsc at startup and necessary securityContext required for gVisor
* Submit a Ray job which will create a sandbox and execute some Python code inside it 
* Terminate sandboxes after the job is done

Below is the script used for the RayJob:

```python
import ray
from ray.experimental import sandbox

ray.init()

sb = sandbox.create(
    image='python:3.12-slim',
    workdir='/workspace',
    cpu=1.0,
    memory='1Gi',
)

script = '''
import platform
import sys

print('=== Hello from inside Ray Sandbox! ===')
print(f'Python Version : {sys.version}')
print(f'Platform       : {platform.platform()}')
'''
ray.get(sb.write_file.remote('/workspace/main.py', script))

result = ray.get(sb.exec.remote('python3 /workspace/main.py'))
print(f'Exit code: {result.exit_code}')
print('Sandbox output:')
print(result.stdout)

ray.get(sb.delete.remote())
print('RayJob completed successfully!')
```

Monitor the status and output of the job:

```sh
# List running job pods (wait for Ray cluster to be in ready state)
kubectl get pods -l job-name=rayjob-sandbox

# Stream the demo logs
kubectl logs -f -l job-name=rayjob-sandbox
```

The output should be similar to the following:

```sh
Sandbox output:
=== Hello from inside Ray Sandbox! ===
Python Version : 3.12.14 (main, Aug 13 2026, 19:41:13) [GCC 14.2.0]
Platform       : Linux-4.19.0-gvisor-x86_64-with-glibc2.41

RayJob completed successfully!
2026-08-15 17:38:41,535	INFO sdk.py:520 -- WebSocket closed for job rayjob-sandbox-gz8j6 with close code 1000
2026-08-15 17:38:41,546	SUCC cli.py:66 -- ------------------------------------
2026-08-15 17:38:41,546	SUCC cli.py:67 -- Job 'rayjob-sandbox-gz8j6' succeeded
2026-08-15 17:38:41,546	SUCC cli.py:68 -- ------------------------------------
```
---

## (Optional) Step 4: Verify Isolation and Security Guarantees

You can verify that untrusted code running inside the sandbox cannot compromise the host environment or escape its sandbox boundary:

### Filesystem Write Protection

By default, base root filesystems are mounted read-only (`readonly=True`). Only the designated `workdir` is writable.

```python
# Attempting to modify /etc or rootfs will fail
res = ray.get(sb.exec.remote("touch /etc/hacked.txt"))
print(res.exit_code)  # Non-zero exit code
print(res.stderr)     # "Read-only file system"
```

### Network Isolation

With `network="none"` (the default), untrusted code cannot establish outbound connections to the internet or probe internal Kubernetes cluster services:

```python
# Attempting network egress will immediately fail
res = ray.get(sb.exec.remote("python3 -c 'import urllib.request; urllib.request.urlopen(\"http://google.com\", timeout=2)'"))
print(res.exit_code)  # Non-zero exit code
```

---

## (Optional) Step 5: Build a Custom Ray Image with Pre-installed `runsc`

Ray Sandboxes require the `runsc` binary to be available in the Ray worker container's `$PATH` (for example, `/usr/local/bin/runsc`).

While the example above downloads `runsc` dynamically at pod startup, pre-installing `runsc` directly in your container image is recommended for production. Pre-baking the binary eliminates runtime network dependencies, avoids external download failures or rate limits, and decreases pod startup latency.

You can build a custom Ray worker image using the following `Dockerfile`:

```dockerfile
FROM rayproject/ray:2.58.0-py312

USER root

# Install wget, download gVisor runsc, and install into system PATH
RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/* && \
    ARCH=$(uname -m) && \
    wget "https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/runsc" -O /usr/local/bin/runsc && \
    chmod a+rx /usr/local/bin/runsc

USER ray
```

---

## Next Steps

* Read the {ref}`Ray Sandboxes Core Documentation <ray-core-sandboxes>` for API details and custom actor patterns.
* Learn more about [gVisor](https://gvisor.dev/docs/).
* Explore {ref}`Resource Isolation with Cgroup v2 <resource-isolation>` to isolate Ray system processes from worker processes.
