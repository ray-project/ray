---
myst:
  html_meta:
    description: "Deploy Ray Sandboxes on Google Kubernetes Engine (GKE) with KubeRay for kernel-isolated, high-throughput untrusted code execution using gVisor."
---

(kuberay-sandboxing)=

# Deploy Ray sandboxes with KubeRay

This guide covers how to deploy and orchestrate Ray Sandboxes using Ray and KubeRay. It uses Google Kubernetes Engine (GKE) as an example, but the same principles apply to other Kubernetes distributions.

Ray Sandboxes run untrusted, model-generated code safely inside lightweight, kernel-isolated environments, for reinforcement learning (RL) rollout workers and autonomous large language model (LLM) agents. Ray runs [gVisor](https://gvisor.dev/docs/) (`runsc`) directly inside Ray worker Pods, which delivers sub-100 ms startup latencies and dense bin packing of hundreds of concurrent sandboxes per node without the multi-second overhead of provisioning separate Kubernetes Pods.

:::{warning}
Ray Sandboxes (`ray.experimental.sandbox`) is an {ref}`alpha <api-stability-alpha>` library. The API can change or disappear in any release before it graduates to stable.
:::

---

## Prerequisites

* `kubectl` installed and configured with access to your Kubernetes cluster.
* `gcloud` CLI installed and authenticated to your Google Cloud project.
* [Helm](https://helm.sh/) v3 installed.
* Ray 2.58.0 or newer with the `ray.experimental.sandbox` package.

---

## Step 1: Create a GKE cluster

Create a GKE cluster with standard Linux worker nodes. Because gVisor runs inside the Ray container processes in rootless user space, you can use standard GKE node pools with the `containerd` container runtime.

```bash
gcloud container clusters create ray-sandbox-cluster \
    --region=us-central1 \
    --machine-type=e2-standard-16 \
    --num-nodes=3
```

---

## Step 2: Install the KubeRay operator

Follow {ref}`KubeRay operator installation <kuberay-operator-deploy>` to install the latest stable KubeRay operator from the Helm repository.

---

## Step 3: Run sandboxes with a RayJob

Create a RayJob that creates a RayCluster configured with `runsc` and submits a Ray job that manages Ray sandboxes:


```python
import ray
from ray.experimental import sandbox

ray.init()

sb = sandbox.create(
    image="python:3.12-slim",
    workdir="/workspace",
    cpu=1.0,
    memory="1Gi",
)

script = """\
import platform
import sys

print("=== Hello from inside Ray Sandbox! ===")
print(f"Python Version : {sys.version}")
print(f"Platform       : {platform.platform()}")
"""
ray.get(sb.write_file.remote("/workspace/main.py", script))

result = ray.get(sb.exec.remote("python3 /workspace/main.py"))
print(f"Exit code: {result.exit_code}")
print("Sandbox output:")
print(result.stdout)

ray.get(sb.delete.remote())
print("RayJob completed successfully!")
```

Monitor the status and output of the job:


---

## (Optional) Step 4: Verify isolation and security guarantees

You can verify that untrusted code running inside an active sandbox cannot compromise the host environment or escape its sandbox boundary:

### Filesystem write protection

By default, base root filesystems are mounted read-only (`readonly=True`). Only the designated `workdir` is writable.

```python
# Assuming an active sandbox `sb = sandbox.create(...)`
# Attempting to modify /etc or rootfs will fail
res = ray.get(sb.exec.remote("touch /etc/hacked.txt"))
print(res.exit_code)  # Non-zero exit code
print(res.stderr)     # "Read-only file system"
```

### Network isolation

With `network="none"` (the default), untrusted code cannot establish outbound connections to the internet or probe internal Kubernetes cluster services:

```python
# Attempting network egress will immediately fail
res = ray.get(sb.exec.remote("python3 -c 'import urllib.request; urllib.request.urlopen(\"http://google.com\", timeout=2)'"))
print(res.exit_code)  # Non-zero exit code
```

---

## (Optional) Step 5: Build a custom Ray image with pre-installed `runsc`

Ray Sandboxes require the `runsc` binary in the Ray worker container's `$PATH`, for example `/usr/local/bin/runsc`.

The example above downloads `runsc` at Pod startup. For production, pre-install `runsc` in your container image. Pre-baking the binary eliminates runtime network dependencies, avoids external download failures or rate limits, and decreases Pod startup latency.

You can build a custom Ray worker image using the following `Dockerfile`:

```dockerfile
FROM rayproject/ray:2.58.0-py312

USER root

# Install wget, download gVisor runsc, and install into system PATH
RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    ARCH=$(uname -m) && \
    wget "https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/runsc" -O /usr/local/bin/runsc && \
    chmod a+rx /usr/local/bin/runsc && \
    rm -rf /var/lib/apt/lists/*

USER ray
```

---

## Next steps

* See {ref}`ray-core-sandboxes` for API details and custom actor patterns.
* Learn more about [gVisor](https://gvisor.dev/docs/).
* Explore {ref}`resource-isolation-with-writable-cgroups` to configure resource isolation on Kubernetes.
