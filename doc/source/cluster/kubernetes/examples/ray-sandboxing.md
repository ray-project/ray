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

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-job.sandbox.yaml
```

The RayJob is configured to do the following:
* Create a RayCluster configured to install `runsc` at startup with the necessary `securityContext` required for gVisor
* Submit a Ray job which will create a sandbox and execute some Python code inside it
* Terminate sandboxes after the job is done

Below is the script used for the RayJob:

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

```bash
# List running job pods (wait for Ray cluster to be in ready state)
kubectl get pods -l job-name=rayjob-sandbox

# Stream the demo logs
kubectl logs -f -l job-name=rayjob-sandbox
```

The output should be similar to the following:

```text
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

With `network="public"`, each sandbox gets internet egress from its own private network namespace, bridged by [slirp4netns](https://github.com/rootless-containers/slirp4netns): sandboxes can bind the same port concurrently without conflicting, and can't reach each other or the Pod's own services. They can still reach anything else the node can reach, including other Pods and internal cluster services, so keep `network="none"` for untrusted code. This mode needs the `slirp4netns` binary on the worker's `$PATH` and `/dev/net/tun` available in the Ray container (present on standard GKE `containerd` node pools).

---

## (Optional) Step 5: Build a custom Ray image with pre-installed `runsc`

Ray Sandboxes require the `runsc` binary in the Ray worker container's `$PATH`, for example `/usr/local/bin/runsc`.

The example above downloads `runsc` at Pod startup. For production, pre-install `runsc` in your container image. Pre-baking the binary eliminates runtime network dependencies, avoids external download failures or rate limits, and decreases Pod startup latency.

You can build a custom Ray worker image using the following `Dockerfile`:

```dockerfile
FROM rayproject/ray:2.58.0-py312

USER root

# Install wget, download gVisor runsc and slirp4netns (for network="public"),
# and install both into the system PATH.
RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    ARCH=$(uname -m) && \
    wget "https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/runsc" -O /usr/local/bin/runsc && \
    wget "https://github.com/rootless-containers/slirp4netns/releases/download/v1.3.5/slirp4netns-${ARCH}" -O /usr/local/bin/slirp4netns && \
    chmod a+rx /usr/local/bin/runsc /usr/local/bin/slirp4netns && \
    rm -rf /var/lib/apt/lists/*

USER ray
```

---

## (Optional) Use the Modal Python SDK

The sandbox gRPC facade lets an external client use a subset of the Modal
Sandbox API against a RayCluster. The client doesn't need Ray installed: the
facade submits work to Ray, and the Ray workers run the sandboxes with `runsc`.

:::{warning}
This example requires the development implementation in
[Ray PR #65839](https://github.com/ray-project/ray/pull/65839), including its
dependency on [#65633](https://github.com/ray-project/ray/pull/65633).
It is not available in the released Ray 2.58.0 image. The example was tested
with facade commit `259c9a3dd654478bac4784ca01923049c7172846` and
`modal==1.5.5`; this is not a claim of full Modal API compatibility.
:::

### Prepare the Ray image

Prepare an image containing the Ray build under test, `ray[serve]`, `grpclib`,
and `runsc`. See {ref}`building-ray` for building Ray from source, and the
custom-image section above for installing `runsc`. Use the same Ray build in
the head, worker, and facade containers. The worker Pods must be able to pull
the sandbox image from Docker Hub, including its authentication and blob
endpoints. The Ray container image and the sandbox image are separate images.

Before deploying, verify your image, replacing the example image name with
your own registry location:

```bash
export RAY_SANDBOX_IMAGE=your-registry/ray-sandbox-sdk:dev
docker run --rm --entrypoint python "$RAY_SANDBOX_IMAGE" \
  -c 'import ray.experimental.sandbox.http.grpc_facade'
docker run --rm --entrypoint runsc "$RAY_SANDBOX_IMAGE" --version
```

### Deploy a persistent cluster and facade

Use a persistent RayCluster for SDK clients, rather than a RayJob that can
shut down its cluster after completion. Download
{download}`ray-sandbox-sdk.yaml <../configs/ray-sandbox-sdk.yaml>` and replace
the example image name in all three containers:

```bash
sed "s|ray-sandbox-sdk:dev|${RAY_SANDBOX_IMAGE}|g" ray-sandbox-sdk.yaml \
  | kubectl apply -f -
kubectl -n ray-sandbox-sdk wait --for=jsonpath='{.status.state}'=ready \
  raycluster/sandbox-sdk --timeout=300s
kubectl -n ray-sandbox-sdk wait --for=condition=Ready pod \
  -l ray.io/cluster=sandbox-sdk,ray.io/node-type=head --timeout=300s
```

The manifest reserves the head for cluster management and gives one worker
two logical CPUs. The facade runs as a sidecar in the head Pod, connects to
`127.0.0.1:6379`, and shares `/tmp/ray` and the Pod's process namespace with
the Ray head container. Native Ray drivers discover the local raylet process
and use its sockets; pointing an ordinary standalone Pod at the GCS address
alone isn't sufficient. The worker Pods run separately and don't share this
process namespace.

Run only one facade process per cluster because command state lives in that
process. Restarting the facade can lose in-flight command state; replicas
behind a load balancer aren't supported by this implementation.

The facade in this example has no client authentication or TLS. Keep it on a
trusted cluster network and use the following loopback-only port forward for
local access. The REST service's token setting doesn't authenticate this
gRPC endpoint. Don't expose it through a public load balancer.

```bash
kubectl -n ray-sandbox-sdk port-forward service/sandbox-grpc 50051:50051
```

Leave this command running. The facade's `--advertise-url` is the command
router address returned to the SDK. It must be reachable **from the client**
and route to the same facade process. The manifest uses
`http://127.0.0.1:50051` for this port-forwarded example. For clients inside
the cluster, set the client's `RAY_SANDBOX_GRPC_URL` and `--advertise-url` to
`http://sandbox-grpc.ray-sandbox-sdk.svc.cluster.local:50051` instead.

### Run the SDK client

In another terminal, download
{download}`ray_sandbox_sdk.py <ray_sandbox_sdk.py>`, create a virtual
environment, and run the example:

```bash
python3 -m venv .venv-sandbox-sdk
. .venv-sandbox-sdk/bin/activate
python -m pip install 'modal==1.5.5'
python ray_sandbox_sdk.py
```

The client uses `modal.Client.anonymous` to select the local facade; it doesn't
require a Modal cloud account or token. It writes and executes a Python file,
checks a file round trip, and terminates the sandbox in a `finally` block:

```{literalinclude} ray_sandbox_sdk.py
:language: python
```

Expected output:

```text
Hello from a Ray sandbox on KubeRay!
File round trip succeeded.
Sandbox terminated.
```

The first operation can wait for the image to download and extract.
`Sandbox.create()` returning a handle doesn't mean the sandbox is ready.
Time the first successful command when measuring readiness. The sandbox's
`block_network=True` setting blocks the sandbox's own network access; it
doesn't prevent the Ray worker from downloading its image.

At the tested revision, use `sandbox.filesystem.write_text` and `read_text`,
as shown above. The deprecated `sandbox.open()` path isn't implemented, and
`filesystem.list_files()` returns a placeholder empty list. Avoid relying on
these operations until the facade supports them.

### Inspect and clean up

If the client doesn't complete, inspect the facade logs and worker scheduling:

```bash
kubectl -n ray-sandbox-sdk logs -l ray.io/cluster=sandbox-sdk,ray.io/node-type=head -c facade
kubectl -n ray-sandbox-sdk get pods
kubectl -n ray-sandbox-sdk get events --sort-by=.lastTimestamp
```

Check image-pull errors in the worker logs, available Ray resources, and both
the client URL and advertised router URL. The default sandbox image cache is
`/tmp/ray/sandbox/images` inside each worker Pod. This manifest doesn't mount
persistent storage there, so replacing a worker Pod loses its cache. A warm
cache on one worker doesn't prewarm another worker.

Stop the port forward with Ctrl+C, then remove the resources created by this
example:

```bash
kubectl delete namespace ray-sandbox-sdk
```

## Next steps

* See {ref}`ray-core-sandboxes` for API details and custom actor patterns.
* Learn more about [gVisor](https://gvisor.dev/docs/).
* Explore {ref}`resource-isolation-with-writable-cgroups` to configure resource isolation on Kubernetes.
