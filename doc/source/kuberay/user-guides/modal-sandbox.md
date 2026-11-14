---
myst:
  html_meta:
    description: "Deploy the Modal-compatible Ray Sandbox gRPC facade on Google Kubernetes Engine (GKE) with KubeRay and Ray Serve, and manage sandboxes with the Modal Python SDK."
---

(kuberay-modal-sandbox)=

# Deploy Modal-compatible Ray Sandbox service using KubeRay

This guide shows how to deploy the Modal-compatible Ray Sandbox gRPC API on Google Kubernetes Engine (GKE) using KubeRay and Ray Serve, and how to create and manage isolated sandboxes with the Modal Python SDK.

Ray Sandboxes run untrusted code inside lightweight [gVisor](https://gvisor.dev/docs/) `runsc` containers directly on worker Pods. The `ray.experimental.sandbox.http.grpc_app` gRPC facade runs behind Ray Serve's gRPC proxy and implements the `ModalClient` control-plane and `TaskCommandRouter` command-router services that the Modal client SDK uses for its Sandbox API. You can point an unmodified Modal Python client at a KubeRay RayService to create sandboxes, execute commands, and read or write files on your own Ray cluster.

:::{warning}
Ray Sandboxes is an {ref}`alpha <api-stability-alpha>` library in the `ray.experimental.sandbox` package. The API can change or disappear in any release before it graduates to stable.
:::

## How the service works

When you deploy the gRPC facade with a KubeRay RayService, the `modal` Python client connects to Ray Serve's gRPC proxy over HTTP/2 on port 9000 and authenticates each request with a shared bearer token. The proxy routes incoming `ModalClient` and `TaskCommandRouter` RPCs to `RaySandboxGrpcFacade`, a single-replica Ray Serve deployment built by `ray.experimental.sandbox.http.grpc_app:build_app`.

The facade translates Modal SDK calls into Ray Sandbox operations and tracks active command executions in memory. For each sandbox, the facade schedules a named, detached `SandboxHost` actor on a worker Pod, and that actor manages one gVisor container through `SandboxRuntime`.

## Prerequisites

Make sure you have the following tools and packages before you start:

* `kubectl` installed and configured.
* `gcloud` CLI installed and authenticated to your Google Cloud project.
* [Helm](https://helm.sh/) v3 installed.
* Docker installed locally, or access to Google Cloud Build, to build the container image.
* Python 3.10 or later and the `modal` client package, installed with `pip install modal`, on your local machine.

## Step 1: Create a GKE cluster

Set environment variables for your Google Cloud project, region, and cluster name:

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export CLUSTER_NAME=ray-modal-sandbox-cluster
```

Create a GKE cluster with standard Linux worker nodes. Because gVisor runs inside the Ray container in rootless user space, standard GKE node pools with the `containerd` runtime work without nested virtualization or special node images:

```bash
gcloud container clusters create "$CLUSTER_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --machine-type=e2-standard-16 \
    --num-nodes=3
```

Fetch credentials for `kubectl`:

```bash
gcloud container clusters get-credentials "$CLUSTER_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID"
```

## Step 2: Install the KubeRay operator

Follow {ref}`Deploy a KubeRay operator <kuberay-operator-deploy>` to install the KubeRay operator from the Helm repository:

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.0
```

Verify that the operator Pod is running:

```bash
kubectl get pods -l app.kubernetes.io/name=kuberay-operator
```

## Step 3: Build and push a Ray image with sandbox dependencies

Every worker Pod that runs sandboxes needs four tools on its `$PATH`:

* `runsc` and `gvisor-bin/`: The gVisor Open Container Initiative (OCI) runtime and its helpers.
* `mkfs.erofs`: Version 1.7 or later from `erofs-utils`, which Ray uses to cache container images as Enhanced Read-Only File System (EROFS) root filesystems.
* `slirp4netns`: The user-mode networking tool that provides private network namespaces for sandboxes with `network="public"`, which is the default when creating sandboxes from the Modal SDK.
* `nft`: The `nftables` CLI, which Ray uses to filter outbound traffic from `public` sandboxes.

Create a `Dockerfile` that adds these tools to the Ray image. Ray's Ubuntu 22.04 base images ship an older `erofs-utils` package, so the first stage compiles `erofs-utils` from source:

```dockerfile
ARG GVISOR_VERSION=20260921.0
ARG SLIRP4NETNS_VERSION=1.3.5
ARG EROFS_UTILS_VERSION=1.9.4

FROM rayproject/ray:nightly-py312 AS build
ARG GVISOR_VERSION
ARG SLIRP4NETNS_VERSION
ARG EROFS_UTILS_VERSION

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends autoconf automake bzip2 \
        ca-certificates curl gcc libtool liblz4-dev make pkg-config uuid-dev

# Download gVisor and its helper binaries.
RUN mkdir -p /out/bin \
    && curl -fsSL "https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/$(uname -m)/gvisor.tar.bz2" \
        | tar -xj -C /out/bin

# Download slirp4netns for network="public" sandboxes.
RUN curl -fsSL -o /out/bin/slirp4netns \
        "https://github.com/rootless-containers/slirp4netns/releases/download/v${SLIRP4NETNS_VERSION}/slirp4netns-$(uname -m)" \
    && chmod a+rx /out/bin/slirp4netns

# Build erofs-utils 1.7 or later from source.
RUN curl -fsSL "https://github.com/erofs/erofs-utils/archive/refs/tags/v${EROFS_UTILS_VERSION}.tar.gz" \
        | tar -xz -C /tmp \
    && cd "/tmp/erofs-utils-${EROFS_UTILS_VERSION}" \
    && ./autogen.sh \
    && ./configure --disable-fuse --prefix=/out \
    && make -j"$(nproc)" \
    && make install

FROM rayproject/ray:nightly-py312
COPY --from=build /out/bin/ /usr/local/bin/

# Install nftables for outbound traffic filtering in network="public" sandboxes.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends nftables \
    && rm -rf /var/lib/apt/lists/*
USER ray
```

Create an Artifact Registry repository in your Google Cloud project and build the image with Cloud Build:

```bash
export REPO_NAME=ray-sandbox
export IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/ray-sandbox:nightly-py312"

gcloud artifacts repositories create "$REPO_NAME" \
    --repository-format=docker \
    --location="$REGION" \
    --project="$PROJECT_ID"

gcloud builds submit --tag "$IMAGE_URI" .
```

:::{note}
The preceding `Dockerfile` targets nodes with 4 KiB kernel pages, which standard x86_64 GKE node pools use. For nodes whose kernel uses 64 KiB pages, build `runsc` and `erofs-utils` with 64 KiB page support as shown in {ref}`Ray Sandboxes requirements <ray-sandbox-requirements>`.
:::

## Step 4: Create the API token Secret and deploy the RayService

The gRPC facade requires an API token in the `RAY_SANDBOX_API_TOKEN` environment variable. Both `build_app` and the `RaySandboxGrpcFacade` replica refuse to start without it because Ray Serve's proxies listen on every Pod's network address.

Generate a random token and store it in a Kubernetes Secret:

```bash
export RAY_SANDBOX_API_TOKEN=$(openssl rand -hex 32)
kubectl create secret generic ray-sandbox-token \
    --from-literal=api-token="$RAY_SANDBOX_API_TOKEN"
```

Create a `ray-service.modal-sandbox.yaml` file with the following manifest. The apply command at the end of this step replaces the `<your-image-uri>` placeholder with the `$IMAGE_URI` you built in Step 3:

```yaml
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: rayservice-modal-sandbox
spec:
  serveConfigV2: |
    grpc_options:
      port: 9000
      grpc_servicer_functions:
        - ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server
    applications:
      - name: sandbox-facade
        route_prefix: /
        import_path: ray.experimental.sandbox.http.grpc_app:build_app
        args:
          advertise_url: http://127.0.0.1:9000
          max_ttl_seconds: 86400
  rayClusterConfig:
    rayVersion: '2.58.0'
    headGroupSpec:
      rayStartParams:
        num-cpus: "0"
      template:
        spec:
          automountServiceAccountToken: false
          containers:
            - name: ray-head
              image: <your-image-uri>
              imagePullPolicy: IfNotPresent
              env:
                - name: RAY_SANDBOX_API_TOKEN
                  valueFrom:
                    secretKeyRef:
                      name: ray-sandbox-token
                      key: api-token
              ports:
                - containerPort: 6379
                  name: gcs-server
                - containerPort: 8265
                  name: dashboard
                - containerPort: 10001
                  name: client
                - containerPort: 8000
                  name: serve
                - containerPort: 9000
                  name: serve-grpc
              resources:
                limits:
                  cpu: "4"
                  memory: "8Gi"
                requests:
                  cpu: "2"
                  memory: "4Gi"
    workerGroupSpecs:
      - groupName: sandbox-workers
        replicas: 2
        minReplicas: 1
        maxReplicas: 5
        rayStartParams: {}
        template:
          spec:
            automountServiceAccountToken: false
            containers:
              - name: ray-worker
                image: <your-image-uri>
                imagePullPolicy: IfNotPresent
                env:
                  - name: RAY_SANDBOX_API_TOKEN
                    valueFrom:
                      secretKeyRef:
                        name: ray-sandbox-token
                        key: api-token
                ports:
                  - containerPort: 8000
                    name: serve
                  - containerPort: 9000
                    name: serve-grpc
                securityContext:
                  seccompProfile:
                    type: Unconfined
                  appArmorProfile:
                    type: Unconfined
                resources:
                  limits:
                    cpu: "8"
                    memory: "16Gi"
                  requests:
                    cpu: "4"
                    memory: "8Gi"
                volumeMounts:
                  - name: dev-net-tun
                    mountPath: /dev/net/tun
            volumes:
              - name: dev-net-tun
                hostPath:
                  path: /dev/net/tun
                  type: CharDevice
```

Key parts of this configuration include the following:

* `grpc_options`: Starts Ray Serve's gRPC proxy on port 9000 and registers `ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server`, which exposes the `modal.client.ModalClient` and `modal.task_command_router.TaskCommandRouter` gRPC services.
* `import_path` and `args`: Deploys `ray.experimental.sandbox.http.grpc_app:build_app` at `/` as the sole application on the Serve instance. Setting `advertise_url` to `http://127.0.0.1:9000` tells the facade to return that local URL as the command-router endpoint when you connect through `kubectl port-forward`.
* `serve` and `serve-grpc` container ports: KubeRay uses the `serve` port 8000 to check the health of Ray Serve's proxy and exposes any port named `serve` or prefixed with `serve-` on the `rayservice-modal-sandbox-serve-svc` Kubernetes Service.
* `num-cpus: "0"` on the head Pod: Prevents Ray from scheduling `SandboxHost` actors onto the head Pod, keeping untrusted sandboxes on worker Pods.
* `securityContext` and `/dev/net/tun` on worker Pods: Sets `seccompProfile` and `appArmorProfile` to `Unconfined` so rootless `runsc` and `slirp4netns` can create user and network namespaces inside the worker container, and mounts `/dev/net/tun` from the node so `slirp4netns` can attach network interfaces for `network="public"` sandboxes.

Apply the manifest:

```bash
sed "s|<your-image-uri>|${IMAGE_URI}|g" ray-service.modal-sandbox.yaml | kubectl apply -f -
```

## Step 5: Verify the RayService deployment

Wait until the RayCluster's Pods are running and the RayService reports `Ready`:

```bash
kubectl get pods -l ray.io/is-ray-node=yes
kubectl get rayservice rayservice-modal-sandbox
```

Verify that KubeRay created the Serve Service with both the `serve` port 8000 and the `serve-grpc` port 9000:

```bash
kubectl get service rayservice-modal-sandbox-serve-svc
```

Forward local port 9000 to the `serve-grpc` port on `rayservice-modal-sandbox-serve-svc`:

```bash
kubectl port-forward svc/rayservice-modal-sandbox-serve-svc 9000:9000
```

## Step 6: Run sandboxes with the Modal Python SDK

Open a second terminal and configure the Modal SDK environment variables:

```bash
export MODAL_SERVER_URL=http://127.0.0.1:9000
export MODAL_SANDBOX_V2=0
export RAY_SANDBOX_API_TOKEN=$(kubectl get secret ray-sandbox-token -o jsonpath='{.data.api-token}' | base64 -d)
```

:::{important}
Set `MODAL_SANDBOX_V2=0` when using `modal` version 1.6 or later. The 1.6 release changed the default client protocol to a newer sandbox API, whereas the Ray Sandbox gRPC facade implements the original Sandbox API.
:::

Save the following script as `run_modal_sandbox.py`. It authenticates to the facade with `modal.Client.from_credentials`, creates a gVisor-isolated sandbox from `python:3.12-slim`, writes a Python script into the sandbox filesystem, executes it, reads back the output file, and stops the sandbox:

```python
import os
import modal

# Authenticate with any non-empty token ID and the RAY_SANDBOX_API_TOKEN secret.
client = modal.Client.from_credentials(
    "ak-ray",
    os.environ["RAY_SANDBOX_API_TOKEN"],
)

app = modal.App.lookup("ray-sandbox-demo", create_if_missing=True, client=client)

# Create a sandbox with 1 CPU core and 1024 MiB of memory.
sb = modal.Sandbox.create(
    app=app,
    image=modal.Image.from_registry("python:3.12-slim"),
    cpu=1.0,
    memory=1024,
    workdir="/workspace",
    timeout=600,
    client=client,
)

# Write a file into the sandbox using the Modal filesystem API.
script = b"""\
import platform
import sys

summary = (
    f"Python: {sys.version.split()[0]}\\n"
    f"Platform: {platform.platform()}\\n"
)
print(summary, end="")
with open("/workspace/result.txt", "w") as f:
    f.write(summary)
"""
sb.filesystem.write_bytes(script, "/workspace/main.py")

# Execute the script inside the sandbox.
process = sb.exec("python3", "/workspace/main.py")
exit_code = process.wait()
print(f"Exit code: {exit_code}")
print("Stdout:")
print(process.stdout.read(), end="")

# Read the generated file back from the sandbox.
result_bytes = sb.filesystem.read_bytes("/workspace/result.txt")
print("Read from /workspace/result.txt:")
print(result_bytes.decode("utf-8"), end="")

# Terminate the sandbox and release its Ray actor reservation.
sb.terminate()
```

Run the script:

```bash
python3 run_modal_sandbox.py
```

The output is similar to the following. The `Platform` line shows the gVisor kernel inside the sandbox:

```text
Exit code: 0
Stdout:
Python: 3.12.16
Platform: Linux-4.19.0-gvisor-x86_64-with-glibc2.41
Read from /workspace/result.txt:
Python: 3.12.16
Platform: Linux-4.19.0-gvisor-x86_64-with-glibc2.41
```

## Configure sandbox networking

The gRPC facade maps the network parameters of the Modal SDK onto Ray Sandbox network modes and `nftables` egress allowlists.

| Modal SDK setting | Ray Sandbox behavior |
| --- | --- |
| `block_network=True` | Creates a sandbox with `network="none"` and no outbound network access. |
| No network setting | Creates a `network="public"` sandbox with the default `["internet"]` allowlist, which allows public internet addresses and drops traffic to private networks, instance metadata, and the Kubernetes API Service. |
| `outbound_cidr_allowlist=[...]` | Creates a `network="public"` sandbox restricted to the listed Classless Inter-Domain Routing (CIDR) ranges and ports, plus Domain Name System (DNS) queries on port 53. |
| `_experimental_set_outbound_network_policy(...)` | Atomically replaces the egress allowlist of a running `network="public"` sandbox. |

For untrusted code that doesn't need network access, pass `block_network=True`:

```python
import os
import modal

client = modal.Client.from_credentials(
    "ak-ray",
    os.environ["RAY_SANDBOX_API_TOKEN"],
)
app = modal.App.lookup("ray-sandbox-demo", create_if_missing=True, client=client)

sb = modal.Sandbox.create(
    app=app,
    image=modal.Image.from_registry("python:3.12-slim"),
    block_network=True,
    client=client,
)
sb.terminate()
```

For untrusted code that needs to install packages first and then run with restricted or no internet egress, start the sandbox with an HTTPS allowlist and tighten the policy at runtime:

```python
import os
import modal

client = modal.Client.from_credentials(
    "ak-ray",
    os.environ["RAY_SANDBOX_API_TOKEN"],
)
app = modal.App.lookup("ray-sandbox-demo", create_if_missing=True, client=client)

sb = modal.Sandbox.create(
    app=app,
    image=modal.Image.from_registry("python:3.12-slim"),
    outbound_cidr_allowlist=["internet:443"],
    client=client,
)

# Install dependencies over HTTPS.
sb.exec("pip", "install", "--quiet", "requests").wait()

# Restrict the running sandbox to DNS queries only.
sb._experimental_set_outbound_network_policy(outbound_cidr_allowlist=[])
sb.terminate()
```

To block additional internal or public IP ranges across every sandbox on the cluster, set `RAY_SANDBOX_EGRESS_DENY` to a comma-separated list of CIDR blocks in the `ray-worker` container environment. See {ref}`Restrict egress to an allowlist <ray-sandbox-egress-allowlist>` for full details on egress filtering.

## Optional: Expose the service behind a TLS Gateway

The Modal Python SDK accepts plaintext `http://` URLs only when connecting to `localhost` or `127.0.0.1`. To connect from remote machines without port-forwarding, configure a TLS proxy on port 443 in front of `rayservice-modal-sandbox-serve-svc` and remove `advertise_url` from `serveConfigV2`:

```yaml
  serveConfigV2: |
    grpc_options:
      port: 9000
      grpc_servicer_functions:
        - ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server
    applications:
      - name: sandbox-facade
        route_prefix: /
        import_path: ray.experimental.sandbox.http.grpc_app:build_app
        args:
          max_ttl_seconds: 86400
```

When you omit `advertise_url`, the facade inspects the `x-modal-host` header that the client SDK sends on control-plane RPCs and returns `https://<dialed-host>` as the command-router URL.

Keep two proxy requirements in mind when exposing the facade over TLS:

* **Use Envoy Gateway or a proxy that detects HTTP/2 without ALPN**: Through `modal` 1.6.1, the command-router TLS connection from the client SDK omits Application-Layer Protocol Negotiation (ALPN) during the TLS handshake and then sends HTTP/2 frames. Envoy detects HTTP/2 from the connection preface and accepts the connection. `ingress-nginx` rejects it, causing `sb.exec()` and `sb.filesystem` calls to fail after sandbox creation succeeds.
* **Disable stream timeouts for long-running commands**: The client SDK streams file writes and waits for command output over long-lived HTTP/2 streams. On Envoy Gateway, apply a `BackendTrafficPolicy` that disables the default 15-second route timeout:

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: sandbox-facade-streams
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: Gateway
      name: <your-gateway-name>
  timeout:
    http:
      requestTimeout: 0s
      maxStreamDuration: 0s
      connectionIdleTimeout: 1h
```

## Limitations

Keep the following constraints in mind when using the Modal-compatible gRPC facade:

* **Single replica**: The facade stores active command execution records in memory, so `RaySandboxGrpcFacade` always runs with `num_replicas=1`. Setting `num_replicas` greater than one raises a `ValueError`. If the replica restarts, running sandboxes keep running because a detached `SandboxHost` actor hosts each sandbox, and the restart loses only in-flight `exec` handles.
* **Single Serve application**: Keep `sandbox-facade` as the only application in `serveConfigV2`. The Modal client SDK doesn't send Ray Serve's `application` routing metadata header.
* **Prebuilt registry images only**: Use `modal.Image.from_registry("<image>")`. The facade rejects multi-stage `Dockerfile` definitions and server-side build steps such as `.pip_install()` or `.run_commands()`. To pull from private registries such as Artifact Registry, configure credentials on the worker Pods as described in {ref}`Ray Sandboxes <ray-core-sandboxes>`.
* **CIDR allowlists only**: The facade rejects `outbound_domain_allowlist` with `INVALID_ARGUMENT` because Ray Sandboxes filter egress by IP address and port with `nftables` rather than a TLS-inspecting proxy.
* **No interactive standard input**: Writing to `process.stdin` on a running command isn't supported and returns `UNIMPLEMENTED`. Use `sb.filesystem.write_bytes()` to pass input data into a sandbox.

## Clean up

Delete the RayService, Secret, KubeRay operator, and GKE cluster when you finish:

```bash
kubectl delete rayservice rayservice-modal-sandbox
kubectl delete secret ray-sandbox-token
helm uninstall kuberay-operator
gcloud container clusters delete "$CLUSTER_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID"
```

## Next steps

* See {ref}`ray-core-sandboxes` for the full Ray Sandboxes architecture, private registry configuration, and HTTP REST API documentation.
* See {ref}`kuberay-sandboxing` for running Ray Sandboxes directly from a KubeRay RayJob.
* See {ref}`resource-isolation-with-writable-cgroups` to configure cgroup v2 resource limits on GKE.
* See {ref}`kuberay-rayservice` for more information on managing RayService deployments.
