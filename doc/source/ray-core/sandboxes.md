---
myst:
  html_meta:
    description: "Execute untrusted model-generated code and agent tool calls safely with Ray Sandboxes using lightweight gVisor kernel isolation."
---

(ray-core-sandboxes)=

# Ray Sandboxes

Ray Sandboxes use [gVisor](https://gvisor.dev/docs/) to provide lightweight, kernel-isolated execution environments for running untrusted code and agent tool calls safely on Ray clusters.

:::{warning}
Ray Sandboxes (`ray.experimental.sandbox`) is an {ref}`alpha <api-stability-alpha>` library. The API can change or disappear in any release before it graduates to stable.
:::

## Background

The ability to sandbox model-generated code is critical for agentic reinforcement learning (RL) and large language model (LLM) agents. Executing untrusted code directly in Ray worker processes or host environments introduces security and stability risks. Ray Sandboxes solve this challenge by running lightweight, kernel-isolated sandboxes directly on Ray worker nodes using [gVisor](https://gvisor.dev/docs/) (`runsc`). Scale and manage sandbox environments with familiar Ray concepts and primitives.

### What is gVisor?

[gVisor](https://gvisor.dev/docs/) is an open-source application kernel written in Go that provides lightweight, defense-in-depth isolation for containers. Developed by Google, gVisor implements a substantial portion of the Linux system call interface in user space, acting as an isolation barrier between untrusted applications and the host operating system kernel.

Unlike standard container runtimes such as Docker or `runc`, where containers share the host Linux kernel directly, gVisor intercepts system calls made by containerized processes before they reach the host. gVisor is daemonless and runs as a non-privileged user, so you can deploy and manage it on top of existing container orchestrators such as Kubernetes.

### Why gVisor?

Untrusted code interacts with gVisor's user-space kernel rather than the host Linux kernel, which shrinks the attack surface for host kernel vulnerabilities and container breakout exploits. gVisor also runs entirely in user space, without host root privileges, the Docker daemon, or nested virtualization hardware extensions, so it runs inside existing Kubernetes Ray worker Pods and cloud container environments.

The runtime cost is low next to full virtual machines (VMs) and MicroVMs, which boot a guest OS kernel and manage heavy disk images. A gVisor sandbox boots in tens of milliseconds, adds minimal memory overhead, and uses near-zero idle CPU, so Ray worker nodes can densely pack hundreds of concurrent sandboxes alongside standard Ray tasks and actors and sustain the high-frequency execution loops that RL rollouts and agent tool calls need.

## Requirements

Ray Sandboxes need the following on every Ray node that runs a sandbox:

* **Linux**: x86_64 or arm64.
* **gVisor (`runsc`)**: Install the `runsc` binary on worker nodes and make it reachable from the system `$PATH`.
* **Ray**: version 2.58.0 or later, which includes the `ray.experimental.sandbox` package.
* **pasta (`network="public"` only)**: The [passt](https://passt.top) package's `pasta` binary on the `$PATH`, plus `/dev/net/tun` in the worker's environment. pasta bridges each sandbox's private network namespace to the node.

To install `runsc` on a Linux worker node, see the [gVisor installation guide](https://gvisor.dev/docs/user_guide/install/). `pasta` ships as the `passt` package on Debian 12+/Ubuntu 23.04+ and Fedora, or as a [static build](https://passt.top/builds/latest/) (x86_64 only; on arm64 use the distro package or build from source — passt has no build dependencies).

## Usage patterns and examples

### Create a basic sandbox and run a command

Use `sandbox.create()` to start an isolated environment from any container image. The function returns a Ray `ActorHandle` representing the sandbox actor.

```python
import ray
from ray.experimental import sandbox

ray.init()

# Create a sandbox with 1 CPU core and 512 MiB RAM
sb = sandbox.create(
    image="python:3.10-slim",
    cpu=1.0,
    memory="512Mi",
    workdir="/workspace",
    timeout_seconds=30.0,
)

# Execute untrusted Python code inside the sandbox
result = ray.get(
    sb.exec.remote("python3 -c 'import sys; print(\"Hello from sandboxed Python:\", sys.version)'")
)

print(f"Exit Code: {result.exit_code}")
print(f"Stdout: {result.stdout.strip()}")
print(f"Execution Duration: {result.duration_ms:.2f} ms")

# Clean up sandbox resources
ray.get(sb.delete.remote())
```

### Read, write, upload, and download files

Write source files directly into the sandbox, or upload local files from the host before execution. By default, the root filesystem is read-only and the configured `workdir`, such as `/workspace`, is the writable scratch space.

```python
import textwrap
import ray
from ray.experimental import sandbox

ray.init()

sb = sandbox.create(
    image="python:3.10-slim",
    workdir="/workspace",
    memory="1Gi",
)

# 1. Write untrusted model-generated script into the sandbox
code = textwrap.dedent("""\
    def fibonacci(n):
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a

    with open('/workspace/output.txt', 'w') as f:
        f.write(f"fib(30) = {fibonacci(30)}")
""")
ray.get(sb.write_file.remote("/workspace/solution.py", code))

# 2. Execute the script inside the sandbox
exec_res = ray.get(sb.exec.remote("python3 /workspace/solution.py"))
print("Execution returncode:", exec_res.exit_code)

# 3. Read generated output file back to the host
output_bytes = ray.get(sb.read_file.remote("/workspace/output.txt"))
print("Result:", output_bytes.decode("utf-8"))

# 4. Alternatively, use upload_file and download_file for host files
# ray.get(sb.upload_file.remote("local_input.json", "/workspace/input.json"))
# ray.get(sb.download_file.remote("/workspace/output.txt", "local_output.txt"))

ray.get(sb.delete.remote())
```

### Schedule a Sandbox actor with custom resources

Because `Sandbox` is a standard Ray actor, you can instantiate it directly with Ray actor scheduling options such as `num_cpus`, `memory`, and custom accelerator or placement constraints.

```python
import ray
from ray.experimental.sandbox import Sandbox

ray.init()

# Instantiate Sandbox actor with Ray Core resource placement options
sandbox_actor = Sandbox.options(
    num_cpus=2.0,
    memory=2 * 1024 * 1024 * 1024,  # 2 GiB
).remote(
    image="python:3.10-slim",
    workdir="/workspace",
    ttl_seconds=600,  # Automatically terminate after 10 minutes
)

# Run command with a per-command execution timeout
result = ray.get(
    sandbox_actor.exec.remote(
        "python3 -c 'import os; print(\"Worker PID:\", os.getpid())'",
        timeout=5.0,  # 5 second execution timeout
    )
)

print(result.stdout)
ray.get(sandbox_actor.delete.remote())
```

### Manage sandboxes inside custom actors with SandboxRuntime

If you're building custom RL environment actors or specialized rollout workers, embed `SandboxRuntime` directly inside your custom actors for fine-grained sandbox lifecycle control:

```python
import ray
from ray.experimental.sandbox.runtime import SandboxRuntime

@ray.remote
class SandboxPool:
    def __init__(self, size: int = 3, image: str = "python:3.10-slim"):
        self.runtime = SandboxRuntime()
        self.sandboxes = [
            self.runtime.create(image=image, memory="512Mi")
            for _ in range(size)
        ]

    def run_command(self, index: int, command: str):
        return self.runtime.exec(self.sandboxes[index], command)

    def close(self):
        for sb_id in self.sandboxes:
            self.runtime.delete(sb_id)

# Deploy an actor managing a pool of local sandboxes
pool = SandboxPool.remote(size=3)
result = ray.get(pool.run_command.remote(0, "python3 -c 'print(\"Hello from pool!\")'"))
print(result.stdout)
ray.get(pool.close.remote())
```

### Pass custom OCI configurations to gVisor

For advanced workloads, you might need to configure low-level runtime options such as custom host mounts, Linux capabilities, or custom network and DNS settings. Use the `_oci_spec_transform_fn` parameter to inspect and modify the generated [Open Container Initiative (OCI) runtime specification](https://github.com/opencontainers/runtime-spec) dictionary before Ray passes it to gVisor (`runsc`).

:::{note}
`_oci_spec_transform_fn` is an experimental hook for advanced use cases. The Ray project is designing first-class configuration APIs for Ray Sandboxes, such as higher-level volume mount and capability abstractions, and this hook is likely to change once those land. To help shape them, open an issue describing your use case.
:::

The `_oci_spec_transform_fn` callable receives the fully generated OCI specification dictionary. It can mutate the dictionary in place or return a modified one. Common use cases include the following:

* **Host mounts**: Mount host directories, read-only datasets, or model weights into the sandbox container.
* **Namespace and mount details**: Configure namespace or mount behavior that the first-class options don't cover.

Internet access, DNS, and Linux capabilities each have a first-class option: `network`, `dns`, and `capabilities`. Pass `capabilities=[]` to run with no capabilities at all. Reserve the hook for network or capability configurations those options don't reach. See [Networking and DNS](#networking-and-dns).

```python
import ray
from ray.experimental import sandbox

ray.init()


def configure_oci_spec(spec: dict) -> dict:
    # Add a host bind mount (e.g., read-only dataset or cache directory)
    spec.setdefault("mounts", []).append(
        {
            "destination": "/mnt/dataset",
            "source": "/path/to/host/dataset",
            "type": "bind",
            "options": ["rbind", "ro"],
        }
    )

    return spec


# Pass the transformation hook when creating the sandbox
sb = sandbox.create(
    image="python:3.10-slim",
    workdir="/workspace",
    _oci_spec_transform_fn=configure_oci_spec,
)

# Execute commands within the customized sandbox
result = ray.get(
    sb.exec.remote(
        "python3 -c 'print(\"Sandbox initialized with custom OCI configuration!\")'"
    )
)
print(result.stdout)

# Clean up resources
ray.get(sb.delete.remote())
```

## Container images

Sandboxes boot from OCI container images. The image manager pulls an image straight from the registry's HTTP API (anonymously, with no Docker daemon and no credentials), extracts its root filesystem into `/tmp/ray/sandbox/images` on the node, and caches it for reuse by subsequent sandboxes on that node using the same image. Sandboxes with write access to the filesystem get their own private writable overlay on top of the cached root filesystem.

### Route Docker Hub pulls through a mirror

Because image pulls are anonymous, every node pulling from Docker Hub consumes the anonymous pull-rate limit and downloads the image over the WAN. In a large cluster, concurrent pulls of multi-GB images can quickly hit the rate limit or saturate network bandwidth, causing image pulls to fail or become slow.

Set `RAY_SANDBOX_REGISTRY_MIRROR` to route Docker Hub pulls through a registry mirror. Ray rewrites only Docker Hub image references. Pulls from other registries, such as GHCR or a private registry, are left unchanged.

The value is `host[:port][/repo-prefix]`. Ray prepends the repository prefix to the repository path, which is the form pull-through caches expect:

| Mirror | Example value | `python:3.10-slim` resolves to |
| --- | --- | --- |
| [ECR pull-through cache](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html) | `<acct>.dkr.ecr.<region>.amazonaws.com/dockerhub` | `<acct>.dkr.ecr.<region>.amazonaws.com/dockerhub/library/python` |
| [Artifact Registry remote repository](https://cloud.google.com/artifact-registry/docs/repositories/remote-repo) | `<region>-docker.pkg.dev/<project>/<repo>` | `<region>-docker.pkg.dev/<project>/<repo>/library/python` |
| In-cluster [`registry:2`](https://distribution.github.io/distribution/recipes/mirror/) proxy | `http://registry.default.svc.cluster.local:5000` | `http://registry.default.svc.cluster.local:5000/library/python` |

Keep the following in mind:

* **A bare host means HTTPS.** Write an explicit `http://` prefix for a plain-HTTP mirror, which an in-cluster `registry:2` proxy typically is.
* **The mirror is authoritative.** Unlike Docker's registry-mirrors behavior, Ray does not fall back to Docker Hub. If the mirror is unreachable or does not contain the image, the pull fails.
* **The mirror must allow anonymous pulls.** Ray talks to a mirror exactly as it talks to any registry, over the same anonymous bearer-token flow. If your mirror normally requires authentication, expose it to Ray through network-level access instead, such as a VPC endpoint or cluster-internal service.

## Networking and DNS

Sandboxes support four network modes. The default is `none`, which follows the safe-defaults principle. Use `public` when a sandbox needs internet access.

| Mode | Network access | `/etc/resolv.conf` | Security property |
| --- | --- | --- | --- |
| `none` *(default)* | None | untouched | No egress. This is the recommended setting for untrusted code. |
| `public` | Internet egress from a network namespace private to the sandbox, bridged by [pasta](https://passt.top) | Generated from `dns` (default `8.8.8.8`, `1.1.1.1`), mounted read-only | Ports and loopback are per-sandbox: a bind on `0.0.0.0` can't collide with, be reached by, or reach other sandboxes or node-local services, and there's no inbound path from the node or cluster. The sandbox inherits nothing from the host's resolver configuration. The sandbox can still reach any network address the node can reach, including other Ray nodes and internal services. Requires `pasta` on the node. |
| `host` | Full host network identity | Host's own file, mounted read-only (`dns=` overrides it) | Strictly more permissive than `public`. The sandbox can reach anything the node can reach, including internal networks and node-local services. |
| `sandbox` | gVisor netstack | untouched | Requires `rootless=False`. runsc doesn't support the sandbox netstack in rootless mode. |

:::{warning}
`public` isolates sandboxes from each other and from the node's own services, not from the network the node sits on. pasta relays every outbound connection through the node, so a `public` sandbox can reach other Ray nodes, including the head node's GCS and dashboard ports, other Kubernetes Pods, and any internal service the node can reach. Use `network="none"` for untrusted code.
:::

To give a sandbox internet access, use `network="public"`. Pair it with `DOCKER_DEFAULT_CAPABILITIES` so standard images behave the way they do under Docker, because `apt-get`, `tar` ownership restore, and similar operations all need those capabilities:

```python
from ray.experimental import sandbox
from ray.experimental.sandbox import DOCKER_DEFAULT_CAPABILITIES

sb = sandbox.create(
    image="python:3.10-slim",
    network="public",
    capabilities=DOCKER_DEFAULT_CAPABILITIES,
    readonly=False,
)
```

### DNS in locked-down networks

Some virtual private clouds (VPCs) block outbound port 53 to public resolvers, so the default `public` DNS settings can't resolve queries. Pass your internal resolver instead with `network="public", dns=["10.0.0.2"]`. If that isn't an option, fall back to `network="host"`, which uses the host's `/etc/resolv.conf`, at the cost of full host network identity. Configure anything beyond that through the OCI spec. See [Pass custom OCI configurations to gVisor](#pass-custom-oci-configurations-to-gvisor).

## Architecture

The Ray Sandboxes subsystem has the following layers:

```text
+-------------------------------------------------------------------+
|               Ray Application / RL Framework                      |
|           (e.g., veRL, SkyRL, RL Rollout Workers, Agents)         |
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|                      ray.experimental.sandbox                     |
|           (High-level create() API & Sandbox Ray Actor)           |
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|                  ray.experimental.sandbox.runtime                 |
|                      SandboxRuntime Interface                     |
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|                 ray.experimental.sandbox.backend                  |
|               GVisorSandboxBackend (runsc OCI)                    |
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|                        Ray Worker Node                            |
|   +-----------------------+       +-----------------------+       |
|   |  gVisor Sandbox 1     |       |  gVisor Sandbox 2     |       |
|   | (python:3.10-slim)    |       | (busybox:latest)      |       |
|   |   CPU: 0.5, Mem: 256M |       |   CPU: 1.0, Mem: 512M |       |
|   +-----------------------+       +-----------------------+       |
+-------------------------------------------------------------------+
```

### Core components

* **High-level helper ({func}`~ray.experimental.sandbox.create`)**: Spawns a Ray actor that encapsulates the sandbox lifecycle and returns an `ActorHandle`.
* **Sandbox actor ({class}`~ray.experimental.sandbox.Sandbox`)**: A Ray actor that serves as a proxy to forward command execution and file I/O to the isolated sandbox instance while managing the scheduling and lifecycle of the sandbox.
* **Sandbox runtime ({class}`~ray.experimental.sandbox.SandboxRuntime`)**: A low-level abstraction that manages the lifecycle of local sandboxes, image pulling and caching, and interactions with the execution backend.
* **gVisor backend (`ray.experimental.sandbox.backend.GVisorSandboxBackend`)**: Executes commands and isolates processes through gVisor's OCI runtime (`runsc`).
* **Image manager (`ray.experimental.sandbox.image_manager.ImageManager`)**: Automatically pulls container images from sources such as Docker Hub, GHCR, or local tar archives, extracts root filesystems into `/tmp/ray/sandbox/images`, and builds OCI `config.json` runtime specifications.

## Security and isolation model

Ray Sandboxes implement multi-layered defense-in-depth isolation:

* **System call interception**: gVisor's Sentry application kernel intercepts system calls in user space, isolating untrusted code from the host Linux kernel.
* **Read-only root filesystem**: Ray mounts base container filesystems read-only (`readonly=True`) with an isolated copy-on-write overlay directory per sandbox.
* **Restricted working directory**: Only the explicit `workdir`, such as `/workspace`, is mounted read-write for application artifacts.
* **Network containment**: By default, `network="none"` disables all outbound network interfaces, which prevents untrusted code from making external API calls or scanning the internal cluster network. When internet access is needed, `network="public"` grants egress without handing over the host's resolver configuration or network identity; see [Networking and DNS](#networking-and-dns).
* **Resource quotas**: cgroups enforce CPU quotas and memory limits, which prevents CPU starvation and out-of-memory (OOM) conditions from affecting other Ray actors.

## API reference

For detailed signatures, parameters, and return types, see {ref}`ray-sandbox-ref`.

## Troubleshooting

* **`runsc` not found in `$PATH`**: Verify that gVisor's `runsc` binary is installed on all Ray worker nodes and sits in a directory on the system `$PATH`, such as `/usr/local/bin/runsc`.
* **cgroup or permission errors**: In containerized environments such as Kubernetes without root permissions, keep the default `rootless=True`. Where cgroups are restricted, set `RAY_SANDBOX_IGNORE_CGROUPS=1`.
* **Image pull failures**: Verify that the node can reach the container registry, such as Docker Hub or GHCR, or pre-populate the image cache directory at `/tmp/ray/sandbox/images`. When many nodes pull large images at once, Docker Hub's anonymous rate limits are a likely cause; see [Route Docker Hub pulls through a mirror](#route-docker-hub-pulls-through-a-mirror).
* **`pasta` not found for `network="public"`**: Install the passt package (or a [static build](https://passt.top/builds/latest/)) on worker nodes.
* **`public` sandboxes fail to start with a tap or namespace error**: pasta needs `/dev/net/tun` in the worker's environment and a seccomp policy that allows unprivileged user+network namespace creation (`unshare -Un true` must succeed as the Ray user). The pasta error appears in the sandbox's `runsc.stderr.log` and in the creation error message.

## Next steps

* See {ref}`kuberay-sandboxing` to deploy Ray Sandboxes on Kubernetes with KubeRay.
* Learn more about [gVisor](https://gvisor.dev/docs/).
* Explore {ref}`resource-isolation` to isolate Ray system processes from worker processes.
