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
* **erofs-utils**: `mkfs.erofs` 1.7 or later on the `$PATH`. Ray caches each image as an EROFS file that gVisor mounts inside its own kernel, so files in the sandbox keep the image's real owners and `chown` works for any uid, with no privileges or id mappings on the node. Sandbox creation fails without it.
* **Overlayfs root filesystems**: Ray boots some sandboxes from a kernel overlayfs over their cached EROFS image, based on each sandbox's final OCI spec. Every worker needs `unshare`, `mount`, `mountpoint`, `flock`, and `setpriv` from util-linux for this. Workers without mount privileges also need `erofsfuse`, and so do workers with them whose kernel lacks the `erofs` driver or free loop devices. Workers without mount privileges that run as a user other than root need the `uidmap` package too. For details, see [Overlayfs root filesystems](#overlayfs-root-filesystems).
* **slirp4netns (`network="public"` only)**: The [slirp4netns](https://github.com/rootless-containers/slirp4netns) binary on the `$PATH`, plus `/dev/net/tun` in the worker's environment. slirp4netns bridges each sandbox's private network namespace to the node.

To install `runsc` on a Linux worker node, see the [gVisor installation guide](https://gvisor.dev/docs/user_guide/install/). gVisor's prebuilt `runsc` only supports 4 KiB pages, so on nodes whose kernel uses 64 KiB pages, build it from source with `--define=pagesize=64k`. `slirp4netns` ships as a package on Debian, Ubuntu, and Fedora, or as a [static build](https://github.com/rootless-containers/slirp4netns/releases) for x86_64 and aarch64. On Ubuntu 24.04 or Debian 13 with a kernel page size of 4 KiB, install the `erofs-utils` and `erofsfuse` packages from the distribution's repositories. On Ubuntu 22.04, the `erofs-utils` package is too old, so you need to build a release from the [erofs-utils repository](https://github.com/erofs/erofs-utils) instead. Install `autoconf`, `automake`, `libtool`, `pkg-config`, `liblz4-dev`, `uuid-dev`, and `libfuse3-dev`, then run `./autogen.sh && ./configure --enable-fuse && make && make install`. The resulting `erofsfuse` also needs the FUSE runtime library, `libfuse3-3` on Ubuntu 22.04. On nodes whose kernel uses 64 KiB pages, you need to build from source in all cases (regardless of distribution). Follow the instructions above, but additionally pass `MAX_BLOCK_SIZE=65536` to `./configure`. This is required because `runsc` only accepts EROFS images whose block size is a multiple of the node's page size, and distro packages build with 4 KiB blocks. Workers that run as a user other than root without mount privileges also need the `uidmap` package, as [Overlayfs root filesystems](#overlayfs-root-filesystems) describes.

The following Dockerfiles add everything sandboxes need to a Ray image, including the tools that overlayfs root filesystems need. Pick the tab that matches your nodes' kernel page size. To check it, run `getconf PAGESIZE` on a node, which prints `4096` for 4 KiB pages and `65536` for 64 KiB pages. Each Dockerfile builds its tools in a separate stage, so the build tools stay out of the final image.

::::{tab-set}

:::{tab-item} 4 KiB pages
This Dockerfile downloads gVisor's prebuilt `runsc`, which supports 4 KiB pages, and builds erofs-utils from source. Ray's images are based on Ubuntu 22.04, whose packaged `erofs-utils` is too old. On an Ubuntu 24.04 or Debian 13 base image, drop the erofs-utils step and its build dependencies from the build stage, and change the final stage's package list to `erofs-utils erofsfuse uidmap`. The `erofsfuse` package pulls in the libfuse3 runtime library.

```dockerfile
ARG GVISOR_VERSION=20260921.0
ARG SLIRP4NETNS_VERSION=1.3.5
ARG EROFS_UTILS_VERSION=1.9.4

FROM rayproject/ray:latest AS build
ARG GVISOR_VERSION
ARG SLIRP4NETNS_VERSION
ARG EROFS_UTILS_VERSION

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends autoconf automake bzip2 \
        ca-certificates curl gcc libfuse3-dev libtool liblz4-dev make pkg-config \
        uuid-dev

# gVisor, with the gvisor-bin/ helpers that must stay next to runsc
RUN mkdir -p /out/bin \
    && curl -fsSL "https://storage.googleapis.com/gvisor/releases/release/${GVISOR_VERSION}/$(uname -m)/gvisor.tar.bz2" \
        | tar -xj -C /out/bin

# slirp4netns, for network="public"
RUN curl -fsSL -o /out/bin/slirp4netns \
        "https://github.com/rootless-containers/slirp4netns/releases/download/v${SLIRP4NETNS_VERSION}/slirp4netns-$(uname -m)" \
    && chmod a+rx /out/bin/slirp4netns

# erofs-utils
RUN curl -fsSL "https://github.com/erofs/erofs-utils/archive/refs/tags/v${EROFS_UTILS_VERSION}.tar.gz" \
        | tar -xz -C /tmp \
    && cd "/tmp/erofs-utils-${EROFS_UTILS_VERSION}" \
    && ./autogen.sh \
    && ./configure --enable-fuse --prefix=/out \
    && make -j"$(nproc)" \
    && make install

FROM rayproject/ray:latest
COPY --from=build /out/bin/ /usr/local/bin/

# Install libfuse3 for erofsfuse, and newuidmap and newgidmap from uidmap to
# map the Ray user's subordinate ids on workers without mount privileges.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfuse3-3 uidmap \
    && rm -rf /var/lib/apt/lists/*
USER ray
```
:::

:::{tab-item} 64 KiB pages
This Dockerfile builds both `runsc` and erofs-utils from source. gVisor's prebuilt `runsc` only supports 4 KiB pages, so this Dockerfile builds `runsc` with `--define=pagesize`. `runsc` also only accepts EROFS images whose block size is a multiple of the node's page size, and distribution packages build erofs-utils with 4 KiB blocks, so it passes `MAX_BLOCK_SIZE` to erofs-utils' `./configure` too. Both come from `PAGE_SIZE`, the page size of the nodes the image runs on, which defaults to 65536. To change it, pass `--build-arg PAGE_SIZE=<bytes>`.

```dockerfile
ARG GVISOR_VERSION=20260921.0
ARG SLIRP4NETNS_VERSION=1.3.5
ARG EROFS_UTILS_VERSION=1.9.4
ARG PAGE_SIZE=65536

FROM rayproject/ray:latest AS build
ARG GVISOR_VERSION
ARG SLIRP4NETNS_VERSION
ARG EROFS_UTILS_VERSION
ARG PAGE_SIZE

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends autoconf automake \
        build-essential bzip2 ca-certificates clang curl git \
        g++-aarch64-linux-gnu g++-x86-64-linux-gnu gcc-aarch64-linux-gnu \
        gcc-x86-64-linux-gnu libbpf-dev libfuse3-dev liblz4-dev libtool pkg-config \
        python3 uuid-dev

# gVisor, with the gvisor-bin/ helpers that must stay next to runsc
RUN curl -fsSL -o /usr/local/bin/bazelisk \
        "https://github.com/bazelbuild/bazelisk/releases/download/v1.29.0/bazelisk-linux-$(dpkg --print-architecture)" \
    && chmod +x /usr/local/bin/bazelisk \
    && git clone --depth 1 --branch "release-${GVISOR_VERSION}" \
        https://github.com/google/gvisor.git /tmp/gvisor \
    && cd /tmp/gvisor \
    && bazelisk build -c opt --define=pagesize="$((PAGE_SIZE / 1024))k" \
        //debian:gvisor-release-tar-bz2 \
    && mkdir -p /out/bin \
    && tar -xjf bazel-bin/debian/gvisor.tar.bz2 -C /out/bin

# slirp4netns, for network="public"
RUN curl -fsSL -o /out/bin/slirp4netns \
        "https://github.com/rootless-containers/slirp4netns/releases/download/v${SLIRP4NETNS_VERSION}/slirp4netns-$(uname -m)" \
    && chmod a+rx /out/bin/slirp4netns

# erofs-utils
RUN curl -fsSL "https://github.com/erofs/erofs-utils/archive/refs/tags/v${EROFS_UTILS_VERSION}.tar.gz" \
        | tar -xz -C /tmp \
    && cd "/tmp/erofs-utils-${EROFS_UTILS_VERSION}" \
    && ./autogen.sh \
    && ./configure --enable-fuse --prefix=/out MAX_BLOCK_SIZE="$PAGE_SIZE" \
    && make -j"$(nproc)" \
    && make install

FROM rayproject/ray:latest
COPY --from=build /out/bin/ /usr/local/bin/

# Install libfuse3 for erofsfuse, and newuidmap and newgidmap from uidmap to
# map the Ray user's subordinate ids on workers without mount privileges.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfuse3-3 uidmap \
    && rm -rf /var/lib/apt/lists/*
USER ray
```
:::

::::

To give sandboxes GPU access, also install `nvidia-container-toolkit-base` on GPU worker nodes. This package provides `nvidia-ctk`, which Ray uses to generate a [CDI](https://github.com/cncf-tags/container-device-interface) spec. Each worker process runs `nvidia-ctk cdi generate` once, when it creates its first GPU sandbox, and reuses the result.

To install `nvidia-container-toolkit-base`, see the [NVIDIA Container Toolkit installation guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).

:::{note}
* `nvidia-container-toolkit-base` must be version 1.20.1 or later.
* gVisor is only supported on NVIDIA driver versions it explicitly recognizes; check yours with `runsc nvproxy list-supported-drivers`.
* MIG isn't supported, since gVisor itself doesn't support it.
:::

`nvidia-container-toolkit-base` installs a file at `/etc/nvidia-container-toolkit/nvidia-cdi-refresh.env` to customize the environment variables `nvidia-ctk cdi generate` uses to generate its CDI spec. This file uses the [systemd `EnvironmentFile=` format](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#EnvironmentFile=). Each line is a plain `KEY=value` pair, and a line starting with `#` or `;` is a comment. For example, GKE nodes need `NVIDIA_CTK_DRIVER_ROOT` set there because their NVIDIA driver installer DaemonSet places driver libraries under `/usr/local/nvidia` instead of `/`. They also need `NVIDIA_CTK_DEV_ROOT` set to `/`, because `nvidia-ctk cdi generate` otherwise assumes device nodes live under the driver root too. Ray reads this file, if it exists, when it generates the spec. To use a different file, set `NVIDIA_CTK_ENV_PATH` in the worker's environment. Changes to the file, the driver, or the toolkit take effect in new worker processes.

```bash
# /etc/nvidia-container-toolkit/nvidia-cdi-refresh.env
NVIDIA_CTK_DRIVER_ROOT=/usr/local/nvidia
NVIDIA_CTK_DEV_ROOT=/
```

:::{note}
Ray always parses `nvidia-ctk cdi generate`'s stdout, so it clears `NVIDIA_CTK_CDI_OUTPUT_FILE_PATH` after reading this file even if you set it there.
:::

See [GPU access](#gpu-access).

### Overlayfs root filesystems

By default, a sandbox boots straight from its cached EROFS image, which gVisor mounts inside its own kernel. Nothing on the host can write to that root filesystem before the sandbox starts. If the sandbox is read-only, runsc also can't create the mount points its mounts need but the image lacks, because the EROFS image is immutable and runsc doesn't layer a writable overlay over a read-only root. To compensate for this, Ray may decide to boot the sandbox from an overlayfs root filesystem instead. Ray mounts the cached EROFS image on the host and layers a kernel overlayfs on top of it, which becomes the sandbox's root filesystem. Ray does this in either of two cases, both decided from the sandbox's final OCI spec.

The first case is a spec with `prestart`, `createRuntime`, or `createContainer` hooks. A Container Device Interface (CDI) spec for a GPU, for example, adds such hooks. They run on the host because they need access to it, but they modify files in the sandbox's root filesystem. On an overlayfs root filesystem, those changes stay private to the sandbox and never reach the cached EROFS image that other sandboxes share.

The second case is a read-only sandbox whose mounts need mount points the image might lack, such as an explicit `workdir` or the libraries a CDI spec mounts. runsc creates those mount points in the overlayfs root filesystem, and the sandbox still sees a read-only root.

Any sandbox can end up in one of these cases, so every node that runs sandboxes needs to install the necessary tools to support overlayfs root filesystems. A worker with mount privileges mounts them directly, with the kernel's `erofs` driver on a loop device, or with `erofsfuse` if that fails. A worker without them mounts them with `erofsfuse` in a new user namespace for each sandbox, which needs Linux 5.11 or later. A worker running as root maps every id to itself in that namespace, so files keep their owners from the image. A worker running as any other user needs subordinate uids and gids for the Ray user to keep them, at least 65536 of each. Ray maps them with `newuidmap` and `newgidmap` from the `uidmap` package.

On a worker that can't mount an overlayfs root filesystem, a sandbox with such hooks fails to start, and a read-only sandbox with missing mount points runs on gVisor's private writable overlay instead, with a warning. For how to fix this, see [Troubleshooting](#troubleshooting).

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

### GPU access

Because `Sandbox` is a standard Ray actor, you can request GPUs with `num_gpus` the same way you would for any other actor. Unlike `cpu` and `memory`, however, there is no way to further limit the number of GPUs seen by a sandbox once the actor is scheduled. Sandbox support for GPUs requires `nvidia-container-toolkit-base` on the node (see [Requirements](#requirements)) and a gVisor build with `--nvproxy` support.

Sandbox GPU ids number GPUs in NVML's order, which is PCI bus order. CUDA numbers them fastest first by default, so on nodes with mixed GPU models, set `CUDA_DEVICE_ORDER=PCI_BUS_ID` in the Ray node's environment so that other workloads number GPUs the same way.

```python
import ray
from ray.experimental.sandbox import Sandbox

ray.init()

sandbox_actor = Sandbox.options(num_gpus=1).remote(
    image="nvidia/cuda:12.4.0-base-ubuntu22.04",
)

result = ray.get(sandbox_actor.exec.remote("nvidia-smi"))
print(result.stdout)

ray.get(sandbox_actor.delete.remote())
```

You can also create a GPU enabled sandbox inside a pre-existing actor using `SandboxRuntime.create()`. Instead of passing `num_gpus`, you pass a `gpu_ids` field to specify which of the GPUs that have been granted to the surrounding actor should be isolated inside the sandbox. This allows a single actor with multiple GPUs to manage several sandboxes, each pinned to a different GPU.

```python
import ray
from ray.experimental.sandbox.runtime import SandboxRuntime

@ray.remote(num_gpus=2)
class GpuSandboxPool:
    def __init__(self, image: str):
        self.runtime = SandboxRuntime()
        self.sandboxes = {
            gpu_id: self.runtime.create(image=image, gpu_ids=[gpu_id])
            for gpu_id in [str(i) for i in ray.get_gpu_ids()]
        }

    def gpu_ids(self):
        return list(self.sandboxes)

    def exec_on(self, gpu_id: str, command: str):
        return self.runtime.exec(self.sandboxes[gpu_id], command)

    def close(self):
        for sandbox_id in self.sandboxes.values():
            self.runtime.delete(sandbox_id)

pool = GpuSandboxPool.remote(image="nvidia/cuda:12.4.0-base-ubuntu22.04")
gpu_id = ray.get(pool.gpu_ids.remote())[0]
result = ray.get(pool.exec_on.remote(gpu_id, "nvidia-smi"))
print(result.stdout)
ray.get(pool.close.remote())
```

Ray validates `SandboxRuntime.create()`'s `gpu_ids` the same way as a `Sandbox`'s assigned GPUs. A sandbox can only access GPUs Ray assigned to the calling actor or task, and sandbox creation fails if Ray assigned it none.

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

See [GPU access](#gpu-access) above for giving each sandbox in a pool like this its own GPU.

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

Sandboxes boot from OCI container images. The image manager pulls an image straight from the registry's HTTP API (anonymously, with no Docker daemon and no credentials), flattens its layers, and caches the result under `/tmp/ray/sandbox/images` on the node for reuse by subsequent sandboxes on that node using the same image. The cached root filesystem is a single EROFS image, built with `mkfs.erofs`, that gVisor mounts inside the Sentry, which keeps the image's file ownership intact. Sandboxes with write access to the filesystem get their own private writable overlay on top of the cached root filesystem. A `readonly=True` sandbox whose mounts need mount points the image might lack, such as an explicit `workdir`, boots from an overlayfs root filesystem instead, because runsc can't create mount points in a read-only image. On a worker that can't mount one, the sandbox gets a writable root filesystem, and Ray discards its writes with the sandbox. For details, see [Overlayfs root filesystems](#overlayfs-root-filesystems). A cache left by an earlier Ray version, which extracted images into directories, is rebuilt on the next pull.

### Bound the image cache

The cache is bounded so that a node that runs many distinct images doesn't fill its disk. Before each pull, Ray evicts the least recently extracted images until the cache fits under the cap. Images that a running sandbox uses are never evicted. The cap defaults to half of the filesystem that holds the cache. Set `RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES` on worker nodes to choose a cap in bytes, or set it to `0` to disable eviction.

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
| `public` | Internet egress from a network namespace private to the sandbox, bridged by [slirp4netns](https://github.com/rootless-containers/slirp4netns) | Generated from `dns` (default `8.8.8.8`, `1.1.1.1`), mounted read-only | Ports and loopback are per-sandbox: a bind on `0.0.0.0` can't collide with, be reached by, or reach other sandboxes or node-local services, and there's no inbound path from the node or cluster. The sandbox inherits nothing from the host's resolver configuration. The sandbox can still reach any network address the node can reach, including other Ray nodes and internal services. The sandbox's own address is `198.18.0.100` (RFC 2544 benchmarking space, chosen not to overlap pod or service ranges). Requires `slirp4netns` on the node. |
| `host` | Full host network identity | Host's own file, mounted read-only (`dns=` overrides it) | Strictly more permissive than `public`. The sandbox can reach anything the node can reach, including internal networks and node-local services. |
| `sandbox` | gVisor netstack | untouched | Requires `rootless=False`. runsc doesn't support the sandbox netstack in rootless mode. |

:::{warning}
`public` isolates sandboxes from each other and from the node's own services, not from the network the node sits on. slirp4netns relays every outbound connection through the node, so a `public` sandbox can reach other Ray nodes, including the head node's GCS and dashboard ports, other Kubernetes Pods, and any internal service the node can reach. Use `network="none"` for untrusted code.
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

## HTTP API service

Ray Sandbox ships an experimental REST API service so you can manage sandboxes from outside the Ray cluster with nothing but an HTTP client and a bearer token. The service is a FastAPI app on Ray Serve (`ray.experimental.sandbox.http`). Each sandbox is held by a named, detached actor, so the service itself is stateless and its replicas can scale or restart without losing sandboxes.

Image pulls and commands can far outlive an HTTP request and the load balancer in front of a deployed service, so creation and execution are asynchronous. `POST` returns immediately and clients poll, optionally long-polling with `wait_seconds` for up to 30 seconds per request.

### Endpoints

All endpoints sit under `/api/v1`. Except for `GET /health`, they require `Authorization: Bearer <token>` when a token is configured.

| Method and path | Description |
| --- | --- |
| `GET /health` | Liveness probe. Never requires auth. |
| `POST /sandboxes` | Create a sandbox. Returns `202` with `status: pending`. Poll until `running` or `error`. Send a `client_token` to make creation idempotent, so a retry returns `200` with the existing sandbox. |
| `GET /sandboxes?label=k=v` | List sandboxes, optionally filtered by labels. |
| `GET /sandboxes/{id}?wait_seconds=N` | Sandbox status. Long-polls while it boots. |
| `DELETE /sandboxes/{id}` | Terminate the sandbox and its actor. Idempotent from any state. Answers `terminating` instead of `terminated` when the actor is still being scheduled or busy tearing down. It finishes and exits on its own. |
| `POST /sandboxes/{id}/execs` | Start a command. Returns `202` with an `exec_id`, or `409` while the sandbox isn't running. A string command runs under the sandbox's shell, `/bin/bash` by default and configurable per sandbox and per exec via `shell`. A list runs argv-style. |
| `GET /sandboxes/{id}/execs/{exec_id}?wait_seconds=N` | Exec status and result: `running`, `completed` with `exit_code`, `stdout`, and `stderr`, `timeout`, or `error`. Output is capped per stream by `max_output_bytes` with a loud truncation marker. |
| `PUT /sandboxes/{id}/files?path=/abs/path` | Write the raw request body to a file in the sandbox. Returns `413` above `max_file_bytes`, and `409 write_failed` when the sandbox can't write the path, such as a directory or a read-only root filesystem. Pass `append=true` to extend the file, which lets clients chunk large uploads under proxy body-size limits. |
| `GET /sandboxes/{id}/files?path=/abs/path` | Read a file from the sandbox as `application/octet-stream`. |

Errors use a JSON envelope of the form `{"error": {"code": "...", "message": "..."}}`. The codes are `401 unauthorized`, `404 sandbox_not_found`, `404 exec_not_found`, `404 file_not_found`, `409 conflict`, `409 unschedulable`, `409 write_failed`, `400 invalid_request`, `413 payload_too_large`, `503 sandbox_unavailable` for an actor that's briefly unreachable, such as during a restart, and FastAPI's native `422` for schema violations. The full OpenAPI schema is served at `/openapi.json`.

Keep this server behavior in mind:

* **TTL**: Every sandbox gets a TTL that reclaims both the sandbox and its hosting actor. Request it with `ttl_seconds`, capped and defaulted by the server's `max_ttl_seconds`.
* **Resources**: `resources` separates cluster reservations from in-sandbox cgroup caps. `cpu_request`, `memory_request_mb`, and `custom` Ray resources reserve cluster capacity, and custom resources such as `{"gvisor": 1}` pin sandboxes to runsc-equipped nodes. `cpu_limit` and `memory_limit_mb` become cgroup caps. Requests default to the limits.
* **Capabilities**: By default sandboxes get Docker's default Linux capability set so images behave the way they do under Docker. Ray's own default is far narrower and breaks `apt-get` and `tar`. The sets are written exactly, so `capabilities: []` runs the sandbox with no capabilities at all.
* **Network modes**: These are the Python API's modes, which Ray validates: `none` (the default), `public` for egress with generated DNS that `dns` overrides, `host`, and `sandbox`. See [Networking and DNS](#networking-and-dns).

### Self-hosted quickstart

On a Linux machine or cluster with `runsc` on `PATH`:

```bash
pip install "ray[serve]"
export RAY_SANDBOX_API_TOKEN=dev-token   # Optional. Unset disables app-level auth.
serve run ray.experimental.sandbox.http.app:build_app
```

```bash
curl -s -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"image": "busybox:latest", "readonly": false, "shell": "/bin/sh"}' \
  http://localhost:8000/api/v1/sandboxes
```

Builder arguments configure the server. See `ray.experimental.sandbox.http.schemas.SandboxAPISettings` for the full list. For example:

```bash
serve run ray.experimental.sandbox.http.app:build_app max_ttl_seconds=86400 num_replicas=2
```

### Deploying as an Anyscale service

Build a cluster image whose worker nodes have `runsc`:

```dockerfile
FROM anyscale/ray:2.58.0-py312
RUN ARCH=$(uname -m | sed 's/arm64/aarch64/') && \
    curl -fsSL -o /usr/local/bin/runsc \
      "https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/runsc" && \
    chmod +x /usr/local/bin/runsc
```

Then deploy the builder as the service's application:

```yaml
# service.yaml
name: ray-sandbox-api
image_uri: <your-registry>/ray-sandbox-api:latest
applications:
  - name: sandbox-api
    import_path: ray.experimental.sandbox.http.app:build_app
    args:
      max_ttl_seconds: 86400
```

```bash
anyscale service deploy -f service.yaml
```

Anyscale services require their own bearer token at the platform edge, so leave `RAY_SANDBOX_API_TOKEN` unset and hand clients the service's base URL and token. Consumers such as the [Harbor](https://harborframework.com) `ray-sandbox` environment take exactly that pair as `RAY_SANDBOX_API_URL` and `RAY_SANDBOX_API_KEY`.

### Local development loop on macOS

`runsc` is Linux-only. Develop against the service in a privileged container:

```bash
docker run --privileged -p 8000:8000 \
  -v ~/path/to/ray/python/ray/experimental/sandbox:/overlay:ro \
  rayproject/ray:nightly-py312 bash -lc '
    pip install "ray[serve]" &&
    SITE=$(python -c "import ray, os; print(os.path.dirname(ray.__file__))") &&
    cp -r /overlay/* "$SITE/experimental/sandbox/" &&
    ARCH=$(uname -m | sed "s/arm64/aarch64/") &&
    curl -fsSL -o /usr/local/bin/runsc "https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}/runsc" &&
    chmod +x /usr/local/bin/runsc &&
    RAY_SANDBOX_API_TOKEN=dev-token serve run --host 0.0.0.0 ray.experimental.sandbox.http.app:build_app'
```

### gRPC facade for third-party sandbox clients

`ray.experimental.sandbox.http.grpc_facade` serves the same detached sandbox actors over gRPC. It implements the subset of a third-party sandbox SDK's control-plane and command-router services that the SDK's Sandbox API uses, so you can point an unmodified client at a Ray cluster to create sandboxes, run commands, and use the client's filesystem API.

The facade requires `grpclib` and `ray[default]`, not the Serve extra. Run it on a node that can reach the cluster and hand clients the URL it advertises:

```bash
pip install grpclib
python -m ray.experimental.sandbox.http.grpc_facade \
  --host 0.0.0.0 --port 50051 --advertise-url http://<facade-host>:50051
```

Keep these limits in mind:

* **Images**: The facade runs prebuilt registry images only. It rejects image definitions that need a server-side build step.
* **Names**: Sandbox names are scoped to the client app. Creating a sandbox under a live name returns the existing sandbox.
* **State**: The facade keeps exec state in memory, so run one facade process per cluster.
* **Network**: The facade doesn't enforce network allowlists. It grants open egress instead.

## API reference

For detailed signatures, parameters, and return types, see {ref}`ray-sandbox-ref`.

## Troubleshooting

* **`runsc` not found in `$PATH`**: Verify that gVisor's `runsc` binary is installed on all Ray worker nodes and sits in a directory on the system `$PATH`, such as `/usr/local/bin/runsc`.
* **`gVisor container failed to start: WARNING: host page size mismatch - running on non-4K host`**: The node's kernel uses 64 KiB pages, and gVisor's prebuilt `runsc` only supports 4 KiB pages. Build `runsc` from source with `--define=pagesize=64k` (see [Requirements](#requirements)).
* **`mkfs.erofs` not found or too old**: Sandbox creation fails with an error naming erofs-utils 1.7. Install erofs-utils 1.7 or later on every worker node; Ubuntu 22.04's packaged 1.4 predates the `--tar` option Ray relies on.
* **`mkfs.erofs failed: ... invalid block size 65536`**: The node's kernel uses 64 KiB pages, and its erofs-utils package can only build 4 KiB blocks. Build erofs-utils from source with `./configure MAX_BLOCK_SIZE=65536` (see [Requirements](#requirements)).
* **cgroup or permission errors**: In containerized environments such as Kubernetes without root permissions, keep the default `rootless=True`. Where cgroups are restricted, set `RAY_SANDBOX_IGNORE_CGROUPS=1`.
* **Node disk filling up with images**: The image cache is capped at half of its filesystem by default. Lower the cap with `RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES` (bytes) on worker nodes, or move the cache to a larger volume. Images that running sandboxes use are never evicted, so many concurrent sandboxes on distinct large images still need that much disk.
* **Image pull failures**: Verify that the node can reach the container registry, such as Docker Hub or GHCR, or pre-populate the image cache directory at `/tmp/ray/sandbox/images`. When many nodes pull large images at once, Docker Hub's anonymous rate limits are a likely cause; see [Route Docker Hub pulls through a mirror](#route-docker-hub-pulls-through-a-mirror).
* **`slirp4netns` not found for `network="public"`**: Install the slirp4netns package (or a [static build](https://github.com/rootless-containers/slirp4netns/releases)) on worker nodes.
* **`public` sandboxes fail to start with a tap or namespace error**: slirp4netns needs `/dev/net/tun` in the worker's environment and a seccomp policy that allows unprivileged user+network namespace creation (`unshare -Un true` must succeed as the Ray user). The slirp4netns error appears in the sandbox's `runsc.stderr.log` and in the creation error message.
* **Warning `readonly=True, but this worker can't mount an overlayfs rootfs`**: The sandbox's mounts need mount points its image might lack, such as an explicit `workdir`, and the worker can't mount an overlayfs root filesystem to create them. The sandbox still runs, but on a writable root whose writes Ray discards with the sandbox. For the cause, see the `Can't mount an overlayfs sandbox's rootfs on this node` entry.
* **`Can't mount an overlayfs sandbox's rootfs on this node`**: The error lists each way Ray tried to mount the image and why it failed. On a worker without mount privileges, the usual causes are an `erofsfuse` that's missing or built without FUSE support, no read-write `/dev/fuse`, or a host or seccomp profile that blocks user namespaces. To check the last one, run `unshare --user --map-root-user --mount true` as the Ray user. On Ubuntu 23.10 and later, AppArmor also blocks mounting inside unprivileged user namespaces by default. Allow it with an AppArmor profile that grants `userns` to the worker's binaries, or with `sysctl kernel.apparmor_restrict_unprivileged_userns=0`. On a worker with mount privileges, check for the `erofs` driver, a free loop device, and a working `erofsfuse`. Each worker process checks only once, so restart Ray on the node after a fix.
* **`gVisor container failed to start: ... rootfs overlay mount failed`**: The worker's test mount worked, but this sandbox's mount failed. With the kernel `erofs` driver, the usual cause is running out of loop devices, which `losetup -f` checks.
* **`mapping ids failed`**: The worker couldn't write the id maps of a sandbox's user namespace. A worker running as root writes them itself, which needs `CAP_SETUID` and `CAP_SETGID`. For a worker running as any other user, `newuidmap` or `newgidmap` refused to map the Ray user's subordinate ids. Both need to be setuid root or have the equivalent file capabilities, which container image builds, `nosuid` mounts, and `allowPrivilegeEscalation: false` in Kubernetes can each remove. They also need to accept the Ray user's ranges. Check them with `getsubids $USER` and `getsubids -g $USER`, or with `grep "^$USER:" /etc/subuid /etc/subgid` where the node doesn't have `getsubids`.
* **Warning `Overlayfs sandboxes on this worker run in a new user namespace`**: Workers running as a user other than root without mount privileges or subordinate ids log this. Only uid 0 and gid 0 map inside their user namespace, so files owned by any other id show up as owned by `nobody`. To keep their owners, give the Ray user subordinate uids and gids, as [Overlayfs root filesystems](#overlayfs-root-filesystems) describes.
* **Overlayfs sandbox fails to start with `Permission denied` for `runsc`, `slirp4netns`, or `nsenter`**: Inside the sandbox's user namespace, a worker running as a user other than root can use a file only as the Ray user, through its primary group, or through the permission bits for other users. Install the tool where every user can execute it, such as `/usr/local/bin`, or run `chmod o+rx` on the tool and each directory on its path.

## Next steps

* See {ref}`kuberay-sandboxing` to deploy Ray Sandboxes on Kubernetes with KubeRay.
* Learn more about [gVisor](https://gvisor.dev/docs/).
* Explore {ref}`resource-isolation` to isolate Ray system processes from worker processes.
