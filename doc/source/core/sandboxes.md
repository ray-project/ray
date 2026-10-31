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

(ray-sandbox-requirements)=

## Requirements

Ray Sandboxes need the following on every Ray node that runs a sandbox:

* **Linux**: x86_64 or arm64.
* **gVisor (`runsc`)**: Install the `runsc` binary on worker nodes and make it reachable from the system `$PATH`.
* **Ray**: version 2.58.0 or later, which includes the `ray.experimental.sandbox` package.
* **erofs-utils**: `mkfs.erofs` 1.7 or later on the `$PATH`. Ray caches each image as an EROFS file that gVisor mounts inside its own kernel, so files in the sandbox keep the image's real owners and `chown` works for any uid, with no privileges or id mappings on the node. Sandbox creation fails without it.
* **Overlayfs root filesystems**: Ray boots some sandboxes from a kernel overlayfs over their cached EROFS image, based on each sandbox's final OCI spec. Every worker needs `unshare`, `mount`, `mountpoint`, `flock`, and `setpriv` from util-linux for this. Workers without mount privileges also need `erofsfuse`, and so do workers with them whose kernel lacks the `erofs` driver or free loop devices. Workers without mount privileges that run as a user other than root need the `uidmap` package too. For details, see [Overlayfs root filesystems](#overlayfs-root-filesystems).
* **slirp4netns (`network="public"` only)**: The [slirp4netns](https://github.com/rootless-containers/slirp4netns) binary on the `$PATH`, plus `/dev/net/tun` in the worker's environment. slirp4netns bridges each sandbox's private network namespace to the node. The `nsenter` and `setpriv` commands from util-linux 2.33 or later, which Ubuntu 20.04 and later include, must also be on the `$PATH`.
* **nftables (`network="public"` only)**: The `nft` binary on the `$PATH`. The node's kernel also needs `nf_tables` built in or loaded as a module. Ray filters each `public` sandbox's outbound traffic with an nftables ruleset, so creating a `public` sandbox fails without them. To check a node, see [Troubleshooting](#troubleshooting).

To install `runsc` on a Linux worker node, see the [gVisor installation guide](https://gvisor.dev/docs/user_guide/install/). gVisor's prebuilt `runsc` only supports 4 KiB pages, so on nodes whose kernel uses 64 KiB pages, build it from source with `--define=pagesize=64k`. `slirp4netns` ships as a package on Debian, Ubuntu, and Fedora, or as a [static build](https://github.com/rootless-containers/slirp4netns/releases) for x86_64 and aarch64. Distributions package `nft` as `nftables`, so on Debian and Ubuntu, run `apt-get install nftables`. On Ubuntu 24.04 or Debian 13 with a kernel page size of 4 KiB, install the `erofs-utils` and `erofsfuse` packages from the distribution's repositories. On Ubuntu 22.04, the `erofs-utils` package is too old, so you need to build a release from the [erofs-utils repository](https://github.com/erofs/erofs-utils) instead. Install `autoconf`, `automake`, `libtool`, `pkg-config`, `liblz4-dev`, `uuid-dev`, and `libfuse3-dev`, then run `./autogen.sh && ./configure --enable-fuse && make && make install`. The resulting `erofsfuse` also needs the FUSE runtime library, `libfuse3-3` on Ubuntu 22.04. On nodes whose kernel uses 64 KiB pages, you need to build from source in all cases (regardless of distribution). Follow the instructions above, but additionally pass `MAX_BLOCK_SIZE=65536` to `./configure`. This is required because `runsc` only accepts EROFS images whose block size is a multiple of the node's page size, and distro packages build with 4 KiB blocks. Workers that run as a user other than root without mount privileges also need the `uidmap` package, as [Overlayfs root filesystems](#overlayfs-root-filesystems) describes.

The following Dockerfiles add everything sandboxes need to a Ray image, including the tools that overlayfs root filesystems need. Pick the tab that matches your nodes' kernel page size. To check it, run `getconf PAGESIZE` on a node, which prints `4096` for 4 KiB pages and `65536` for 64 KiB pages. Each Dockerfile builds its tools in a separate stage, so the build tools stay out of the final image.

::::{tab-set}

:::{tab-item} 4 KiB pages
This Dockerfile downloads gVisor's prebuilt `runsc`, which supports 4 KiB pages, and builds erofs-utils from source. Ray's images are based on Ubuntu 22.04, whose packaged `erofs-utils` is too old. On an Ubuntu 24.04 or Debian 13 base image, drop the erofs-utils step and its build dependencies from the build stage, and change the final stage's package list to `erofs-utils erofsfuse nftables uidmap`. The `erofsfuse` package pulls in the libfuse3 runtime library.

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

# Install libfuse3 for erofsfuse, newuidmap and newgidmap from uidmap to map
# the Ray user's subordinate ids on workers without mount privileges, and
# nftables for network="public".
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfuse3-3 nftables uidmap \
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

# Install libfuse3 for erofsfuse, newuidmap and newgidmap from uidmap to map
# the Ray user's subordinate ids on workers without mount privileges, and
# nftables for network="public".
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfuse3-3 nftables uidmap \
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
* The worker's user must be able to read and write the GPU device nodes, such as `/dev/nvidiactl`, which Ray opens to detect GPUs and to generate the CDI spec. NVIDIA's default mode, `0666`, allows this. If the driver restricts them to a group, through `NVreg_DeviceFileMode` and `NVreg_DeviceFileGID`, add the worker's user to that group.
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

### Cap disk use

A writable sandbox (`readonly=False`) keeps the files it writes in a private overlay, which is a file on the node's disk. Set `disk` to cap that overlay, so that a runaway build or download in one sandbox can't fill the node's disk:

```python
sb = sandbox.create(image="python:3.12-slim", readonly=False, disk="10Gi")
```

Writes past the cap fail inside the sandbox with `ENOSPC` ("No space left on device"), and the sandbox keeps running. Deleting files frees space again. `disk` takes the units that `memory` takes and also counts `/tmp`. In-memory mounts such as `/dev/shm` count toward the sandbox's memory instead.

A `readonly=True` sandbox keeps its root filesystem read-only, so `disk` caps its `workdir` instead. The `workdir`'s writes then go to a file on the node's disk that the cap bounds, rather than straight to the `workdir`'s directory on the node. A read-only sandbox without a `workdir` has nothing writable to cap, so `disk` requires `readonly=False` or a `workdir`.

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

When a command runs past its `timeout`, Ray kills it and every process it started, and `exec` raises `SandboxTimeoutError`. Only a daemon keeps running: a process that left the command's session with `setsid` and whose parent has exited. Output bytes that aren't valid UTF-8 come back as U+FFFD replacement characters.

### GPU access

Because `Sandbox` is a standard Ray actor, you can request GPUs with `num_gpus` the same way you would for any other actor. Unlike `cpu` and `memory`, however, there is no way to further limit the number of GPUs seen by a sandbox once the actor is scheduled. Sandbox support for GPUs requires `nvidia-container-toolkit-base` on the node (see [Requirements](#requirements)) and gVisor `release-20260520.0` or newer, the first release that runs the `createContainer` hooks in a GPU's CDI spec. A gVisor built from source needs a checkout from at least that release.

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

A sandbox doesn't outlive the Ray worker that creates it. When the worker exits or crashes, Ray stops the processes the worker started, and the sandbox with them, so an actor that dies without deleting its sandboxes doesn't leak their CPU and memory. Sandbox creation on the node then deletes what they left on disk, in the background and at most once a minute per process: runsc's state, the writable overlay, and the image cache's record that the sandbox uses the image. A sandbox created outside a Ray worker, such as by a driver script, keeps running after its creator exits until that cleanup stops it, so delete those explicitly.

See [GPU access](#gpu-access) above for giving each sandbox in a pool like this its own GPU.

### Checkpoint and restore sandboxes

Ray Sandboxes support checkpointing and restoration, which enables two distinct use cases in agentic AI and reinforcement learning:

1. **Parallel branching for tree search**: Algorithms such as Monte Carlo Tree Search (MCTS) or Search-over-Thoughts explore multiple diverging execution paths from a common prefix. Instead of running expensive warmup steps (installing dependencies, compiling, or loading data) repeatedly from scratch for each branch, you can warm up a base sandbox once, take a snapshot checkpoint, and restore multiple isolated child sandboxes with low latency (< 10ms) to explore different branches in parallel without cross-branch pollution.
2. **Suspend and resume for multi-turn rollouts**: In long-running conversational agent interactions or multi-turn agent evaluations, an environment often remains idle while waiting for an external LLM generation or human response. You can suspend the sandbox state to local disk or NVMe SSD (freeing node memory and CPU resources), and resume the environment on demand when the next turn's action arrives.

#### Branching for tree search

The following example demonstrates checkpointing a warmed environment and branching into isolated parallel sandboxes:

```python
import ray
from ray.experimental import sandbox

ray.init()

# 1. Start a base sandbox (must run with rootless=False)
base_sb = sandbox.create(
    image="python:3.10-slim",
    rootless=False,
    workdir="/workspace",
)

# 2. Warm up environment (e.g. install dependencies, clone repo, compile)
ray.get(base_sb.exec.remote("python3 -c 'import math; print(\"Environment warmed!\")'"))

# 3. Take a snapshot checkpoint of the sandbox state to local disk or SSD
# (checkpoint_path is optional; defaults to /tmp/ray/sandbox/checkpoints/<sandbox_id>-<uuid>)
checkpoint_path = ray.get(base_sb.checkpoint.remote())

# 4. Restore multiple branching child sandboxes with low latency from the checkpoint
branch_1 = sandbox.restore(checkpoint_path)
branch_2 = sandbox.restore(checkpoint_path)

# 5. Execute separate actions in isolated child environments
res1 = ray.get(branch_1.exec.remote("echo 'branch 1' > /workspace/branch.txt && cat /workspace/branch.txt"))
res2 = ray.get(branch_2.exec.remote("echo 'branch 2' > /workspace/branch.txt && cat /workspace/branch.txt"))

print("Branch 1:", res1.stdout.strip())
print("Branch 2:", res2.stdout.strip())

# Clean up
ray.get(branch_1.delete.remote())
ray.get(branch_2.delete.remote())
ray.get(base_sb.delete.remote())
```

#### Suspend and resume for multi-turn rollouts

To free memory during idle multi-turn rollouts (such as between LLM reasoning turns), you can checkpoint a sandbox and stop it with `leave_running=False`. The `Sandbox` actor keeps its CPU and memory reservation until it exits, so kill it too. When the next turn arrives, restore the sandbox from the saved checkpoint to continue execution:

```python
# Checkpoint and stop the running sandbox, then release the actor's resources
checkpoint_path = ray.get(sb.checkpoint.remote(leave_running=False))
ray.kill(sb)

# Later, resume execution for the next turn
resumed_sb = sandbox.restore(checkpoint_path)
res = ray.get(resumed_sb.exec.remote("cat /workspace/state.txt"))
```

:::{note}
* **Rootless mode:** gVisor's checkpoint and restore mechanism requires `rootless=False` and `SYS_PTRACE` capabilities inside worker containers to dump process memory maps and namespaces.
* **Node-local storage:** A checkpoint only resides on the worker node that took it, such as under `/tmp/ray` or on a local NVMe SSD. `sandbox.restore()` pins the restored actor to that node with Ray's `NodeAffinitySchedulingStrategy`, and reserves the checkpointed sandbox's CPU and memory unless you override them.
* **Overrides on restore:** `sandbox.restore()` accepts `cpu`, `memory`, `env`, `dns`, and `cidr_allowlist` overrides. The saved state depends on `image`, `network`, `readonly`, `capabilities`, `disk`, and, on a read-only root filesystem, `workdir`, so restore raises an error if you pass a different value for any of them. A sandbox created with an `_oci_spec_transform_fn` must be restored with the same function, because the checkpoint can't save it.
* **Egress allowlist:** a checkpoint saves the allowlist in force when it's taken, including changes from `set_egress_allowlist()`.
* **Checkpoint path:** `checkpoint()` replaces an earlier checkpoint at the same path, but refuses a non-empty directory that doesn't hold one.
* **Image builds:** gVisor restores only onto the same build of the sandbox's image. If the image was evicted from the cache and rebuilt, or its tag moved, since the checkpoint, restore fails with an error that says so.
* **Compression:** `checkpoint(compression="flate-best-speed")` writes a smaller checkpoint. gVisor loads guest memory in the background only from uncompressed checkpoints, so a sandbox restored from a compressed one starts after all of its memory is loaded.
:::

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

Sandboxes boot from OCI container images. The image manager pulls an image straight from the registry's HTTP API, with no Docker daemon, flattens its layers, and caches the result under `/tmp/ray/sandbox/images` on the node for reuse by subsequent sandboxes on that node using the same image. References that name the same image share one cache entry, so `python:3.12-slim`, `library/python:3.12-slim`, and `docker.io/library/python:3.12-slim` pull and build it once. The cached root filesystem is a single EROFS image, built with `mkfs.erofs`, that gVisor mounts inside the Sentry, which keeps the image's file ownership intact. Sandboxes with write access to the filesystem get their own private writable overlay on top of the cached root filesystem. A `readonly=True` sandbox whose mounts need mount points the image might lack, such as an explicit `workdir`, boots from an overlayfs root filesystem instead, because runsc can't create mount points in a read-only image. On a worker that can't mount one, the sandbox gets a writable root filesystem, and Ray discards its writes with the sandbox. For details, see [Overlayfs root filesystems](#overlayfs-root-filesystems). A cache left by an earlier Ray version, which extracted images into directories, is rebuilt on the next pull.

### Pull from private registries

Ray pulls a private image with the credentials that the node's Docker configuration file holds for its registry. A sandbox can boot from any private image named at its creation, with no Docker installation and no preloading. Ray reads `$DOCKER_CONFIG/config.json`, by default `~/.docker/config.json` of the user that runs Ray, and looks up a registry's credentials the way the Docker CLI does:

1. `credHelpers` maps a registry to a [credential helper](https://github.com/docker/docker-credential-helpers). Ray runs that `docker-credential-<name>` program from the `$PATH` to get the registry's credentials. A helper such as [`docker-credential-ecr-login`](https://github.com/awslabs/amazon-ecr-credential-helper) mints short-lived credentials from the node's cloud identity at pull time, so no secret sits on disk.
1. `credsStore` names the helper for every registry that `credHelpers` doesn't list.
1. `auths` holds static credentials, in the format that `docker login` writes and that a Kubernetes `kubernetes.io/dockerconfigjson` Secret stores. Mount such a Secret's `.dockerconfigjson` key as `config.json`.

Ray reads the file only when a registry asks a pull to authenticate. It handles both the token authentication that Docker Hub, GHCR, and Artifact Registry use and the HTTP basic authentication that Amazon ECR uses. Docker Hub and GHCR ask every pull to authenticate, including pulls of public images. When the file holds credentials for Docker Hub or GHCR, Ray uses them for public images too. If that registry's credential helper fails, public image pulls from it fail too, as they do with `docker pull`.

The credentials stay out of sandboxes. The Ray worker that pulls an image holds them in memory for the pull and sends them only to the registry and to the token service that the registry names, not to the object storage that serves the image's layers. Ray never writes them to the image cache or its logs, and a sandbox's processes see only the image's files and the environment variables that you pass. Keep the following in mind:

* **Ray checks credentials when it pulls, not when a sandbox starts.** Every sandbox on a node can use any image that the node already holds in its cache.
* **A sandbox with network access can reach whatever its node can.** That can include the source a credential helper draws on, such as a cloud's instance metadata service. On Amazon EKS, require Instance Metadata Service Version 2 (IMDSv2) with a hop limit of 1 so that Pods and the sandboxes they run can't reach it.

For example, to pull from Amazon ECR with the IAM role of the Ray Pods, through IAM roles for service accounts (IRSA) or EKS Pod Identity, add `docker-credential-ecr-login` to the Ray image. Then give every Ray container that runs sandboxes the following configuration:

```json
{"credHelpers": {"<acct>.dkr.ecr.<region>.amazonaws.com": "ecr-login"}}
```

One way to provide the file is a ConfigMap. The following example mounts a ConfigMap named `ray-docker-config`, which holds the file as `config.json`, into each Ray container and points `DOCKER_CONFIG` at the mount:

```yaml
containers:
  - name: ray-worker
    env:
      - name: DOCKER_CONFIG
        value: /etc/ray-docker
    volumeMounts:
      - name: docker-config
        mountPath: /etc/ray-docker
        readOnly: true
volumes:
  - name: docker-config
    configMap:
      name: ray-docker-config
```

The role needs `ecr:GetAuthorizationToken`, `ecr:BatchGetImage`, and `ecr:GetDownloadUrlForLayer`, all of which the `AmazonEC2ContainerRegistryReadOnly` managed policy grants.

### Bound the image cache

The cache is bounded so that a node that runs many distinct images doesn't fill its disk. Before each pull, Ray evicts the least recently extracted images until the cache fits under the cap and the filesystem that holds the cache has a minimum of free space, so the cache gives way when sandboxes' writable overlays, logs, or spilled objects fill the disk. Images that a running sandbox uses are never evicted. The cap defaults to half of the filesystem and the free-space floor to a tenth of it, the level at which the kubelet starts evicting Pods. Set `RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES` and `RAY_SANDBOX_IMAGE_CACHE_MIN_FREE_BYTES` on worker nodes to choose either in bytes, or set both to `0` to disable eviction.

### Route Docker Hub pulls through a mirror

Unless the nodes hold Docker Hub credentials, every node pulling from Docker Hub consumes the anonymous pull-rate limit and downloads the image over the WAN. In a large cluster, concurrent pulls of multi-GB images can quickly hit the rate limit or saturate network bandwidth, causing image pulls to fail or become slow.

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
* **The mirror authenticates like any registry.** Ray uses the credentials that the node's Docker configuration holds for the mirror's host, so an ECR pull-through cache works with a `credHelpers` entry for it. See [Pull from private registries](#pull-from-private-registries).

## Networking and DNS

Sandboxes support four network modes. The default is `none`, which follows the safe-defaults principle. Use `public` when a sandbox needs internet access.

The following table compares the modes:

| Mode | What the sandbox can reach | `/etc/resolv.conf` | Use it for |
| --- | --- | --- | --- |
| `none` *(default)* | Nothing | Untouched | Untrusted code that doesn't need the network. This is the recommended setting. |
| `public` | The public internet, with internal addresses left out as a best effort, or only the destinations in an allowlist that you set | Generated from `dns`, which defaults to `8.8.8.8` and `1.1.1.1`, and mounted read-only | Code that needs the internet. For untrusted code, set an allowlist. See [What can a public sandbox reach?](#what-can-a-public-sandbox-reach) |
| `host` | Everything the node can reach, including internal networks, node-local services, every Ray port, the Kubernetes API, and instance metadata | The node's own file, mounted read-only. `dns` overrides it. | Trusted code only. |
| `sandbox` | Whatever gVisor's own network stack reaches. Ray doesn't filter its traffic. | Untouched | Trusted code only. Requires `rootless=False`, because runsc doesn't support the sandbox network stack in rootless mode. |

:::{warning}
A `public` sandbox can reach the whole internet, so code inside it can send data anywhere. By default, Ray leaves out internal addresses, such as private networks, instance metadata, and the Kubernetes API, but that's a best effort, not a guarantee for every service your cluster runs. For untrusted code that doesn't need the network, use `network="none"`. For untrusted code that does, set an allowlist that names only the services it needs. See [Restrict egress to an allowlist](#restrict-egress-to-an-allowlist).
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

### What can a public sandbox reach?

Each `public` sandbox gets a network namespace of its own, which [slirp4netns](https://github.com/rootless-containers/slirp4netns) connects to the node. The sandbox has its own ports and loopback. A server that binds `0.0.0.0` inside it can't collide with, be reached by, or reach other sandboxes or services on the node, and nothing on the node or in the cluster can open a connection into it. The sandbox's address is `198.18.0.100`, from the RFC 2544 benchmarking range, which Ray picked because it doesn't overlap the networks that clusters usually use. Its `/etc/resolv.conf` comes from `dns`, so it inherits nothing from the node's resolver configuration.

Ray filters all traffic that leaves the sandbox. Before the sandbox starts, Ray installs an nftables ruleset in the sandbox's namespace that drops all outbound traffic except the following:

* DNS over UDP and TCP port 53 to the resolvers in `dns`.
* The destinations in the sandbox's allowlist. Unless you pass `cidr_allowlist`, the allowlist is `["internet"]`, every public address. It leaves out private, shared, and other special-purpose networks, where clusters usually run their nodes and services.

If the ruleset fails to install, Ray doesn't start the sandbox, so a sandbox never runs with unfiltered egress.

A denylist keeps a few internal destinations closed whatever the allowlist says. Ray drops traffic to them ahead of the allowlist:

* Loopback and link-local addresses. Link-local addresses hold most clouds' instance metadata services, such as `169.254.169.254`. The sandbox's own loopback still works.
* Alibaba Cloud's metadata service at `100.100.100.200`, and Azure's platform endpoint at `168.63.129.16`.
* The Kubernetes API Service, at the address in `KUBERNETES_SERVICE_HOST`.
* Every address in `RAY_SANDBOX_EGRESS_DENY`. See [Block more destinations on every sandbox](#block-more-destinations-on-every-sandbox).

:::{important}
Leaving out internal addresses is a best effort, not a boundary you can rely on for everything your cluster runs. It doesn't cover services on public addresses, and Ray doesn't track your Ray nodes. Under the default allowlist, a `public` sandbox can't reach a Ray node only because the node's address is internal. If you rely on that, make sure that every Ray node, including the nodes that the autoscaler adds later, has an internal address, or add your nodes' network to `RAY_SANDBOX_EGRESS_DENY`. For untrusted sandboxes that need network access, write an allowlist that names only the services they need.
:::

(ray-sandbox-egress-allowlist)=

### Restrict egress to an allowlist

Pass `cidr_allowlist` to replace the default `["internet"]` with the destinations a `public` sandbox needs. Ray drops everything else for every protocol. The denylist and DNS work the same way with any allowlist. The following sandbox reaches HTTPS servers on the internet and port 8000 on `10.0.0.84`, and nothing else:

```python
from ray.experimental import sandbox
from ray.experimental.sandbox import DOCKER_DEFAULT_CAPABILITIES

sb = sandbox.create(
    image="python:3.10-slim",
    network="public",
    cidr_allowlist=["internet:443", "10.0.0.84:8000"],
    capabilities=DOCKER_DEFAULT_CAPABILITIES,
    readonly=False,
)
```

An allowlist entry takes one of the following forms:

| Entry | What it allows |
| --- | --- |
| `203.0.113.7` or `203.0.113.0/24` | An IPv4 or IPv6 address or network, on every port and protocol |
| `203.0.113.7:8000` or `10.0.1.0/24:8000-8010` | An address or network, on one TCP and UDP port or a range of them |
| `[2001:db8::1]:443` | An IPv6 address or network with ports, written in brackets |
| `internet` | Every public address. It leaves out private, shared, loopback, link-local, multicast, and the other special-purpose IPv4 networks, and keeps IPv6 global unicast addresses. |
| `internet:443` | Every public address, on port 443 only |

Ray normalizes each entry, so `10.0.1.5/24` becomes `10.0.1.0/24`, and rejects anything that isn't an IP address, such as a host name. Allowing destinations by domain name takes a proxy that inspects TLS, which Ray doesn't provide.

The following allowlists cover common cases:

* `["internet"]` is the default. It reaches the public internet and leaves out internal addresses.
* `["internet:443"]` reaches HTTPS servers on the internet and nothing else.
* `["internet", "10.0.0.84:8000"]` adds port 8000 on one address. If `10.0.0.84` is a Ray node, the sandbox reaches that port and none of the node's Ray ports.
* `[]` allows DNS and nothing else. Use it for the strictest `public` sandbox, or as a starting point that you widen later.
* `ALLOW_ALL_CIDRS`, which is `["0.0.0.0/0", "::/0"]`, reaches every destination outside the denylist, including your private networks and every Ray node on them.

:::{caution}
An allowlist that includes a private network, such as `["10.0.0.0/8"]` or `ALLOW_ALL_CIDRS`, reaches every service on that network outside the denylist, including your Ray nodes and every Ray port on them. On Kubernetes, it also reaches other Pods and Services, and the kubelets on the Kubernetes nodes' own addresses. List only the destinations the sandbox needs, or close everything else with `RAY_SANDBOX_EGRESS_DENY`.
:::

:::{note}
Some Kubernetes network plugins, such as Cilium, translate a Service's IP address to the address of one of its Pods when a connection opens, before the sandbox's filter sees the traffic. On those clusters, the filter matches Pod IPs, not Service IPs. To reach a Service, list the addresses of its Pods, and don't pass a Service IP in `dns`. The same translation sends a connection to the Kubernetes API Service to the API server's own address, so a broad allowlist such as `ALLOW_ALL_CIDRS` reaches the API server. To close it, add the API server's addresses to `RAY_SANDBOX_EGRESS_DENY`.
:::

DNS is the one exception to the allowlist. UDP and TCP port 53 to the resolvers in `dns` stay open, so names resolve even with `[]`. Ray opens only port 53 on those addresses, so add a resolver's address to the allowlist if the sandbox needs anything else from it. Ray opens DNS ahead of the denylist, so a resolver on a link-local address also works. Public resolvers forward queries for any domain, so code in the sandbox can still send data out inside DNS queries. If no data may leave the sandbox, use `network="none"`.

### Change the allowlist of a running sandbox

`set_egress_allowlist` replaces the allowlist of a running `public` sandbox in one atomic step, and reads `RAY_SANDBOX_EGRESS_DENY` again. Connections that the new allowlist doesn't allow stall from their next packet, because Ray doesn't reset them. On a sandbox from `sandbox.create()`, call it on the actor:

```python
ray.get(sb.set_egress_allowlist.remote(["internet:443"]))
```

With `SandboxRuntime`, pass the sandbox's instance ID. The following example installs a package over HTTPS, then cuts the sandbox off from everything but DNS:

```python
from ray.experimental.sandbox import SandboxRuntime

runtime = SandboxRuntime()
instance_id = runtime.create(
    image="python:3.12-slim",
    network="public",
    cidr_allowlist=["internet:443"],
    readonly=False,
)

# Install a package over HTTPS.
runtime.exec(instance_id, "pip install requests")

# Allow nothing but DNS from here on.
runtime.set_egress_allowlist(instance_id, [])
```

The [HTTP API service](#http-api-service) sets the allowlist only when it creates a sandbox. The [gRPC facade](#grpc-facade-for-third-party-sandbox-clients) changes it through the client SDK.

### Block more destinations on every sandbox

Add destinations that must stay closed to the denylist with `RAY_SANDBOX_EGRESS_DENY`, a comma-separated list of IP addresses and networks. Use it for your Ray nodes' network when an allowlist could cover it, for the Kubernetes API server's own addresses, and for internal services on public addresses. Because it names networks rather than nodes, it also covers the nodes that the autoscaler adds later. No allowlist entry opens them:

```bash
export RAY_SANDBOX_EGRESS_DENY=203.0.113.10,10.128.0.0/16
```

The worker process that creates a sandbox reads the variable when it creates the sandbox and each time the sandbox's allowlist changes. Set it on every node that runs sandboxes before you start Ray there, so that every worker process inherits it. An entry that isn't an IP address or network makes `public` sandbox creation fail, so a typo can't leave a destination open.

On Kubernetes, find the API server's addresses with `kubectl`:

```bash
kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes \
  -o jsonpath='{.items[*].endpoints[*].addresses[*]}'
```

Then set the variable on the Ray container of each RayCluster group that runs sandboxes:

```yaml
workerGroupSpecs:
  - groupName: sandbox-workers
    template:
      spec:
        containers:
          - name: ray-worker
            env:
              - name: RAY_SANDBOX_EGRESS_DENY
                value: "<api-server-addresses>,<node-network>"
```

### DNS in locked-down networks

Some virtual private clouds (VPCs) block outbound port 53 to public resolvers, so the default `dns` can't resolve names. Pass your network's resolver instead, such as `network="public", dns=["10.0.0.2"]`. Pass IPv4 addresses, because `public` sandboxes have no IPv6 route. On Kubernetes clusters that translate Service IPs, the cluster's DNS Service, which is the nameserver in a Pod's `/etc/resolv.conf`, doesn't work as a `dns` entry, so use a resolver that isn't a Service.

When you pass `dns`, Ray checks that the sandbox can reach each resolver before it hands you the sandbox. If the sandbox can't reach one, creation fails with an error that names the resolver, instead of every name lookup failing inside the sandbox.

If the sandbox can't reach any resolver, `network="host"` uses the node's own `/etc/resolv.conf`. It also gives the sandbox the node's whole network, including every Ray port and instance metadata, so use it only for trusted code. Configure anything beyond that through the OCI spec. See [Pass custom OCI configurations to gVisor](#pass-custom-oci-configurations-to-gvisor).

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

* **High-level helpers ({func}`~ray.experimental.sandbox.create`, {func}`~ray.experimental.sandbox.restore`)**: Spawns a Ray actor that encapsulates the sandbox lifecycle (or restores a snapshot) and returns an `ActorHandle`.
* **Sandbox actor ({class}`~ray.experimental.sandbox.Sandbox`)**: A Ray actor that serves as a proxy to forward command execution, file I/O, and snapshot checkpointing to the isolated sandbox instance while managing the scheduling and lifecycle of the sandbox.
* **Sandbox runtime ({class}`~ray.experimental.sandbox.SandboxRuntime`)**: A low-level abstraction that manages the lifecycle, checkpointing, and restoration of local sandboxes, image pulling and caching, and interactions with the execution backend.
* **gVisor backend (`ray.experimental.sandbox.backend.GVisorSandboxBackend`)**: Executes commands, manages snapshots (`runsc checkpoint` / `runsc restore`), and isolates processes through gVisor's OCI runtime (`runsc`).
* **Image manager (`ray.experimental.sandbox.image_manager.ImageManager`)**: Automatically pulls container images from sources such as Docker Hub, GHCR, or local tar archives, extracts root filesystems into `/tmp/ray/sandbox/images`, and builds OCI `config.json` runtime specifications.

## Security and isolation model

Ray Sandboxes implement multi-layered defense-in-depth isolation:

* **System call interception**: gVisor's Sentry application kernel intercepts system calls in user space, isolating untrusted code from the host Linux kernel.
* **Read-only root filesystem**: Ray mounts base container filesystems read-only (`readonly=True`) with an isolated copy-on-write overlay directory per sandbox.
* **Restricted working directory**: Only the explicit `workdir`, such as `/workspace`, is mounted read-write for application artifacts.
* **Network containment**: By default, `network="none"` disables all outbound network interfaces, which prevents untrusted code from making external API calls or scanning the internal cluster network. When code needs the internet, `network="public"` gives it the public internet without the node's resolver configuration or network identity. Its egress filter keeps instance metadata and the Kubernetes API Service closed whatever the allowlist says, and its default allowlist leaves out internal addresses as a best effort. See [What can a public sandbox reach?](#what-can-a-public-sandbox-reach)
* **Resource quotas**: Ray configures a host cgroup v2 for each limited sandbox. CPU bandwidth and memory limits apply to the sandbox runtime and its helpers. A `disk` cap bounds a sandbox's writable overlay on the node's disk.

### How does Ray filter a public sandbox's traffic?

Each `public` sandbox runs inside a user namespace and a network namespace of its own. Ray's launch process creates both, which gives it the `CAP_NET_ADMIN` capability inside them, so it loads the sandbox's nftables ruleset there without `CAP_NET_ADMIN` on the node. On Kubernetes, the worker Pod doesn't need `CAP_NET_ADMIN` either.

gVisor runs in that network namespace with `--network=host`, so every socket the sandbox opens is a real kernel socket in the namespace. Every packet the sandbox sends passes the ruleset's output chain before it reaches slirp4netns, whatever interface or route the packet takes.

Code inside the sandbox can't change the ruleset, even as root with `CAP_NET_ADMIN` or `CAP_NET_RAW`. gVisor handles every system call the sandbox makes, and without `--net-raw`, its host networking never passes the netlink and packet sockets that `nft` and `iptables` use to the kernel. Inside the sandbox, `nft flush ruleset` and `iptables -F` fail with `Protocol not supported`, and the rules stay in place.

On Kubernetes, a NetworkPolicy can't do this job. slirp4netns opens each sandbox connection again from the Pod's own sockets, so a policy on the Pod can't tell sandbox traffic from the Ray worker's.

### Limits of network containment

The egress filter matches IP addresses only. Keep the following gaps in mind:

* Anything on a public address counts as the internet. Under `internet`, a sandbox can reach a public Kubernetes API endpoint, a load balancer in front of Ray Serve or the Ray dashboard, and a Ray node's public IP address if the node has one. Ray passes no credentials into the sandbox, but the sandbox can call any of them that doesn't require credentials. Add those addresses to `RAY_SANDBOX_EGRESS_DENY`.
* Private networks that use public address ranges count as the internet too. If your cluster's nodes or services use public ranges internally, add those ranges to `RAY_SANDBOX_EGRESS_DENY`.
* Ray doesn't track your Ray nodes. A sandbox can't reach them only while their addresses are internal and no allowlist entry covers them, or while `RAY_SANDBOX_EGRESS_DENY` lists their network.

:::{tip}
For defense in depth, turn on Ray's token authentication with `RAY_AUTH_MODE=token` on clusters that host sandboxes, so that Ray's own ports refuse callers without the cluster's token. On Kubernetes, also set `automountServiceAccountToken: false` on worker Pods that don't call the Kubernetes API.
:::

### Enforce resource limits

Provision a writable cgroup v2 subtree on each eligible worker node and set `RAY_SANDBOX_CGROUP_PATH` to its mount path. Enable the `cpu` and `memory` controllers in the subtree's `cgroup.subtree_control`, and keep processes in separate leaf cgroups. Ray requires `cgroup.kill` support and permission to write it. Grant the worker permission to create child cgroups and to move its launch helpers from the worker cgroup into them. See [Linux cgroup v2 delegation](https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation) for the directory, controller, and common-ancestor permissions.

Every positive `cpu` or `memory` limit requires this configuration. Without it, creation fails before the image pull. Ray writes `cpu.max`, `memory.max`, and `memory.swap.max` before starting runsc or the network wrapper. Fractional CPU limits remain bandwidth limits: `cpu=0.25` gives the whole sandbox one quarter of a CPU. A sandbox with a CPU limit also runs on `max(2, ceil(cpu))` of the pool's CPUs, which Ray picks so that sandboxes spread across the pool. gVisor sizes a sandbox's CPUs from the CPUs it can run on, so `nproc`, and tools that size worker pools by CPU count, see the limit instead of the whole node. Two is gVisor's minimum. Memory limits include Sentry, Gofer, and network helpers, and limited sandboxes can't use swap. Memory exhaustion terminates the sandbox and leaves its hosting Ray worker alive.

Ray owns the cgroup, so `--ignore-cgroups` disables runsc's own cgroup management without disabling Ray's limits. `RAY_SANDBOX_IGNORE_CGROUPS=1` can't bypass a requested limit. Passing zero means no limit. Passing `cpu=None` to the sandbox actor derives a limit from its assigned Ray CPU resources.

Ray picks a sandbox's CPUs under a file lock in `/tmp/ray/sandbox` and records them in the sandbox's directory there, so sandboxes created at the same time spread across the pool too. Every worker on a node must see the same `/tmp/ray/sandbox` and the same cgroup mount path. When a worker exits or crashes, its sandboxes stop with it, and the cleanup that sandbox creation runs on the node also removes their cgroups. See [Manage sandboxes inside custom actors with SandboxRuntime](#manage-sandboxes-inside-custom-actors-with-sandboxruntime).

:::{note}
A CPU limit caps what a sandbox uses but doesn't reserve CPU time for it. Other work on the node still affects how long a task takes, so resource limits don't guarantee a wall-clock deadline.
:::

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

Errors use a JSON envelope of the form `{"error": {"code": "...", "message": "..."}}`. The codes are `401 unauthorized`, `403 forbidden` for a network mode or `rootless: false` the server doesn't allow, `404 sandbox_not_found`, `404 exec_not_found`, `404 file_not_found`, `409 conflict`, `409 unschedulable`, `409 write_failed`, `400 invalid_request`, `413 payload_too_large`, `503 sandbox_unavailable` for an actor that's briefly unreachable, such as during a restart, and FastAPI's native `422` for schema violations. The full OpenAPI schema is served at `/openapi.json`.

Keep this server behavior in mind:

* **TTL**: Every sandbox gets a TTL that reclaims both the sandbox and its hosting actor. Request it with `ttl_seconds`, capped and defaulted by the server's `max_ttl_seconds`.
* **Resources**: `resources` separates cluster reservations from enforced sandbox budgets. `cpu_request`, `memory_request_mb`, and `custom` Ray resources reserve cluster capacity, and custom resources such as `{"gvisor": 1}` pin sandboxes to runsc-equipped nodes. `cpu_limit` and `memory_limit_mb` become host cgroup caps on provisioned nodes, and creation fails if the node can't enforce them. Requests default to the limits. `disk_limit_mb` caps the writable root filesystem (see [Cap disk use](#cap-disk-use)) and requires `readonly: false` or a `workdir`.
* **Capabilities**: By default sandboxes get Docker's default Linux capability set so images behave the way they do under Docker. Ray's own default is far narrower and breaks `apt-get` and `tar`. The sets are written exactly, so `capabilities: []` runs the sandbox with no capabilities at all.
* **Network modes**: The server accepts `none`, the default, and `public`. It answers `403 forbidden` to `host` and `sandbox` unless its `allowed_network_modes` setting lists them, and to `rootless: false` unless you set `allow_rootful`. `host` gives a sandbox the node's network and every Ray port on it, and `sandbox` needs a rootful runsc, so allow them only for trusted clients. See [Networking and DNS](#networking-and-dns).
* **Egress allowlists**: A `public` sandbox takes `cidr_allowlist` at creation, with the same entries and defaults as the Python API. See [Restrict egress to an allowlist](#restrict-egress-to-an-allowlist). The HTTP API can't change the allowlist afterward.

### Self-hosted quickstart

On a Linux machine or cluster with `runsc` on `PATH`:

```bash
pip install "ray[serve]"
export RAY_SANDBOX_API_TOKEN=dev-token   # Required: build_app refuses to build without it.
serve run ray.experimental.sandbox.http.app:build_app
```

```bash
curl -s -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"image": "busybox:latest", "readonly": false, "shell": "/bin/sh"}' \
  http://localhost:8000/api/v1/sandboxes
```

To create a `public` sandbox that reaches HTTPS servers on the internet and nothing else, pass `network` and `cidr_allowlist`:

```bash
curl -s -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.12-slim", "network": "public", "cidr_allowlist": ["internet:443"]}' \
  http://localhost:8000/api/v1/sandboxes
```

Builder arguments configure the server. See `ray.experimental.sandbox.http.schemas.SandboxAPISettings` for the full list. For example:

```bash
serve run ray.experimental.sandbox.http.app:build_app max_ttl_seconds=86400 num_replicas=2
```

`serve run` passes each command-line argument as a string, so set list settings such as `allowed_network_modes` in a Serve config file instead. For example, the following config also accepts `host` sandboxes:

```yaml
# serve_config.yaml
applications:
  - name: sandbox-api
    import_path: ray.experimental.sandbox.http.app:build_app
    args:
      allowed_network_modes: ["none", "public", "host"]
```

```bash
serve run serve_config.yaml
```

### Deploying as an Anyscale service

Build a cluster image whose nodes have every tool in [Requirements](#requirements). Start from one of the Dockerfiles there, and change the base image of its last stage to an Anyscale image, such as `anyscale/ray:2.58.0-py312`.

Then deploy the builder as the service's application:

```yaml
# service.yaml
name: ray-sandbox-api
image_uri: <your-registry>/ray-sandbox-api:latest
query_auth_token_enabled: false
applications:
  - name: sandbox-api
    import_path: ray.experimental.sandbox.http.app:build_app
    args:
      max_ttl_seconds: 86400
```

```bash
export RAY_SANDBOX_API_TOKEN=$(openssl rand -hex 32)
anyscale service deploy -f service.yaml --env RAY_SANDBOX_API_TOKEN="$RAY_SANDBOX_API_TOKEN"
```

Keep the token, because clients send it as their bearer token. The app checks it itself, so the service config turns off the service's own token check with `query_auth_token_enabled: false`. Both checks read the same `Authorization` header, so no client could pass both. The app's check also covers callers inside the cluster, which reach Serve's proxies on every node's address without passing the service's edge.

Hand clients the service's base URL and the API token. Consumers such as the [Harbor](https://harborframework.com) `ray-sandbox` environment take exactly that pair as `RAY_SANDBOX_API_URL` and `RAY_SANDBOX_API_KEY`. To rely on a platform edge alone, pass `allow_unauthenticated: true` in `args`, but only where nothing else on the cluster's network can reach Serve's proxies.

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

The facade requires `ray[default]`, not the Serve extra. Run it on a node that can reach the cluster, with a token that clients must present:

```bash
pip install "ray[default]"
export RAY_SANDBOX_API_TOKEN=$(openssl rand -hex 32)
python -m ray.experimental.sandbox.http.grpc_facade \
  --host 0.0.0.0 --port 50051 --advertise-url https://<facade-endpoint>
```

Clients connect to the URL that the facade advertises. Current client SDKs accept a plaintext `http://` URL only when their server URL is on `localhost`, so either terminate TLS in front of the facade and advertise that `https://` endpoint, or forward a local port to the facade and advertise `http://127.0.0.1:<port>`.

The facade serves the client SDK's original sandbox API. The SDK's 1.6 release made a newer API its default, so set `MODAL_SANDBOX_V2=0` in clients on 1.6 or later.

#### Authentication

With `RAY_SANDBOX_API_TOKEN` set, every call must present the token, either as the client SDK's token secret, with any value as the token ID, or as an `authorization: Bearer <token>` header. Calls without it fail with `UNAUTHENTICATED`. The SDK's anonymous client sends no credentials, so give clients token credentials instead. Use an opaque random token like the one in the preceding example, and don't reuse the Ray cluster's own authentication token.

Without a token, the facade serves only loopback addresses such as `127.0.0.1`. Other Pods and processes on the cluster's network can reach a facade on a network address, and a `network="host"` sandbox on its node can too. Pass `--allow-unauthenticated` only when nothing but an authenticating proxy can reach the facade. A relay that exposes a loopback facade to other machines still needs a token.

The token isn't scoped to a client. Anyone who has it controls every sandbox in the facade's Ray namespace, which the REST API shares by default: they can create sandboxes, run commands in them, read and write their files, and terminate them. The facade serves plaintext HTTP/2, so the token crosses the network unencrypted unless TLS terminates in front of the facade.

Keep these limits in mind:

* **Images**: The facade runs prebuilt registry images only. It rejects image definitions that need a server-side build step. Nodes pull images with their own registry credentials, so the facade ignores registry secrets that clients attach to images. See [Pull from private registries](#pull-from-private-registries).
* **Names**: Sandbox names are scoped to the client app. Creating a sandbox under a live name returns the existing sandbox.
* **State**: The facade keeps no exec state of its own: each exec lives with its sandbox, so several facade processes can serve the same sandboxes.
* **Network**: The facade maps the client SDK's network settings onto network modes and egress allowlists.

The facade handles each of the client SDK's network settings as follows:

| Client SDK setting | What the facade does |
| --- | --- |
| `block_network=True` | Creates a `network="none"` sandbox. |
| No network setting | Creates a `network="public"` sandbox with the default `["internet"]` allowlist. |
| `outbound_cidr_allowlist=[...]` | Creates a `network="public"` sandbox with that list as its `cidr_allowlist`. See [Restrict egress to an allowlist](#restrict-egress-to-an-allowlist). |
| `outbound_domain_allowlist=[...]` | Rejects the request with `INVALID_ARGUMENT`, because the facade has no proxy that inspects TLS. |
| `inbound_cidr_allowlist=[...]` | Accepts the setting and changes nothing, because sandboxes accept no inbound connections. |
| `_experimental_set_outbound_network_policy()` | Replaces the allowlist of a running sandbox. Called without arguments, it sets `["internet"]`. On a `block_network=True` sandbox, it fails with `FAILED_PRECONDITION`. |

#### Serve the facade with Ray Serve

`ray.experimental.sandbox.http.grpc_app` serves the facade through Ray Serve's gRPC proxy instead of the facade's own server, so you can deploy it as a Serve application, such as an Anyscale service. Name two functions in the Serve config: the facade's `add_servicers_to_server` registers its gRPC services with the proxy, and `grpc_app.build_app` builds the application.

```yaml
grpc_options:
  grpc_servicer_functions:
    - ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server
applications:
  - name: sandbox-facade
    route_prefix: /
    import_path: ray.experimental.sandbox.http.grpc_app:build_app
```

The application needs `ray[serve]`. Set `RAY_SANDBOX_API_TOKEN` both where the application is built and where its replica runs: `build_app` and the replica each refuse to start without a token, because Serve's proxies listen on every node's address, where other Pods and processes on the cluster's network can reach them. To run more than one replica, set `num_replicas` in the application's `args`. Keep it the only application on its Serve instance, because clients don't send the `application` metadata that Serve uses to choose among several.

Clients use the address of Serve's gRPC proxy as their server URL. By default, the facade gives each client `https://` plus the host it dialed as its command-router URL, which suits a TLS endpoint on port 443 in front of the proxy. For any other setup, set `advertise_url` in the application's `args`, such as `http://127.0.0.1:9000` for clients of a local proxy.

On Anyscale, list both of the facade's gRPC services and turn off the service's own token check, which limits a service to one gRPC service name. The facade checks `RAY_SANDBOX_API_TOKEN` itself, which also covers traffic from inside the cluster that the service's check doesn't see:

```yaml
query_auth_token_enabled: false
grpc_options:
  service_names:
    - modal.client.ModalClient
    - modal.task_command_router.TaskCommandRouter
  grpc_servicer_functions:
    - ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server
applications:
  - name: sandbox-facade
    route_prefix: /
    import_path: ray.experimental.sandbox.http.grpc_app:build_app
```

The TLS endpoint in front of the facade must accept HTTP/2 from a client that doesn't negotiate it. The client SDK's command-router connection, which carries commands and file operations, offers no ALPN in its TLS handshake (releases through 1.6.1) and then speaks HTTP/2 anyway. Envoy accepts that connection, because it detects HTTP/2 from the client's connection preface. ingress-nginx doesn't: behind it, creating and terminating sandboxes works, but running commands and file operations fail. On Anyscale, deploy the service to a Kubernetes cloud whose operator routes services through an Envoy Gateway (`networking.gateway` in the operator's Helm values) rather than ingress-nginx.

A proxy in front of the facade also limits each HTTP/2 stream. The client SDK sends a file write as one request stream, and it reads a command's output on a stream that stays quiet until the command exits. Envoy doesn't limit request size by default, but its default route timeout ends any response that takes longer than 15 seconds, a command's output stream included. With Envoy Gateway, turn that timeout off on the Gateway that serves the facade. On Anyscale, that's the Gateway named in the operator's `networking.gateway` values:

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: sandbox-facade-streams
  namespace: <the Gateway's namespace>
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: Gateway
      name: <the Gateway's name>
  timeout:
    http:
      requestTimeout: 0s
      maxStreamDuration: 0s
      connectionIdleTimeout: 1h
```

ingress-nginx defaults to 1 MiB per request and 60 seconds without data from the backend, which cap file writes at about 7.5 MiB and output reads on a running command at about 11 minutes, once the SDK's retries run out. The annotations `nginx.ingress.kubernetes.io/proxy-body-size: "0"`, `nginx.ingress.kubernetes.io/proxy-read-timeout`, and `nginx.ingress.kubernetes.io/proxy-send-timeout` on the facade's Ingress raise those limits, but no ingress-nginx setting lets it serve the client SDK's command-router connection.

## API reference

For detailed signatures, parameters, and return types, see {ref}`ray-sandbox-ref`.

## Troubleshooting

* **`runsc` not found in `$PATH`**: Verify that gVisor's `runsc` binary is installed on all Ray worker nodes and sits in a directory on the system `$PATH`, such as `/usr/local/bin/runsc`.
* **`gVisor container failed to start: WARNING: host page size mismatch - running on non-4K host`**: The node's kernel uses 64 KiB pages, and gVisor's prebuilt `runsc` only supports 4 KiB pages. Build `runsc` from source with `--define=pagesize=64k` (see [Requirements](#requirements)).
* **`mkfs.erofs` not found or too old**: Sandbox creation fails with an error naming erofs-utils 1.7. Install erofs-utils 1.7 or later on every worker node; Ubuntu 22.04's packaged 1.4 predates the `--tar` option Ray relies on.
* **`mkfs.erofs failed: ... invalid block size 65536`**: The node's kernel uses 64 KiB pages, and its erofs-utils package can only build 4 KiB blocks. Build erofs-utils from source with `./configure MAX_BLOCK_SIZE=65536` (see [Requirements](#requirements)).
* **cgroup or permission errors**: Keep `rootless=True` for unprivileged runsc. Positive CPU or memory limits also require a delegated cgroup v2 subtree configured through `RAY_SANDBOX_CGROUP_PATH`. `RAY_SANDBOX_IGNORE_CGROUPS=1` disables runsc's cgroup setup but doesn't remove this requirement. See [Enforce resource limits](#enforce-resource-limits).
* **Node disk filling up with images**: The image cache is capped at half of its filesystem by default. Lower the cap with `RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES` (bytes) on worker nodes, or move the cache to a larger volume. Images that running sandboxes use are never evicted, so many concurrent sandboxes on distinct large images still need that much disk.
* **Image pull failures**: Verify that the node can reach the container registry, such as Docker Hub or GHCR, or pre-populate the image cache directory at `/tmp/ray/sandbox/images`. When many nodes pull large images at once, Docker Hub's anonymous rate limits are a likely cause. See [Route Docker Hub pulls through a mirror](#route-docker-hub-pulls-through-a-mirror). An error that a registry requires credentials, or that a `docker-credential-*` helper failed, points at the node's Docker configuration. See [Pull from private registries](#pull-from-private-registries).
* **`slirp4netns` not found for `network="public"`**: Install the slirp4netns package (or a [static build](https://github.com/rootless-containers/slirp4netns/releases)) on worker nodes.
* **`public` sandboxes fail to start with a tap or namespace error**: slirp4netns needs `/dev/net/tun` in the worker's environment and a seccomp policy that allows unprivileged user+network namespace creation (`unshare -Un true` must succeed as the Ray user). The slirp4netns error appears in the sandbox's `runsc.stderr.log` and in the creation error message.
* **`nft` not found for `network="public"`**: Install nftables in the image or on the node that runs Ray, such as with `apt-get install nftables`. See [Requirements](#requirements).
* **`egress ruleset install failed` with `Protocol not supported`**: The node's kernel hasn't loaded `nf_tables`, and a user namespace can't load kernel modules. Load the module on the node with `modprobe nf_tables`, or use a kernel with `nf_tables` built in. To check a node, run the following command as the user that runs Ray, in the same container. It prints nothing when the node can load a sandbox's ruleset:

  ```bash
  unshare --user --map-root-user --net nft -f - <<'EOF'
  table inet probe {
      chain output {
          type filter hook output priority 0; policy accept;
      }
  }
  EOF
  ```

* **`dns resolver ... isn't reachable from a network='public' sandbox`**: The sandbox's egress filter dropped the resolver's traffic, or the sandbox has no route to it. Pass an IPv4 resolver that isn't a Kubernetes Service IP, such as your cloud network's resolver. See [DNS in locked-down networks](#dns-in-locked-down-networks).
* **`Failed to write the egress ruleset`**: Ray couldn't build a `public` sandbox's ruleset, so it didn't start the sandbox. The message names the cause, which is an entry in `RAY_SANDBOX_EGRESS_DENY` that isn't an IP address or network, a `dns` entry that isn't an IP address, or a failed lookup of the cluster's nodes in GCS.
* **A `public` sandbox can't reach a service on your network**: The default allowlist blocks private networks. Add the service's address and port to `cidr_allowlist`, such as `10.0.0.84:8000`. On Kubernetes clusters that translate Service IPs, list the addresses of the Service's Pods. See [Restrict egress to an allowlist](#restrict-egress-to-an-allowlist).
* **Connections from a `public` sandbox hang or fail**: The egress filter blocked them. A blocked TCP connection times out, and a blocked UDP packet fails at once with `Operation not permitted`. `public` sandboxes have no IPv6 route, so IPv6 connections fail with `Network is unreachable`.
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
