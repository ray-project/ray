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

To install `runsc` on a Linux worker node, see the [gVisor installation guide](https://gvisor.dev/docs/user_guide/install/).

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

For advanced workloads, you might need to configure low-level runtime options such as custom host mounts, Linux capabilities, or custom network and DNS settings. Use the `_oci_spec_transform_fn` parameter to inspect and modify the generated [OCI runtime specification](https://github.com/opencontainers/runtime-spec) dictionary before Ray passes it to gVisor (`runsc`).

:::{note}
`_oci_spec_transform_fn` is an experimental hook for advanced use cases. The Ray project is designing first-class configuration APIs for Ray Sandboxes, such as higher-level volume mount and capability abstractions, and this hook is likely to change once those land. To help shape them, open an issue describing your use case.
:::

The `_oci_spec_transform_fn` callable receives the fully generated OCI specification dictionary. It can mutate the dictionary in place or return a modified one. Common use cases include the following:

* **Host mounts**: Mount host directories, read-only datasets, or model weights into the sandbox container.
* **Namespace and mount details**: Configure namespace or mount behavior that the first-class options don't cover.

Internet access, DNS, and Linux capabilities each have a first-class option: `network=`, `dns=`, and `capabilities=`. Pass `capabilities=[]` to run with no capabilities at all. Reserve the hook for network or capability configurations those options don't reach. See [Networking and DNS](#networking-and-dns).

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

(ray-sandbox-modal-api)=

## Modal-compatible API

`ray.experimental.sandbox.modal` presents the [Modal Sandbox API](https://modal.com/docs/guide/sandbox) on top of Ray Sandboxes. Use it to move an existing Modal-based agent or RL workload onto a Ray cluster without rewriting its call sites, or when you want a live process handle rather than the buffered `ExecResult` that {func}`~ray.experimental.sandbox.create` returns.

The two APIs differ in shape. The core API gives you an `ActorHandle`, so every call goes through `ray.get(...)`. The Modal-compatible API gives you an object with ordinary methods, and `exec()` returns a process you can stream from and write to while it runs:

```python
from ray.experimental.sandbox import modal

sandbox = modal.Sandbox.create(image="python:3.13-slim", timeout=120)

# Output arrives as the command produces it, not only when it exits.
process = sandbox.exec("bash", "-c", "for i in 1 2 3; do echo step $i; sleep 1; done")
for line in process.stdout:
    print(line, end="")
assert process.wait() == 0

sandbox.terminate()
```

### Streams and stdin

`exec()` returns a `ContainerProcess` whose `stdout` and `stderr` you can read whole or iterate, and whose `stdin` accepts input while the command runs:

```python
process = sandbox.exec("cat")
process.stdin.write(b"piped in\n")
process.stdin.write_eof()
process.stdin.drain()
print(process.stdout.read())  # "piped in\n"
process.wait()
```

Pass `stdout=modal.StreamType.DEVNULL` to discard a stream, or `StreamType.STDOUT` to have it printed locally as it arrives. Use `text=False` for bytes instead of decoded text, and `bufsize=1` to iterate a line at a time.

A `StreamReader` is an *iterable*, not a restartable one: iterating it a second time yields nothing, because the first pass consumed the stream.

### Filesystem

`sandbox.filesystem` is a full path-based namespace. All paths must be absolute:

```python
sandbox.filesystem.write_text("hello\n", "/tmp/hello.txt")
print(sandbox.filesystem.read_text("/tmp/hello.txt"))

info = sandbox.filesystem.stat("/tmp/hello.txt")
print(info.name, info.size, info.permissions)

for entry in sandbox.filesystem.list_files("/tmp"):
    print(entry.name, entry.type)

sandbox.filesystem.make_directory("/tmp/a/b/c")
sandbox.filesystem.copy_from_local("local.json", "/tmp/input.json")
sandbox.filesystem.copy_to_local("/tmp/output.txt", "local_output.txt")
sandbox.filesystem.remove("/tmp/a", recursive=True)
```

Failures raise typed errors — `SandboxFilesystemNotFoundError`, `SandboxFilesystemIsADirectoryError`, `SandboxFilesystemNotADirectoryError`, `SandboxFilesystemDirectoryNotEmptyError`, `SandboxFilesystemPathAlreadyExistsError`, and `SandboxFilesystemPermissionError` — all deriving from `SandboxFilesystemError`.

Note that the write methods take the data first, matching Modal: `write_text(data, remote_path)`.

### Async

Every method blocks by default and also carries an `.aio` variant that runs on your own event loop:

```python
import asyncio
from ray.experimental.sandbox import modal

async def main():
    sandbox = await modal.Sandbox.create.aio(image="python:3.13-slim")
    process = await sandbox.exec.aio("echo", "hello")
    async for line in process.stdout:
        print(line, end="")
    await process.wait.aio()
    await sandbox.terminate.aio()

asyncio.run(main())
```

Don't mix the two surfaces on one object. A stream first iterated through the blocking API holds state bound to the internal event loop that drives it.

### Exit codes

`Sandbox.returncode` reflects the most recent `wait()` or `poll()` and follows Modal's conventions:

| Outcome | Exit code |
| --- | --- |
| Main process exited normally | Its own exit code |
| Main process killed by signal *N* | `128 + N` |
| Sandbox hit its `timeout` | `124`, and `wait()` raises `SandboxTimeoutError` |
| Sandbox was terminated while running | `137`, and `wait()` raises `SandboxTerminatedError` |
| `exec(timeout=...)` elapsed | `-1` on that process, with no exception raised |

`ContainerProcess.returncode` differs deliberately from `Sandbox.returncode`: it raises `InvalidError` until you call `wait()`. To check a still-running process without blocking, use `poll()`.

### What isn't supported

A Ray sandbox is owned by the handle that created it and there's no hosted control plane behind it, so the parts of Modal's API that depend on one raise `NotImplementedError` rather than failing quietly:

| Modal feature | Status |
| --- | --- |
| `Sandbox.create`, `exec`, `wait`, `poll`, `terminate`, `returncode` | Supported |
| `stdout` / `stderr` / `stdin`, `StreamType`, `text`, `bufsize` | Supported |
| `filesystem.*` except `watch()` | Supported |
| `App`, and the `app` / `name` arguments | Accepted and ignored |
| `Image` | Supported as a reference to a registry image or a local OCI tar. Layer builders such as `pip_install()` aren't available. |
| `cpu`, `memory`, `gpu`, `timeout`, `workdir`, `env`, `block_network` | Supported. `gpu` uses the count only, since Ray schedules on GPU count rather than model. |
| `from_id`, `from_name`, `list`, `get_tags`, `set_tags` | Not supported — sandboxes aren't registered anywhere |
| `tunnels`, `create_connect_token`, and the `*_ports` arguments | Not supported — the backend publishes no ports |
| `snapshot_filesystem`, `snapshot_directory`, `mount_image`, `unmount_image` | Not supported |
| `secrets`, `volumes`, `network_file_systems`, `proxy` | Not supported |
| `cloud`, `region`, `readiness_probe`, `idle_timeout`, `pty` | Not supported |
| `filesystem.watch()` | Not supported — needs inotify inside the sandbox |

Two behavioral differences worth knowing:

* **Writable by default.** `Sandbox.create()` here defaults to `readonly=False`, so the filesystem is writable like Modal's. Writes land in a per-sandbox copy-on-write overlay, so the base image is never modified and sandboxes sharing an image can't see each other's changes. The core {func}`~ray.experimental.sandbox.create` API defaults to `readonly=True` instead.
* **Network on by default.** Modal sandboxes have internet access unless you pass `block_network=True`, so this API defaults to `network="public"`. The core API defaults to `network="none"`.

The filesystem layer runs POSIX shell commands inside the sandbox, so the image needs `/bin/sh` and the usual `stat`, `readlink`, and `cat` utilities. Both busybox-based and GNU coreutils images work; a distroless image with no shell doesn't.

## Networking and DNS

Sandboxes support four network modes. The default is `none`, which follows the safe-defaults principle. Use `public` when a sandbox needs internet access.

| Mode | Network access | `/etc/resolv.conf` | Security property |
| --- | --- | --- | --- |
| `none` *(default)* | None | untouched | No egress. |
| `public` | Host egress | Generated from `dns` (default `8.8.8.8`, `1.1.1.1`), mounted read-only | Egress works, but the sandbox inherits nothing from the host's resolver configuration. No internal search domains, resolver addresses, or `ndots` options leak in, and the sandbox config stays portable across clusters. |
| `host` | Full host network identity | Host's own file, mounted read-only (`dns=` overrides it) | Strictly more permissive than `public`. The sandbox can reach anything the node can reach, including internal networks and node-local services. Use `public` for untrusted code. |
| `sandbox` | gVisor netstack | untouched | Requires `rootless=False`. runsc doesn't support the sandbox netstack in rootless mode. |

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

Some VPCs block outbound port 53 to public resolvers, where the `public` defaults can't resolve. Pass your internal resolver instead with `network="public", dns=["10.0.0.2"]`. If that isn't an option, fall back to `network="host"`, which uses the host's `/etc/resolv.conf`, at the cost of full host network identity. Configure anything beyond that through the OCI spec. See [Pass custom OCI configurations to gVisor](#pass-custom-oci-configurations-to-gvisor).

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
* **Image manager (`ray.experimental.sandbox.image_manager.ImageManager`)**: Automatically pulls container images from sources such as Docker Hub, GHCR, or local tar archives, extracts root filesystems into `/tmp/ray-<uid>/sandbox/images`, and builds OCI `config.json` runtime specifications.

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
* **Image pull failures**: Verify that the node can reach the container registry, such as Docker Hub or GHCR, or pre-populate the image cache directory at `/tmp/ray-<uid>/sandbox/images`.

## Next steps

* See {ref}`kuberay-sandboxing` to deploy Ray Sandboxes on Kubernetes with KubeRay.
* Learn more about [gVisor](https://gvisor.dev/docs/).
* Explore {ref}`resource-isolation` to isolate Ray system processes from worker processes.
