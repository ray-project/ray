.. meta::
   :description: Execute untrusted model-generated code and agent tool calls safely with Ray Sandboxes using lightweight gVisor kernel isolation.

.. _ray-core-sandboxes:

=============================
Ray Sandboxes (Experimental)
=============================

Ray Sandboxes use `gVisor <https://gvisor.dev/docs/>`_ to provide lightweight, kernel-isolated execution environments for running untrusted code and agent tool calls safely on Ray clusters.

.. warning::

   This is an **experimental** library (``ray.experimental.sandbox``). Experimental APIs are subject to change or removal in future releases prior to General Availability (GA) graduation.

Background
==========

The ability to sandbox model-generated code is critical for agentic RL and LLM agents. However, executing untrusted code directly within Ray worker processes
or host environments introduces critical security and stability risks. Ray Sandboxes solve this challenge by running lightweight, kernel-isolated sandboxes
directly on Ray worker nodes using `gVisor <https://gvisor.dev/docs/>`_ (``runsc``). The library allows developers to scale and manage sandbox environments
using familiar Ray concepts and primitives.

What is gVisor?
---------------

`gVisor <https://gvisor.dev/docs/>`_ is an open-source application kernel written in Go that provides lightweight, defense-in-depth isolation for containers. Developed by Google, gVisor implements a substantial portion of the Linux system call interface in user space, acting as an isolation barrier between untrusted applications and the host operating system kernel.

Unlike standard container runtimes (such as Docker or ``runc``) where containers share the host Linux kernel directly, gVisor intercepts system calls made by containerized processes before they reach the host.
Due to the daemonless nature of gVisor and its ability to run as a non-privileged user, it is easy to deploy and manage on top of existing container orchestrators like Kubernetes.

Benefits of gVisor
^^^^^^^^^^^^^^^^^^

Using gVisor as the execution backend for Ray Sandboxes provides key advantages over traditional Virtual Machines (VMs) and standard container runtimes:

* **Strong Kernel Isolation:** Because untrusted code interacts with the user-space kernel rather than the host Linux kernel, the attack surface for host kernel vulnerabilities and container breakout exploits is dramatically minimized.
* **Sub-100ms Startup Latency:** Unlike full VMs or MicroVMs that require booting guest OS kernels and managing heavy disk images, gVisor sandboxes boot in tens of milliseconds. This enables high-frequency, low-latency execution loops needed for iterative RL rollouts and agent tool calls.
* **Dense Bin Packing & Low Footprint:** Each gVisor sandbox has a minimal memory overhead and near-zero idle CPU usage. Ray worker nodes can densely pack hundreds of concurrent sandboxes alongside standard Ray tasks and actors.
* **Rootless & Container-Native:** gVisor runs entirely in user space without requiring host root privileges, the Docker daemon, or nested virtualization hardware extensions. This makes it straightforward to run securely inside existing Kubernetes Ray worker Pods and cloud container environments.

Architecture
============

The Ray Sandboxing subsystem is organized into hierarchical layers:

.. code-block:: text

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

Core Components
---------------

1. **High-Level Helper (:func:`ray.experimental.sandbox.create <ray.experimental.sandbox.create>`):** Spawns a Ray actor that encapsulates the sandbox lifecycle and returns an ``ActorHandle``.
2. **Sandbox Actor (:class:`ray.experimental.sandbox.Sandbox <ray.experimental.sandbox.Sandbox>`):** A Ray actor managing scheduling, lifecycle, command execution, and file I/O for an isolated sandbox instance.
3. **Sandbox Runtime (:class:`ray.experimental.sandbox.runtime.SandboxRuntime <ray.experimental.sandbox.runtime.SandboxRuntime>`):** A low-level abstraction that manages the lifecycle of local sandboxes, image pulling/caching, and interactions with the execution backend.
4. **gVisor Backend (``ray.experimental.sandbox.backend.GVisorSandboxBackend``):** Executes commands and isolates processes via gVisor's OCI runtime (``runsc``).
5. **Image Manager (``ray.experimental.sandbox.image_manager.ImageManager``):** Automatically pulls container images (e.g. from Docker Hub, GHCR, or local tar archives), extracts root filesystems into ``/tmp/ray/sandbox/images``, and builds OCI ``config.json`` runtime specifications.

Requirements
============

To use Ray Sandboxes on your Ray nodes:

* **Linux OS:** Linux x86_64 or arm64.
* **gVisor (``runsc``):** The ``runsc`` binary must be installed on worker nodes and accessible in the system ``$PATH``.
* **Ray 2.58.0+** with the ``ray.experimental.sandbox`` package.

To install ``runsc`` on a Linux worker node, see the `gVisor installation guide <https://gvisor.dev/docs/user_guide/install/>`_.


Usage Patterns and Examples
===========================

Example: Basic Sandbox Creation and Command Execution
-------------------------------------------------------

Use ``sandbox.create()`` to start an isolated environment from any container image. The function returns a Ray ``ActorHandle`` representing the sandbox actor.

.. code-block:: python

   import ray
   from ray.experimental import sandbox

   ray.init()

   # Create a sandbox with 1 CPU core and 512MB RAM
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

Example: File Operations (Reading, Writing, Uploading, Downloading)
---------------------------------------------------------------------

You can write source files directly into the sandbox or upload local files from the host before execution. By default, the root filesystem is read-only, and the configured ``workdir`` (e.g. ``/workspace``) is the writable scratch space.

.. code-block:: python

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

Example: Direct Actor Creation with Resource Scheduling
---------------------------------------------------------

Because ``Sandbox`` is a standard Ray Actor, you can instantiate it directly with Ray actor scheduling options such as ``num_cpus``, ``memory``, and custom accelerator or placement constraints.

.. code-block:: python

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

Example: Custom Actors using ``SandboxRuntime``
-----------------------------------------------------------

If you are building custom RL environment actors or specialized rollout workers, you can embed ``SandboxRuntime`` directly inside your custom actors for fine-grained sandbox lifecycle control:

.. code-block:: python

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


API Reference
=============

For detailed signatures, parameters, and return types, see the :ref:`Ray Sandbox API reference <ray-sandbox-ref>`.


Security & Isolation Model
==========================

Ray Sandboxes implement multi-layered defense-in-depth isolation:

1. **System Call Interception:** gVisor's Sentry application kernel intercepts system calls in user space, isolating untrusted code from the host Linux kernel.
2. **Read-Only Root Filesystem:** Base container filesystems are mounted read-only (``readonly=True``) with an isolated copy-on-write overlay directory per sandbox.
3. **Restricted Working Directory:** Only the explicit ``workdir`` (e.g. ``/workspace``) is mounted read-write for application artifacts.
4. **Network Containment:** By default, ``network="none"`` disables all outbound network interfaces, preventing untrusted code from making external API calls or scanning the internal cluster network.
5. **Resource Quotas:** CPU quotas and memory limits are enforced via cgroups, preventing CPU starvation and out-of-memory (OOM) conditions from affecting other Ray actors.

Troubleshooting
===============

* **``runsc not found in PATH``:** Verify that gVisor's ``runsc`` binary is installed on all Ray worker nodes and located in a directory present in the system ``PATH`` (e.g., ``/usr/local/bin/runsc``).
* **Cgroup or Permission Errors:** If running in containerized environments (such as Kubernetes) without root permissions, ensure ``rootless=True`` is set (the default). If running in environments where cgroups are restricted, you can set the environment variable ``RAY_SANDBOX_IGNORE_CGROUPS=1``.
* **Image Pull Failures:** Ensure the node has internet access to reach the container registry (e.g. Docker Hub, GHCR), or pre-populate the image cache directory at ``/tmp/ray/sandbox/images``.

Next Steps
==========

* Deploy Ray Sandboxes on Kubernetes with KubeRay: :ref:`kuberay-sandboxing`.
* Learn more about `gVisor <https://gvisor.dev/docs/>`_.
* Explore :ref:`Resource Isolation With Cgroup v2 <resource-isolation>` to isolate Ray system processes from worker processes.
