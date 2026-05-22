# Architecture Overview

This document describes how the Ray clone repository is organized and what you need to know as a developer to navigate the codebase.

## Repository Structure

The repository is a multi-language project consisting of a C++ core, several language-specific SDKs, and high-level libraries.

- `src/`: Contains the **Ray Core** implemented in C++.
    - `ray/raylet/`: The Raylet process (scheduler, object manager).
    - `ray/gcs/`: Global Control Service (metadata management).
    - `ray/common/`: Shared data structures and protocols (using Protobuf/Flatbuffers).
- `python/`: The **Python SDK** and the main interface for most users.
    - `ray/`: The `ray` Python package.
    - `ray/_private/`: Internal Python implementation details.
    - `ray/dashboard/`: The web UI and observability backend.
- `java/`: The **Java SDK**.
- `cpp/`: The **C++ SDK**.
- `rllib/`: **RLlib**, a scalable reinforcement learning library.
- `bazel/`: Bazel build configuration and external dependencies.
- `ci/`: Scripts and configurations for Continuous Integration.
- `doc/`: Documentation source files.

## Key Components

### 1. Raylet (C++)
The Raylet is the worker process that runs on each node in a Ray cluster. It is responsible for:
- **Task Scheduling**: Receiving tasks and assigning them to workers.
- **Object Management**: Storing and retrieving immutable objects.
- **Node Management**: Heartbeats and resource tracking.

### 2. GCS (Global Control Service)
The GCS is a centralized service (or a set of services) that stores metadata for the cluster, such as the location of objects and the status of actors.

### 3. Python Worker
When you run a Ray task or actor in Python, it runs inside a Python worker process managed by the Raylet.

### 4. Language-Specific SDKs
Ray provides SDKs in Python, Java, and C++. These SDKs communicate with the Raylet and GCS via standardized protocols.

## Build System (Bazel)

Ray uses **Bazel** as its primary build system. Bazel manages the compilation of the C++ core and its integration with the Python/Java/C++ SDKs.

- `WORKSPACE`: Defines external dependencies.
- `BUILD.bazel`: Defines build targets and visibility.

## Development Workflow

1. **C++ Changes**: If you modify code in `src/`, you must rebuild the core using Bazel.
2. **Python Changes**: If you modify code in `python/`, and you have an editable install (`pip install -e .`), your changes are reflected immediately (unless they involve the core).
3. **Protobuf/Flatbuffers**: Any changes to protocols in `src/ray/protobuf` or similar will require re-running the generator or a full build.

## Implementation Plan: Hostname Routing Support

This plan addresses issue #61651: Supporting Host Name routing rather than primarily relying on IP address/port.

### Objective
Allow Ray nodes to identify themselves using their hostname instead of an IP address. This is controlled by a new environment variable `RAY_NODE_USE_HOSTNAME=1`.

### Rationale
In environments with strict TLS requirements, certificates often only contain hostname Subject Alternative Names (SANs). Using hostnames for node identification ensures that gRPC TLS handshakes can be validated against these certificates.

### Proposed Changes

#### 1. Core C++ Changes
Modify `src/ray/util/network_util.cc`:
- In `GetNodeIpAddressFromPerspective`, check for the `RAY_NODE_USE_HOSTNAME` environment variable.
- If set to `"1"`, return `boost::asio::ip::host_name()`.

```cpp
// src/ray/util/network_util.cc

std::string GetNodeIpAddressFromPerspective(const std::optional<std::string> &address) {
  const char *use_hostname_env = std::getenv("RAY_NODE_USE_HOSTNAME");
  if (use_hostname_env != nullptr && std::string(use_hostname_env) == "1") {
    return boost::asio::ip::host_name();
  }
  // ... existing logic ...
}
```

#### 2. Python SDK Changes
Modify `python/ray/_private/services.py`:
- Update `get_node_ip_address()` to respect `RAY_NODE_USE_HOSTNAME`.

```python
# python/ray/_private/services.py

def get_node_ip_address(address=None):
    if os.environ.get("RAY_NODE_USE_HOSTNAME") == "1":
        return socket.gethostname()

    if ray._private.worker._global_node is not None:
        return ray._private.worker._global_node.node_ip_address
    # ...
```

#### 3. Propagation and Verification
- **Ray Start**: The `ray start` command will automatically pick up the hostname if the environment variable is set, as it relies on `services.get_node_ip_address()`.
- **GCS Registration**: Nodes will register with the GCS using their hostname.
- **gRPC Connections**: All inter-node gRPC connections will use the hostnames provided by GCS, enabling proper TLS verification.

### Verification Plan

1.  **Unit Tests**:
    - Create a test in `python/ray/tests/test_hostname_routing.py`.
    - Set `RAY_NODE_USE_HOSTNAME=1` in the environment.
    - Verify `ray.util.get_node_ip_address()` returns `socket.gethostname()`.
2.  **Cluster Integration Test**:
    - Start a head node and a worker node with `RAY_NODE_USE_HOSTNAME=1`.
    - Verify they can successfully connect and run a simple task.
    - (Optional) Verify TLS handshake success if certificates are configured.

### Considerations
- **DNS**: Ensure that all nodes in the cluster can resolve each other's hostnames.
- **Local Mode**: For `ray.init(local_mode=True)`, we should continue to default to `127.0.0.1` unless explicitly overridden, to avoid unnecessary network stack involvement.
