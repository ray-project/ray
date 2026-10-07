---
myst:
  html_meta:
    description: "How Ray assigns and discovers ports across raylet, agents, and core workers, plus GCS-side discovery via the node and actor tables."
---

(ray-port-service-discovery)=

# Port service discovery

This page describes how Ray assigns ports dynamically and how components discover them.

## Design principle

When neither you nor a start script specifies a port explicitly, Ray uses a *bind-then-report* pattern. Each component first binds to a random port and then reports the actual port.

This pattern avoids the time-of-check to time-of-use (TOCTOU) race condition. If Ray preassigned a port and passed it to a component, another process might bind to that port before the component does.

## Two-layer discovery

Ray uses two discovery mechanisms, depending on the relationship between processes:

```{list-table}
:header-rows: 1

* - Layer
  - Scope
  - Mechanism
* - **Raylet internal**
  - The raylet discovers the ports of its child processes
  - File-based for agents, interprocess communication (IPC) for workers
* - **GCS**
  - All other Ray components
  - Node table and actor table
```

## Raylet internal port discovery

The raylet spawns child processes and must discover their ports. The mechanism depends on whether the child process uses the same language as the raylet.

### File-based: Raylet ↔ agents

The raylet, written in C++, spawns agents written in Python. Communication crosses a language boundary, and files are the simplest mechanism that works reliably on both Linux and Windows and in both C++ and Python. The raylet spawns two agents:

1. The dashboard agent exposes three ports:

   - `dashboard_agent_listen_port`: Serves HTTP for the dashboard UI. The default is 52365.
   - `metrics_agent_port`: Serves gRPC for internal communication. The default is a random port.
   - `metrics_export_port`: Exports Prometheus metrics. The default is a random port.

1. The runtime environment agent manages runtime environments and exposes one port:

   - `runtime_env_agent_port`: Serves HTTP. The default is a random port.

After binding, agents write their ports to `{session_dir}/{port_name}_{node_id_hex}`. For the implementation, see [port_persistence.h](https://github.com/ray-project/ray/blob/master/src/ray/util/port_persistence.h). The raylet polls these files and waits for all agent ports before it registers the node with the GCS.

### IPC-based: Raylet ↔ core workers

The raylet, written in C++, spawns core workers, which are also written in C++. Because they share a language, they communicate over socket-based IPC with the FlatBuffers protocol. On Linux and macOS, the socket is a Unix domain socket. On Windows, it's a TCP socket on localhost.

Without a port range, which is the default, the worker binds to port 0 so that the OS picks a random port. The worker then tells the raylet the actual port.

When you set a port range with `--min-worker-port` and `--max-worker-port`, the raylet must manage the range itself. The OS only supports binding to a specific port or to port 0 for a random port. No syscall binds to an arbitrary port within a range.

The raylet maintains a `free_ports_` queue. When a worker registers, the raylet assigns it an unused port from the queue. The worker binds to that port and then confirms it with `AnnounceWorkerPort`.

At startup, each raylet independently seeds its queue with a random permutation of the configured ports. Without this randomization, every raylet that shares a network namespace and a port range starts from the lower bound of the range and deterministically contends for the same ports. Randomizing lowers the odds of a collision, but it isn't a reservation protocol. Two raylets can still pick the same port and rely on the retry path that the next paragraph describes.

If the worker fails to bind the port, for example because an external process already uses it, the worker crashes. The raylet detects the socket disconnect through [NodeManager::HandleClientConnectionError](https://github.com/ray-project/ray/blob/10869d565047ae02b398802e1efaf04109f27249/src/ray/raylet/node_manager.h), which returns the port to the queue and starts a new worker.

## GCS port discovery

Raylet internal port discovery covers only the raylet discovering the ports of its own child processes. For everything else, components query the GCS. The following sections describe two common examples.

### Node table

Each raylet registers a GcsNodeInfo with the GCS. The GcsNodeInfo contains the raylet's own ports, `node_manager_port` and `object_manager_port`. It also contains the agent ports `runtime_env_agent_port`, `metrics_agent_port`, `metrics_export_port`, and `dashboard_agent_listen_port`.

Components that query the GCS for this information include the following:

- The object manager needs the `object_manager_port` of remote nodes to pull objects across nodes.
- The core worker needs the `node_manager_port` of remote nodes for task cancellation and object recovery.
- The dashboard needs the `runtime_env_agent_port` of each node to collect runtime environment information.
- The Ray Client server needs the `runtime_env_agent_port` to set up runtime environments for client jobs.

### Actor table

Actors are stateful, so callers must reach the same worker every time. Actor method calls require a direct RPC to a specific worker.

When an actor is created, its worker address is registered with the GCS through [GcsActorManager::HandleRegisterActor](https://github.com/ray-project/ray/blob/10869d565047ae02b398802e1efaf04109f27249/src/ray/gcs/gcs_actor_manager.h). The address consists of `address.ip_address` and `address.port`. Callers query the GCS for the address and then communicate directly with the worker.
