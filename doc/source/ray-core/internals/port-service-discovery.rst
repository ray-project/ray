.. _ray-port-service-discovery:

Port Service Discovery
======================

This document describes Ray's dynamic port assignment and discovery.

Design Principle
----------------

When a port is not explicitly specified by the user or start script, Ray uses a
**bind-then-report** pattern: components first bind to a random port, then report
the actual port.

This avoids the TOCTOU (Time-Of-Check-Time-Of-Use) race condition: if Ray preassigned
a port and passed it to a component, another process might bind to that port before
the component does.

Two-Layer Discovery
-------------------

Ray uses two discovery mechanisms based on process relationships:

.. list-table::
   :header-rows: 1

   * - Layer
     - Scope
     - Mechanism
   * - **Raylet Internal**
     - Raylet discovers child process ports
     - File-based (Agents) or IPC (Workers)
   * - **GCS**
     - All other Ray components
     - Node Table / Actor Table

Raylet Internal Port Discovery
------------------------------

Raylet spawns child processes and needs to discover their ports. The mechanism
depends on the language boundary.

File-Based: Raylet ↔ Agents
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Raylet (C++) spawns Agents (Python). Cross-language communication is needed, and
files are the simplest solution that works reliably across both platforms (Linux/Windows)
and languages (C++/Python). Raylet spawns two agents:

1. **Dashboard Agent** - exposes three ports:

   - ``dashboard_agent_listen_port``: HTTP for Dashboard UI (default: 52365)
   - ``metrics_agent_port``: gRPC for internal communication (default: random)
   - ``metrics_export_port``: Prometheus metrics export (default: random)

2. **Runtime Env Agent** - manages runtime environments:

   - ``runtime_env_agent_port``: gRPC (default: random)

After binding, agents write their ports to
``{session_dir}/{port_name}_{node_id_hex}`` (see `port_persistence.h <https://github.com/ray-project/ray/blob/master/src/ray/util/port_persistence.h>`_).
Raylet polls these files and waits for all agent ports before registering the node to GCS.

IPC-Based: Raylet ↔ Core Workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Raylet (C++) spawns Core Workers (C++). Same language, so they use socket-based
IPC with Flatbuffers protocol. On Linux/macOS this is a Unix domain socket; on
Windows it's a TCP socket on localhost.

**Default (no port range):**

Worker binds to port 0 (OS picks a random port), then tells Raylet the actual port.

**With port range (** ``--min-worker-port`` **/** ``--max-worker-port`` **):**

OS only supports binding to a specific port or port 0 (random). There's no syscall
for "bind to any port within this range". So Raylet must manage the range itself.

Raylet maintains a ``free_ports_`` queue. When a worker registers, Raylet assigns
it an unused port from the queue. The worker binds, then confirms with ``AnnounceWorkerPort``.

If the worker fails to bind the port (e.g., port already in use by external process),
it crashes. Raylet detects the socket disconnect via
`NodeManager::HandleClientConnectionError <https://github.com/ray-project/ray/blob/10869d565047ae02b398802e1efaf04109f27249/src/ray/raylet/node_manager.h>`_, which returns the port to the queue and starts a new worker.

GCS Port Discovery
------------------

Raylet Internal Port Discovery only serves Raylet discovering its own children's ports.
For everything else they query GCS.

Two common examples:

Node Table
~~~~~~~~~~

Each Raylet registers a GcsNodeInfo to GCS, containing its own ports
(``node_manager_port``, ``object_manager_port``) and agent ports
(``runtime_env_agent_port``, ``metrics_agent_port``, ``dashboard_agent_listen_port``).

Other components query GCS for this information:

- Object Manager needs ``object_manager_port`` of remote nodes to pull objects across nodes
- Core Worker needs ``node_manager_port`` of remote nodes for task cancellation, and object recovery
- Dashboard needs ``runtime_env_agent_port`` of each node to collect runtime env info
- Ray Client Server needs ``runtime_env_agent_port`` to set up runtime environments for client jobs
- ...

Actor Table
~~~~~~~~~~~

Actors are stateful—callers must reach the same worker every time. Actor method
calls require direct RPC to a specific worker.

When an actor is created, its worker address (``address.ip_address`` and ``address.port``)
is registered to GCS via `GcsActorManager::HandleRegisterActor <https://github.com/ray-project/ray/blob/10869d565047ae02b398802e1efaf04109f27249/src/ray/gcs/gcs_actor_manager.h>`_.
Callers query GCS to get the address, then communicate directly with the worker.
