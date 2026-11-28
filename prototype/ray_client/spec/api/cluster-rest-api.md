# Cluster REST API

The Ray Dashboard HTTP server on port `8265` serves four endpoint families for external clients:
`/api/actors`, `/api/cluster`, `/api/jobs`, and `/api/packages`.

## Design principles

- **Zero gRPC proxy:** External clients never speak Ray's internal gRPC or mailbox protocols.
  Only working directory archives and JSON payloads cross the client-cluster boundary.
- **Keepalive streaming against proxy timeouts:** Endpoints that may block on scheduling,
  `runtime_env` setup, or long-running computations (`POST /api/actors`,
  `POST /api/actors/{name}/methods/{method}`, `POST /api/cluster/node_groups/{group}/nodes` with
  `wait_ready=true`) stream periodic keepalive bytes (`" "` whitespace for unary JSON responses,
  `: ping\n\n` comments for SSE) every 1 second so intermediate load balancers and ingress
  proxies never close idle connections prematurely, while immediately detecting client disconnects.
- **Ray 2.x and Ray 3.0 compatibility:** `/api/actors`, `/api/jobs`, and `/api/packages` run on the
  Python Ray Dashboard across Ray 2.x and Ray 3.0. `/api/cluster/*` read endpoints
  (`GET /api/cluster/node_groups`, `GET /api/cluster/nodes`) use `ray.cluster` when available and
  fall back to `ray.nodes()` on Ray 2.x; `/api/cluster/*` node mutations (`POST .../nodes`,
  `DELETE /api/cluster/nodes/{node_id}`) delegate to Ray 3.0's `ray.cluster` module and return
  `501 Not Implemented` on Ray 2.x.

## Authentication

When token authentication is enabled (`RAY_AUTH_MODE=token`), all endpoints require
`Authorization: Bearer <token>`, `X-Ray-Authorization`, or the `ray-authentication-token` cookie.
Missing tokens return `401 Unauthorized`; invalid tokens return `403 Forbidden`.

## Endpoints overview

| Area | Endpoint | Purpose |
|---|---|---|
| Actors | `POST /api/actors` | Create or reattach to a root actor (`_RemoteClientRootActor`) |
| Actors | `GET /api/actors` | List active client-managed root actors |
| Actors | `GET /api/actors/{name}` | Get metadata and live status for a named actor |
| Actors | `POST /api/actors/{name}/methods/{method}` | Invoke a unary or generator (SSE) method on an actor |
| Actors | `GET /api/actors/{name}/logs` | Get captured `stdout` and `stderr` from the root actor process |
| Actors | `DELETE /api/actors/{name}` | Terminate a named actor and clean up its owned resources |
| Cluster | `GET /api/cluster/node_groups`, `GET /api/cluster/node_groups/{group}` | List node groups or inspect one node group |
| Cluster | `POST /api/cluster/node_groups/{group}/nodes` | Add detached nodes to a node group |
| Cluster | `GET /api/cluster/nodes`, `GET /api/cluster/nodes/{node_id}` | List cluster nodes or inspect one node |
| Cluster | `DELETE /api/cluster/nodes/{node_id}` | Destroy a cluster node with a reason and optional grace period |
| Jobs | `POST /api/jobs/`, `GET /api/jobs/{id}`, `GET /api/jobs/{id}/logs`, `POST /api/jobs/{id}/stop`, `DELETE /api/jobs/{id}` | Submit, monitor, fetch logs for, stop, or delete shell entrypoint jobs |
| Packages | `GET`, `PUT /api/packages/gcs/{pkg_uri}` | Check or upload content-addressed `_ray_pkg_<sha1>.zip` working directory archives |

## Actors API (`/api/actors`)

### `POST /api/actors`

Create or reattach to a root actor on the cluster.

Request JSON body:

- `name` (`str | None`): Actor name. Auto-generated as `<target_name>_<8-hex>` when omitted.
- `kind` (`"class" | "function"`, default `"class"`): Target workload kind.
- `detached` (`bool`, default `true` for `"class"`, `false` for `"function"`): Whether the root
  actor is detached or owned by the Dashboard agent process.
- `class_name` (`str | None`), `fn_name` (`str | None`): Target class or function name when
  `module_source` is provided.
- `module` (`str | None`): Module import path when loading `class_name` or `fn_name` from the
  cluster's installed environment instead of `module_source`.
- `module_source` (`str | None`): Python source defining `class_name` or `fn_name`.
- `captured_globals` (`dict[str, Any]`): Module-level JSON constants injected into the module
  namespace before executing `module_source`.
- `args` (`list[Any]`), `kwargs` (`dict[str, Any]`): Constructor arguments (`kind="class"`).
- `runtime_env` (`dict[str, Any] | None`): Runtime environment configuration.
- `node_id` (`str | None`): Target Ray node ID (hex string). Mutually exclusive with
  `label_selector`.
- `num_cpus` (`float | None`), `num_gpus` (`float | None`), `memory` (`float | None`),
  `resources` (`dict[str, float] | None`): Resource requirements. `resources` accepts unified
  keys (`"CPU"`, `"GPU"`, `"memory"`, and custom resources).
- `label_selector` (`dict[str, str] | None`): Node label placement constraints.
- `max_concurrency` (`int`, default `1`): Maximum concurrent method executions (`>= 1`).
- `ttl_seconds` (`float | None`): Idle TTL in seconds (`1800.0` default for new `"class"` actors
  and `300.0` for `"function"` actors when `"ttl_seconds"` is omitted; explicit `null` disables
  idle reaping; `<= 0` is rejected with `400 Bad Request`; omitting `"ttl_seconds"` on reattach
  preserves the existing actor's TTL).
- `get_if_exists` (`bool | "recreate"`, default `true` for `"class"`, `false` for `"function"`):
  Reattachment policy when an actor with `name` already exists.
- `timeout` (`float | None`): Creation and `__init__` timeout in seconds.

Responses:

- **`400 Bad Request`** (returned before streaming begins): Invalid specification fields (such as
  missing `class_name`/`fn_name`, `ttl_seconds <= 0`, `max_concurrency < 1`, non-JSON
  `args`/`kwargs`, conflicting resources, or both `node_id` and `label_selector` set). Body:
  `{"ok": false, "error": {"exc_type": "ValueError" | "TypeError", "exc_message": "..."}}`.
- **`200 OK` (`application/json`, with 1s `" "` whitespace keepalives)**:
  - On success:
    ```json
    {
      "ok": true,
      "name": "trainer",
      "class_name": "Trainer",
      "kind": "class",
      "namespace": "ray_client",
      "status": "ALIVE",
      "detached": true,
      "spec_hash": "8f3c...",
      "active_calls": 0,
      "ttl_seconds": 1800.0,
      "remaining_ttl_seconds": 1800.0,
      "created_at": 1730000000.0,
      "last_active": 1730000000.0,
      "generator_methods": ["stream_steps"],
      "created": true,
      "spec_mismatch": false
    }
    ```
  - On constructor exception, conflict (`get_if_exists=false`), or creation timeout:
    ```json
    {
      "ok": false,
      "error": {
        "exc_type": "ValueError",
        "exc_message": "...",
        "traceback": "Traceback (most recent call last): ...",
        "timed_out": false
      },
      "logs": "..."
    }
    ```

### `GET /api/actors` and `GET /api/actors/{name}`

- `GET /api/actors` returns a JSON array `[<actor_info>, ...]` (`200 OK`) of metadata dictionaries
  for all active client-managed root actors.
- `GET /api/actors/{name}` returns `{"ok": true, ...<actor_info>}` (`200 OK`), or `404 Not Found`
  (`{"ok": false, "error": {"exc_type": "KeyError", "exc_message": "Actor '<name>' does not exist."}}`)
  if no actor with `name` exists.

### `POST /api/actors/{name}/methods/{method}`

Invoke `{method}` on `{name}`.

Request JSON body:

- `args` (`list[Any]`, default `[]`): Positional arguments.
- `kwargs` (`dict[str, Any]`, default `{}`): Keyword arguments.
- `timeout` (`float | None`): Execution timeout in seconds.
- `stream` (`bool`, default `false`, or query parameter `?stream=true`): When `true`, forces
  Server-Sent Events (`text/event-stream`) and fails with `TypeError` if `{method}` is not a
  generator. When `false`, generator methods are still auto-detected and streamed over SSE.

Responses:

- **`400 Bad Request`**: Malformed JSON or invalid `args`/`kwargs`/`timeout` types.
- **`404 Not Found`**: Actor `{name}` does not exist
  (`{"ok": false, "error": {"exc_type": "KeyError", "exc_message": "Actor '<name>' does not exist."}}`).
- **Unary response (`200 OK`, `application/json`, with 1s `" "` whitespace keepalives)**:
  - Success: `{"ok": true, "result": <json_value>}`
  - Error or timeout:
    `{"ok": false, "error": {"exc_type": "...", "exc_message": "...", "traceback": "...", "timed_out": <bool>}, "logs": "..."}`
- **Streaming response (`200 OK`, `text/event-stream; charset=utf-8`)**:
  - Periodic `: ping\n\n` keepalive comments while waiting for the next yielded item.
  - Each yielded item: `data: {"ok": true, "item": <json_value>}\n\n`
  - Normal generator completion: `data: {"ok": true, "done": true}\n\n`
  - Generator exception or timeout:
    `data: {"ok": false, "error": {"exc_type": "...", "exc_message": "...", "traceback": "...", "timed_out": <bool>}, "logs": "..."}\n\n`

### `GET /api/actors/{name}/logs`

Returns `{"ok": true, "name": "<name>", "logs": "<stdout_and_stderr>"}` (`200 OK`), or
`404 Not Found`
(`{"ok": false, "error": {"exc_type": "KeyError", "exc_message": "Actor '<name>' does not exist."}}`)
if `{name}` does not exist.

### `DELETE /api/actors/{name}`

Terminates `{name}` (running any user-defined `close()` or `__exit__()` hook first and removing
owned placement groups and child actors). Returns `{"ok": true, "name": "<name>", "killed": true}`
(`200 OK`), or `404 Not Found` (`{"ok": false, "killed": false, "name": "<name>"}`) if `{name}`
does not exist.

## Cluster Node Management API (`/api/cluster`)

On error, `/api/cluster` endpoints return
`{"ok": false, "error": {"exc_type": "...", "exc_message": "..."}}` with the corresponding HTTP
status code (`400`, `404`, `408`, `409`, or `501`).

### `GET /api/cluster/node_groups` and `GET /api/cluster/node_groups/{group}`

- `GET /api/cluster/node_groups` returns a JSON array `[<NodeGroup>, ...]` (`200 OK`, always
  including the `"head"` group).
- `GET /api/cluster/node_groups/{group}` returns a `<NodeGroup>` JSON object (`200 OK`) or
  `404 Not Found`.

Each `NodeGroup` object has:

```json
{
  "name": "worker-group",
  "resources": {"CPU": 8.0},
  "labels": {"ray.io/node-group": "worker-group"},
  "max_nodes": 16
}
```

### `POST /api/cluster/node_groups/{group}/nodes`

Add detached nodes to `{group}`.

Request JSON body:

- `count` (`int`, default `1`, must be `>= 1`): Number of nodes to add.
- `wait_ready` (`bool`, default `false`): Whether to wait until all added nodes transition to
  `ALIVE`.
- `timeout_s` (`float | None`): Timeout in seconds when `wait_ready=true`.

Responses:

- `200 OK`:
  - When `wait_ready=false`: Returns a JSON array `[<NodeSummary>, ...]`.
  - When `wait_ready=true`: Validates parameters and provisions the detached nodes first (so
    `400`/`404`/`409`/`501` errors return immediately with their HTTP status code), then streams
    1s `" "` whitespace keepalives while waiting for all added nodes to become `ALIVE` and writes
    the refreshed `[<NodeSummary>, ...]` JSON array (or
    `{"ok": false, "status": 408 | 400, "error": {"exc_type": "...", "exc_message": "...", "message": "...", "timed_out": <bool>}}`
    if waiting times out or a node dies during provisioning, after destroying the newly added
    nodes).
- `400 Bad Request`: `count < 1`, non-boolean `wait_ready`, or `timeout_s <= 0`.
- `404 Not Found`: Unknown node group `{group}`.
- `408 Request Timeout`: `wait_ready=true` timed out waiting for nodes to become `ALIVE`.
- `409 Conflict`: `{group}` is `"head"` or adding `count` nodes exceeds the group's `max_nodes`
  limit (`MaxNodesExceededError`).
- `501 Not Implemented`: Cluster does not provide `ray.cluster` (Ray 2.x).

### `GET /api/cluster/nodes` and `GET /api/cluster/nodes/{node_id}`

- `GET /api/cluster/nodes` accepts optional query parameters:
  - `node_group` (`str`): Filter by `group_name`.
  - `state` (comma-separated `str`): Filter by state (`PENDING`, `ALIVE`, `TERMINATING_SOON`,
    `DEAD`).
  - `include_dead` (`"true" | "false"`, default `"false"`): Include `DEAD` nodes when `state` is
    not specified.
  Returns a JSON array `[<NodeSummary>, ...]` (`200 OK`).
- `GET /api/cluster/nodes/{node_id}` returns a `<NodeSummary>` JSON object (`200 OK`) or
  `404 Not Found`.

Each `NodeSummary` object matches the Ray 3.0 `Node` / `NodeStatus` schema:

```json
{
  "id": "a1b2c3...",
  "group_name": "head",
  "state": "ALIVE",
  "resources": {"CPU": 8.0, "memory": 17179869184.0},
  "labels": {"ray.io/node-id": "a1b2c3...", "ray.io/node-group": "head"},
  "detached": true,
  "death_reason": null,
  "termination_deadline": null
}
```

### `DELETE /api/cluster/nodes/{node_id}`

Destroy a node by ID.

Request JSON body:

- `reason` (`str`, required, non-empty): Human-readable explanation for destroying the node.
- `grace_period_s` (`float`, optional, must be `>= 0`): Grace period in seconds during which an
  `ALIVE` node enters `TERMINATING_SOON` before becoming `DEAD`.

Responses:

- `200 OK`: Returns the updated `NodeSummary` merged with `{"ok": true, "destroyed": true}`.
- `400 Bad Request`: Empty `reason`, negative `grace_period_s`, or attempting to destroy the head
  node.
- `404 Not Found`: Unknown `node_id`.
- `501 Not Implemented`: Cluster does not provide `ray.cluster` and the target node is not the
  head node.
