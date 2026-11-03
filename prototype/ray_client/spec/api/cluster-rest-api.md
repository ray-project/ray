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
- **Ray 2.x and Ray 3.0 compatibility:** `/api/actors`, `/api/jobs`, and `/api/packages` work on
  both Ray 2.x and Ray 3.0 (`RAY_BACKEND=rust`). `/api/cluster/*` node mutations delegate to Ray
  3.0's `ray.cluster` module and return `501 Not Implemented` on Ray 2.x.

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

- `name` (`str | None`): Actor name. Auto-generated as `actor_<10-hex>` when omitted.
- `kind` (`"class" | "function"`, default `"class"`): Target workload kind.
- `detached` (`bool`, default `true` for `"class"`, `false` for `"function"`): Whether the root
  actor is detached or owned by the Dashboard agent process.
- `class_name` (`str | None`), `fn_name` (`str | None`): Target class or function name when
  `module_source` is provided.
- `module` (`str | None`): Dotted import path (`"pkg.mod:Attr"` or `"pkg.mod.Attr"`) when
  loading from the cluster's installed environment instead of `module_source`.
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
- `ttl_seconds` (`float | None`): Idle TTL in seconds (`1800.0` default for new `"class"` actors;
  `<= 0` disables idle reaping; `null` on reattach preserves the existing actor's TTL).
- `get_if_exists` (`bool | "recreate"`, default `true` for `"class"`, `false` for `"function"`):
  Reattachment policy when an actor with `name` already exists.
- `timeout` (`float | None`): Creation and `__init__` timeout in seconds.

Responses:

- **`400 Bad Request`** (returned before streaming begins): Invalid specification fields (such as
  missing `module_source`/`module`, `max_concurrency < 1`, non-JSON `args`/`kwargs`, or both
  `node_id` and `label_selector` set). Body:
  `{"ok": false, "error": {"exc_type": "ValueError" | "TypeError", "exc_message": "..."}}`.
- **`200 OK` (`application/json`, with 1s `" "` whitespace keepalives)**:
  - On success:
    ```json
    {
      "ok": true,
      "name": "trainer",
      "actor_id": "a1b2c3...",
      "node_id": "d4e5f6...",
      "class_name": "Trainer",
      "module": "inline",
      "status": "ALIVE",
      "detached": true,
      "created": true,
      "spec_mismatch": false,
      "spec_hash": "8f3c...",
      "max_concurrency": 1,
      "ttl_seconds": 1800.0,
      "created_at": 1730000000.0,
      "last_active_at": 1730000000.0
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

- `GET /api/actors` returns `{"ok": true, "actors": [<actor_info>, ...]}` for all active
  client-managed root actors.
- `GET /api/actors/{name}` returns `{"ok": true, ...<actor_info>}` (`200 OK`), or `404 Not Found`
  (`{"ok": false, "error": "Actor '<name>' not found"}`) if no actor with `name` exists.

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
- **`404 Not Found`**: Actor `{name}` does not exist (`{"ok": false, "error": {"exc_type": "KeyError", "exc_message": "..."}}`).
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
`404 Not Found` if `{name}` does not exist.

### `DELETE /api/actors/{name}`

Terminates `{name}` (running any user-defined `close()` or `__exit__()` hook first and removing
owned placement groups and child actors). Returns `{"ok": true, "name": "<name>", "killed": true}`
(`200 OK`), or `404 Not Found` (`{"ok": false, "name": "<name>", "killed": false}`) if `{name}`
does not exist.

## Cluster Node Management API (`/api/cluster`)

All `/api/cluster` responses wrap payloads in `{"ok": true, ...}` on success and
`{"ok": false, "error": {"exc_type": "...", "exc_message": "..."}}` on error.

### `GET /api/cluster/node_groups` and `GET /api/cluster/node_groups/{group}`

- `GET /api/cluster/node_groups` returns `{"ok": true, "node_groups": [<NodeGroup>, ...]}`
  (always including the `"head"` group).
- `GET /api/cluster/node_groups/{group}` returns `{"ok": true, ...<NodeGroup>}` (`200 OK`) or
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
- `wait_ready` (`bool`, default `false`): Whether to wait until all added nodes leave `PENDING`.
- `timeout_s` (`float | None`): Timeout in seconds when `wait_ready=true`.

Responses:

- `200 OK` (streams 1s `" "` whitespace keepalives when `wait_ready=true`):
  `{"ok": true, "nodes": [<NodeSummary>, ...]}`.
- `400 Bad Request`: `count < 1`, `timeout_s <= 0`, or `{group}` is `"head"`.
- `404 Not Found`: Unknown node group `{group}`.
- `408 Request Timeout`: `wait_ready=true` timed out waiting for nodes to become `ALIVE`.
- `409 Conflict`: Request exceeds the group's `max_nodes` limit (`NodeGroupLimitExceeded`).
- `501 Not Implemented`: Cluster does not provide `ray.cluster` (Ray 2.x).

### `GET /api/cluster/nodes` and `GET /api/cluster/nodes/{node_id}`

- `GET /api/cluster/nodes` accepts optional query parameters:
  - `node_group` (`str`): Filter by `group_name`.
  - `state` (repeatable or comma-separated `str`): Filter by state (`PENDING`, `ALIVE`,
    `TERMINATING_SOON`, `DEAD`).
  - `include_dead` (`"true" | "false"`, default `"false"`): Include `DEAD` nodes when `state` is
    not specified.
  Returns `{"ok": true, "nodes": [<NodeSummary>, ...]}`.
- `GET /api/cluster/nodes/{node_id}` returns `{"ok": true, ...<NodeSummary>}` (`200 OK`) or
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

Request JSON body (or query parameters):

- `reason` (`str`, required, non-empty): Human-readable explanation for destroying the node.
- `grace_period_s` (`float`, default `0.0`, must be `>= 0`): Grace period in seconds during which
  the node enters `TERMINATING_SOON` before becoming `DEAD`.

Responses:

- `200 OK`: Returns the updated `NodeSummary` merged with `{"ok": true, "destroyed": true}`.
- `400 Bad Request`: Empty `reason`, negative `grace_period_s`, or attempting to destroy the head
  node.
- `404 Not Found`: Unknown `node_id`.
- `501 Not Implemented`: Cluster does not provide `ray.cluster` and the target node is not the
  head node.
