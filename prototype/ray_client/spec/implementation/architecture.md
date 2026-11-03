# Implementation Architecture

This document specifies how the Ray Dashboard server executes `ray_client` workloads and enforces
ownership, isolation, cancellation, and lifecycle invariants.

## Actor-backed execution (`_RemoteClientRootActor`)

Both `cluster.run()` and `cluster.actor()` execute user code inside a Ray actor
(`_RemoteClientRootActor`) managed by `ClientActorManager` on the Ray Dashboard agent:

- **One-shot driver functions (`cluster.run`, `kind="function"`):** Deployed with
  `detached=False` (owned by the Dashboard agent process) and `resources={}` (`num_cpus=0`,
  pinned to the head node by default unless `RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES=1`, explicit
  placement options `node_id` / `label_selector`, or nonzero resources are set). Keeping the
  Dashboard agent as a persistent owner allows warm child `@ray.remote` workers and cached
  `runtime_env` environments to be reused across consecutive `cluster.run()` calls without
  per-call `ray.init()` driver startup overhead.
- **Stateful actors (`cluster.actor`, `kind="class"`):** Deployed with `detached=True`,
  `resources={"CPU": 1}`, `max_concurrency=1`, and `ttl_seconds=1800.0` by default.

Inside `_RemoteClientRootActor`, user classes and functions see a natural Ray environment:

- `ray.get_runtime_context()` reports the user class's `repr_name` and task name
  (`<ClassName>.<method>`).
- `self` inside user methods is the user class instance (not the wrapper actor).
- All `stdout` and `stderr` produced by the root actor process are tee'd into an in-memory ring
  buffer (`512 KiB`) exposed via `GET /api/actors/{name}/logs` and attached to
  `RayJobExecutionError.logs` on failure.

## Ownership and fate-sharing

Every `_RemoteClientRootActor` is the Ray owner of all child actors, tasks, placement groups,
resource reservations, non-detached nodes
(`ray.cluster.get_node_group(...).add_nodes(..., detached=False)`), and objects it creates. When
the root actor is terminated (`DELETE /api/actors/{name}`, context manager exit, one-shot
completion, client disconnect, failed `__init__`, or idle TTL expiration):

1. `ClientActorManager` invokes `_ray_client_teardown` on the root actor (bounded by a 2-second
   timeout), which runs any user-defined `close()` or `__exit__(None, None, None)` method,
   removes non-detached placement groups created in that root actor, and terminates tracked
   non-detached child actors.
2. `ClientActorManager` calls `ray.kill(handle, no_restart=True)` (or `__ray_destroy__` on Ray
   3.0), causing Ray's ownership tree to reclaim all remaining child actors, reservations,
   non-detached nodes, and objects owned by the root actor.

## Child namespace isolation

Because all root actors created by the Dashboard agent share the agent's Ray job, `ray_client`
applies two-tier namespace isolation to `@ray.remote` child actors created inside a root actor:

- **Non-detached child actors** default to the per-root-actor namespace
  `ray_client_<root_actor_name>`, preventing naming collisions between concurrent `run()` or
  `actor()` sessions.
- **Detached child actors** (`lifetime="detached"` in Ray 2.x or `detached=True` in Ray 3.0)
  default to the shared `"ray_client"` namespace so separate sessions can hand off named
  detached actors intentionally.
- **`ray.get_actor(name)` inside the root actor process** checks `ray_client_<root_actor_name>`
  first and falls back to `"ray_client"`. Nested tasks or child actors that call
  `ray.get_actor(name)` must pass `namespace=` explicitly.

## Actor lookup state machine and concurrency control

`ClientActorManager` coordinates concurrent operations per actor name using a fine-grained
per-name lock (`_name_locks[name]`) and an in-flight creation placeholder (`_creating[name]`):

| Lookup state | Meaning | Behavior |
|---|---|---|
| `FOUND` | Live root actor responds to `_ray_client_get_info` | Returned by `GET /api/actors/{name}`, `get_if_exists=True`, and method calls |
| `CREATING` | `POST /api/actors` is currently instantiating `name` | Included in `GET /api/actors` (`status="CREATING"`); `DELETE /api/actors/{name}` cancels the in-flight creation immediately without waiting on `_name_locks[name]` |
| `UNRESPONSIVE` | Actor exists in GCS as `ALIVE` / `PENDING` but `_ray_client_get_info` timed out because the actor is busy | Preserved (never killed) on `get_if_exists=True`, `GET /api/actors`, and idle TTL sweeps; raises `TimeoutError` (`RayJobTimeoutError`) on `create_actor` |
| `BROKEN` | Actor is `DEAD` in GCS or its worker process died | Evicted from `_actors` and killed so the name can be recreated cleanly |
| `NON_ROOT` | A non-`ray_client` actor in `"ray_client"` namespace holds `name` | Raises `ValueError` conflict without killing the foreign actor |
| `MISSING` | No actor with `name` exists | `create_actor` proceeds to create a new root actor |

## Cancellation on client disconnect or timeout

- **During `POST /api/actors`:** If the client disconnects or `timeout` expires while waiting for
  actor scheduling or `__init__`, `JobHead` / `JobAgent` cancels the creation task, kills the
  partially created actor, removes any placement groups created during `__init__`, and waits for
  GCS deregistration so the same actor name can be reused immediately.
- **During `POST /api/actors/{name}/methods/{method}`:**
  - For ephemeral `run()` actors (`detached=False`), a disconnect or timeout immediately kills
    the root actor and all child actors/tasks it spawned.
  - For persistent `actor()` sessions (`detached=True`), a disconnect or timeout calls
    `ray.cancel(ref, force=False, recursive=True)` (and `force=True` for synchronous thread
    tasks on timeout) and cancels any active generator stream via `_ray_client_close_stream`,
    stopping queued calls, `async` coroutines, generators, and child Ray tasks while keeping the
    actor alive for subsequent calls.

## Autoscaler activity reporting

`ClientActorManager.is_any_actor_active()` reports `ACTIVE` to `/api/component_activities`
(`driver` component) whenever any client root actor is alive, creating, or within its idle TTL,
and updates `last_activity_at` on every `run()` and `actor()` operation so auto-terminating
clusters do not shut down mid-session.

## Testing

- **Offline unit and mock server tests (`tests/test_unit.py`):** Tests AST validation, module
  constant capture, round-trip JSON checks, deterministic `working_dir` zipping, SSE framing,
  disconnect handling, and all `RayCluster` methods against an in-process mock HTTP server
  without requiring `ray`.
- **Live cluster integration tests (`tests/test_integration_live_cluster.py`):** Exercises
  `cluster.run()`, `cluster.actor()`, `cluster.submit_job()`, `/api/cluster/*` node APIs,
  placement groups, child namespace isolation, disconnect/timeout cancellation, and all runnable
  scripts in `examples/` against a live Ray cluster.
