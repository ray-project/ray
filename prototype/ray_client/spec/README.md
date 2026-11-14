# Ray Client Specification

This directory is the ground truth for how the Ray Cluster REST API and `ray_client` Python SDK
are **intended** to behave: their user-facing contract, wire protocol, and implementation
invariants. It is written for users, contributors, and agents working on this codebase.

When this spec disagrees with the code or other documentation, the spec describes the intent.
The code is either behind or wrong.

## Layout

| Path | What it holds |
|---|---|
| [`api/python-sdk.md`](api/python-sdk.md) | The `ray_client` Python SDK (`RayCluster`, `ActorHandle`, `JobResult`, `run`, node management, AST/JSON validation, and `working_dir` packaging) |
| [`api/cluster-rest-api.md`](api/cluster-rest-api.md) | The HTTP/JSON and SSE endpoints on port `8265` (`/api/actors`, `/api/cluster/*`, `/api/jobs/*`, `/api/packages/*`) |
| [`implementation/architecture.md`](implementation/architecture.md) | Server-side `_RemoteClientRootActor` ownership, fate-sharing, child namespace isolation, cancellation, teardown hooks, and activity reporting |
