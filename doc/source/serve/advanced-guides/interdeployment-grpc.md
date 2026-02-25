(serve-interdeployment-grpc)=
# Interdeployment gRPC Transport

By default, when one Ray Serve deployment calls another via a `DeploymentHandle`, requests are sent through Ray's actor RPC system ("by reference"). You can optionally switch this internal transport to gRPC, which serializes requests over the network instead.

This is separate from the [gRPC ingress proxy](serve-set-up-grpc-service), which handles external gRPC clients. Interdeployment gRPC controls how deployments talk to *each other* internally.

## When to use gRPC transport

gRPC transport bypasses Ray's actor scheduler and object store, reducing per-request overhead. It is most beneficial for **high-throughput workloads with small payloads** — requests and responses under ~1 MB typically show a benefit. Pairing it with a fast serializer like `msgpack` or `orjson` further reduces overhead when data is JSON-serializable.

**Keep the default (by-reference) transport when:**
- Payloads are large. By-reference mode uses Ray's actor RPC, which can pass data without serialization overhead for co-located replicas.
- Deployments are chained. By-reference mode passes `DeploymentResponse` objects through the pipeline without materializing intermediate results.
- You need `_to_object_ref()` for custom async patterns.

## How it works

When gRPC transport is enabled for a handle:
1. Each replica starts an internal gRPC server on a random port at startup.
2. Requests are serialized and sent over gRPC directly to the target replica's port, bypassing Ray's actor RPC.
3. Serialization format is configurable (cloudpickle, pickle, msgpack, or orjson).

## Enabling gRPC transport

### Per-handle

Use `handle.options(_by_reference=False)` to enable gRPC transport for a specific handle:

```{literalinclude} ../doc_code/interdeployment_grpc.py
:start-after: __begin_per_handle__
:end-before: __end_per_handle__
:language: python
```

### Globally

Set the `RAY_SERVE_USE_GRPC_BY_DEFAULT` environment variable to `1` on all nodes before starting Ray:

```bash
export RAY_SERVE_USE_GRPC_BY_DEFAULT=1
```

This makes all `DeploymentHandle` calls use gRPC transport by default. Individual handles can still override this with `handle.options(_by_reference=True)`.

:::{note}
When `RAY_SERVE_USE_GRPC_BY_DEFAULT=1` is set, proxy-to-replica communication also uses gRPC by default. You can control this independently with `RAY_SERVE_PROXY_USE_GRPC=0` or `RAY_SERVE_PROXY_USE_GRPC=1`.
:::

## Serialization options

The gRPC transport supports multiple serialization formats. Configure them per-handle:

```{literalinclude} ../doc_code/interdeployment_grpc.py
:start-after: __begin_serialization_options__
:end-before: __end_serialization_options__
:language: python
```

Available formats:
- `"cloudpickle"` (default) — most flexible, supports arbitrary Python objects.
- `"pickle"` — standard library pickle.
- `"msgpack"` — fast binary format, requires data to be msgpack-serializable.
- `"orjson"` — fast JSON format, requires data to be JSON-serializable.

:::{note}
Using `msgpack` or `orjson` can improve serialization performance but restricts the types of objects you can pass. Use them when your request/response data consists of simple types (dicts, lists, strings, numbers).
:::

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_USE_GRPC_BY_DEFAULT` | `0` | Set to `1` to use gRPC transport for all interdeployment calls. |
| `RAY_SERVE_PROXY_USE_GRPC` | Inherits from above | Set to `1` or `0` to independently control proxy-to-replica transport. |
| `RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH` | `4194304` (4 MB) | Maximum gRPC message size for interdeployment calls. Increase if you send large payloads between deployments. |

## Limitations

- **No `_to_object_ref` support.** Handles using gRPC transport (`_by_reference=False`) cannot call `._to_object_ref()` or `._to_object_ref_sync()`. These methods require Ray actor RPC.
- **Java deployments not supported.** gRPC transport is only available for Python deployments.
- **Data must be serializable.** While `cloudpickle` can serialize most Python objects, other formats like `msgpack` or `orjson` are more restrictive. Ensure your data is compatible with the chosen serializer.
