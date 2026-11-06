# Read Redis data through an external ADBC datasource

The [Redis ADBC driver](https://github.com/redis/redis-adbc-driver) provides an
external `adbc_driver_redis.ray.read_query` helper, initially pinned to Ray 2.59.0.
It plans supported SQL through ADBC partitions and reads Arrow batches on workers.
This is a Redis-project adapter, not a built-in Ray datasource or endorsement.

## Install matching builds

The helper and `ray` extra require a build containing the adapter. Older published
wheels may not include them. Follow the [Redis source installation guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#install-and-discover-the-driver),
build the native library, and install `./python[ray]` in the caller and every worker
environment. Set `REDIS_ADBC_LIBRARY` to the matching worker-local native library
for a source installation. Redis must run separately with Search enabled.

## Plan and read an existing SQL table

```python
import os

from adbc_driver_redis.ray import read_query

dataset = read_query(
    uri=os.environ.get("REDIS_URI", "redis://localhost:6379"),
    query="SELECT id, amount FROM analytics.orders",
    partition_count=4,
    batch_rows=1024,
)
print(dataset.take(5))
```

Create, ingest or adopt `analytics.orders` through the Redis driver first; arbitrary
Redis keys do not automatically become tables. Use an endpoint reachable by all
workers, with `rediss://` and worker-local CA files when needed. A client-local
`localhost` URI or library path is insufficient for a remote cluster.

Planning closes its connections before read tasks execute. Workers open independent
connections, stream batches and close their owned readers/cursors/connections.
Reads are live and unordered: there is no shared snapshot, stable retry result,
CDC or exactly-once guarantee. Unsupported partition SQL fails without a whole-result
fallback. Cooperative cleanup cannot guarantee cleanup after abrupt worker death.

The dedicated [Redis integration guide](https://github.com/redis/redis-adbc-driver/blob/f78bcb7/docs/integrations/ray.md)
details partition/ACL restrictions, SQL reader/writer alternatives, TLS and retry
limits. The [recorded validation](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/ray-worker-164/validation.md)
reports 17 local worker-process checks, including verified TLS. Multi-node execution,
Linux execution, release wheels and performance were not validated by that run.
