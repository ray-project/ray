---
myst:
  html_meta:
    description: "Ray Core internals for advanced users: task lifecycle, streaming generators, autoscaler v2, RPC fault tolerance, object spilling, and metrics."
---

(ray-core-internals)=

# Internals

This section provides a look into some of Ray Core internals. It's primarily intended for advanced users and developers of Ray Core. For the high level architecture overview, please refer to the [whitepaper](https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview).

```{toctree}
:maxdepth: 1

task-lifecycle
streaming-generator
autoscaler-v2
rpc-fault-tolerance
token-authentication
metric-exporter
ray-event-exporter
port-service-discovery
object-spilling
```
