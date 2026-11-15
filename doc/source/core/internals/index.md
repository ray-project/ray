---
myst:
  html_meta:
    description: "Ray Core internals for advanced users: task lifecycle, streaming generators, autoscaler v2, RPC fault tolerance, object spilling, and metrics."
---

(ray-core-internals)=

# Internals

This section describes some of the internals of Ray Core. It's primarily for advanced users and for developers who work on Ray Core. For a high-level overview of the architecture, see the [Ray 2.0 Architecture white paper](https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview).

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
