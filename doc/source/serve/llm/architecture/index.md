# Architecture

How Ray Serve LLM is built: the components a deployment is made of, how a request flows through them, and the patterns that scale serving across GPUs and nodes. Read these to extend the system or to reason about performance. To deploy models, see the {doc}`User guides <../user-guides/index>` instead.

Start with the overview, then read the pages relevant to your use case:

- {doc}`Architecture overview <overview>`: the components of a deployment (engine, server, ingress) and how a request flows through them. Read this first.
- {doc}`Core components <core>`: the key abstractions and extension points, including the engine protocol, `LLMConfig`, the builder functions, and custom server classes.
- {doc}`Serving patterns <serving-patterns/index>`: distributed patterns (data parallel attention, prefill-decode disaggregation) and how they compose.
- {doc}`Request routing <routing-policies>`: how a replica is selected for each request, the built-in policies, and how to write a custom router.

```{toctree}
:hidden:
:maxdepth: 1

Architecture overview <overview>
Core components <core>
Serving patterns <serving-patterns/index>
Request routing <routing-policies>
```
