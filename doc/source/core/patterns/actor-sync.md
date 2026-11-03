---
myst:
  html_meta:
    description: "Pattern: use a single actor as a synchronization point to coordinate other tasks and actors."
---

# Pattern: Using an actor to synchronize other tasks and actors

When multiple tasks need to wait on a condition or otherwise synchronize across tasks and actors on a cluster, use a central actor to coordinate them.

## Example use case

You can use an actor to implement a distributed `asyncio.Event` that multiple tasks can wait on.

## Code example

```{literalinclude} ../doc_code/actor-sync.py
```
