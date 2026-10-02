---
myst:
  html_meta:
    description: "Communicate between actors outside method calls by wrapping library processes, using Ray Collective, or running an HTTP server in an actor."
---

# Out-of-band communication

Ray actors typically communicate through actor method calls and share data through the distributed object store. In some use cases, out-of-band communication can be useful instead.

## Wrapping library processes
Many libraries have mature, high-performance internal communication stacks and use Ray as a language-integrated actor scheduler. These libraries handle most communication between actors out-of-band, through their existing communication stacks. For example, Horovod-on-Ray uses collective communication based on NCCL or the Message Passing Interface (MPI), and RayDP uses Spark's internal RPC and object manager. See [Ray Distributed Library Patterns](https://www.anyscale.com/blog/ray-distributed-library-patterns) for more details.

## Ray Collective
The Ray collective communication library, `ray.util.collective`, provides efficient out-of-band collective and point-to-point communication between distributed CPUs or GPUs. See {ref}`Ray Collective <ray-collective>` for more details.

## HTTP server
You can start an HTTP server inside an actor and expose HTTP endpoints, so clients outside the Ray cluster can communicate with the actor.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/actor-http-server.py
```
:::
::::

You can expose other types of servers the same way, such as gRPC servers.

## Limitations

Ray doesn't manage calls between actors that use out-of-band communication. As a result, features such as distributed reference counting don't work with out-of-band communication, so don't pass object refs this way.
