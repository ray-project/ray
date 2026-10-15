---
myst:
  html_meta:
    description: "Common Ray Compiled Graph problems: current limitations, returning NumPy arrays, and tearing down before reusing the same actors."
---

# Troubleshooting

This page contains common issues and solutions for Compiled Graph execution.

## Limitations

Compiled Graph has the following limitations:

- Invoking Compiled Graph

  - Only the process that compiles the Compiled Graph can call it.

  - A Compiled Graph has a maximum number of in-flight executions. When you use the DAG API, if there aren't enough resources at the time of `dag.execute()`, Ray queues the tasks for later execution. Ray Compiled Graph currently doesn't support queuing past its maximum capacity, so you might need to consume some results with `ray.get()` before you submit more executions. As a stopgap, `dag.execute()` throws a `RayCgraphCapacityExceeded` exception if the call takes too long. Compiled Graph might add better error handling and queuing in a later release.

- Compiled Graph execution

  - Avoid executing other tasks on an actor while it participates in a Compiled Graph. Compiled Graph tasks execute on a background thread. Concurrent tasks that you submit to the actor can still execute on the main thread, but you're responsible for synchronizing them with the Compiled Graph background thread.

  - For now, an actor can execute only one Compiled Graph at a time. To execute a different Compiled Graph on the same actor, you must tear down the current Compiled Graph. For details, see {ref}`Explicitly tear down before reusing the same actors <troubleshoot-teardown>`.

- Passing and getting Compiled Graph results, which are {class}`CompiledDAGRef <ray.experimental.compiled_dag_ref.CompiledDAGRef>` objects

  - You can't pass Compiled Graph results to another task or actor. A later release might loosen this restriction. For now, the restriction gives better performance because the backend knows exactly where to push the results.

  - You can call `ray.get()` at most once on a {class}`CompiledDAGRef <ray.experimental.compiled_dag_ref.CompiledDAGRef>`. Calling it twice on the same {class}`CompiledDAGRef <ray.experimental.compiled_dag_ref.CompiledDAGRef>` raises an exception. The restriction exists because the underlying memory for the result might need to be reused for a later DAG execution. Restricting `ray.get()` to once per reference simplifies tracking of the memory buffers.

  - If `ray.get()` returns a zero-copy deserialized value, subsequent executions of the same DAG block until the value goes out of scope in Python. So if you hold onto zero-copy deserialized values that `ray.get()` returns and you try to execute the Compiled Graph beyond its maximum concurrency, it might deadlock. Detection of this case is planned for a later release. For now, you receive a `RayChannelTimeoutError`. For details, see {ref}`Return NumPy arrays <troubleshoot-numpy>`.

- Collective operations

  - For GPU-to-GPU communication, Compiled Graph supports only peer-to-peer transfers. Support for collective communication operations is planned.

Watch for the following features in later Ray releases:

- Better queuing of DAG inputs, for more concurrent executions of the same DAG
- More collective operations with NCCL
- Multiple DAGs executing on the same actor
- General performance improvements

To report other issues or share feedback or questions, file an issue on [GitHub](https://github.com/ray-project/ray/issues). For a full list of known issues, see the `compiled-graphs` label on the Ray GitHub repository.

(troubleshoot-numpy)=

## Returning NumPy arrays
Ray zero-copy deserializes NumPy arrays when possible. If you execute a Compiled Graph with a NumPy array output multiple times, you might run into issues if the NumPy array output from a previous execution isn't deleted before you try to get the result of a later execution of the same Compiled Graph. The NumPy array stays in the buffer of the Compiled Graph until you or Python delete it. Python might not always garbage collect the NumPy array as soon as you expect, so delete it explicitly.

For example, the following code might hang or raise a `RayChannelTimeoutError` if the NumPy array isn't deleted:

```{literalinclude} ../doc_code/cgraph_troubleshooting.py
:language: python
:start-after: __numpy_troubleshooting_start__
:end-before: __numpy_troubleshooting_end__
```

In the preceding code, Python might not garbage collect the NumPy array in `result` on each iteration of the loop. Explicitly delete the NumPy array before you try to get the result of later Compiled Graph executions.

(troubleshoot-teardown)=
(explicitly-teardown-before-reusing-the-same-actors)=

## Explicitly tear down before reusing the same actors
To reuse the actors of a Compiled Graph, explicitly tear down the Compiled Graph first. Otherwise, the resources created for the actors in a Compiled Graph might conflict with later use of those actors.

For example, in the following code, Python might delay garbage collection, which triggers the implicit teardown of the first Compiled Graph. The delay might cause a segfault because of the resource conflicts described earlier:

```{literalinclude} ../doc_code/cgraph_troubleshooting.py
:language: python
:start-after: __teardown_troubleshooting_start__
:end-before: __teardown_troubleshooting_end__
```
