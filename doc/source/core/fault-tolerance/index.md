---
myst:
  html_meta:
    description: "How Ray classifies application-level and system-level failures, and where to find the per-component fault-tolerance guarantees."
---

(fault-tolerance)=

# Fault tolerance

Ray is a distributed system, so failures can happen. Generally, Ray classifies failures into two classes:

- Application-level failures, which bugs in user-level code or external system failures trigger.
- System-level failures, which node failures, network failures, or bugs in Ray trigger.

The following sections describe the mechanisms Ray provides for applications to recover from failures.

To handle application-level failures, Ray provides mechanisms to catch errors, retry failed code, and handle misbehaving code. See the pages for {ref}`task <fault-tolerance-tasks>` and {ref}`actor <fault-tolerance-actors>` fault tolerance for more information on these mechanisms.

Ray also provides several mechanisms to automatically recover from internal system-level failures, such as {ref}`node failures <fault-tolerance-nodes>`. In particular, Ray can automatically recover from some failures in the {ref}`distributed object store <fault-tolerance-objects>`.

## How to write fault-tolerant Ray applications

Follow these recommendations to make your Ray applications fault tolerant.

First, if Ray's fault-tolerance mechanisms don't work for you, you can always catch the {ref}`exceptions <ray-core-exceptions>` that failures cause and recover manually.

```{literalinclude} ../doc_code/fault_tolerance_tips.py
:language: python
:start-after: __manual_retry_start__
:end-before: __manual_retry_end__
```

Second, avoid letting an object ref outlive its {ref}`owner <fault-tolerance-objects>` task or actor. The owner is the task or actor that creates the initial object ref by calling {meth}`ray.put() <ray.put>` or `foo.remote()`. As long as references to an object still exist, the object's owner worker keeps running, even after the corresponding task or actor finishes. If the owner worker fails, Ray {ref}`can't recover <fault-tolerance-ownership>` the object automatically for any caller that tries to access it. For example, returning an object ref that `ray.put()` created from a task creates an object that outlives its owner:

```{literalinclude} ../doc_code/fault_tolerance_tips.py
:language: python
:start-after: __return_ray_put_start__
:end-before: __return_ray_put_end__
```

In the preceding example, object `x` outlives its owner task `a`. If the worker process running task `a` fails, calling `ray.get` on `x_ref` afterward results in an `OwnerDiedError` exception.

The following fault-tolerant version returns `x` directly. In this example, the driver owns `x` and you only access it within the lifetime of the driver. If `x` is lost, Ray can automatically recover it through {ref}`lineage reconstruction <fault-tolerance-objects-reconstruction>`. See {doc}`/core/patterns/return-ray-put` for details.

```{literalinclude} ../doc_code/fault_tolerance_tips.py
:language: python
:start-after: __return_directly_start__
:end-before: __return_directly_end__
```

Third, avoid {ref}`custom resource requirements <custom-resources>` that only particular nodes can satisfy. If that node fails, Ray can't retry the running tasks or actors on other nodes.

```{literalinclude} ../doc_code/fault_tolerance_tips.py
:language: python
:start-after: __node_ip_resource_start__
:end-before: __node_ip_resource_end__
```

If you want a task to run on a particular node, use the {class}`NodeAffinitySchedulingStrategy <ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy>`. With this strategy, you can specify the affinity as a soft constraint, so even if the target node fails, Ray can still retry the task on other nodes.

```{literalinclude} ../doc_code/fault_tolerance_tips.py
:language: python
:start-after: __node_affinity_scheduling_strategy_start__
:end-before: __node_affinity_scheduling_strategy_end__
```

## More about Ray fault tolerance

```{toctree}
:maxdepth: 1

tasks
actors
objects
nodes
gcs
```
