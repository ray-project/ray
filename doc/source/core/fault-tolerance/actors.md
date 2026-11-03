---
myst:
  html_meta:
    description: "Actor fault tolerance in Ray: process and creator failure, restarts and checkpointing, force-killing, and unavailable-actor errors."
---

(fault-tolerance-actors)=
(actor-fault-tolerance)=

# Actor fault tolerance

An actor can fail if its process dies or if its *owner* dies. The owner of an actor is the worker that originally created the actor by calling `ActorClass.remote()`. {ref}`Detached actors <actor-lifetimes>` don't have an owner process. They're cleaned up when the Ray cluster is destroyed.


## Actor process failure

Ray can automatically restart actors that crash unexpectedly. The `max_restarts` option controls this behavior by setting the maximum number of times Ray restarts an actor. The default value of `max_restarts` is 0, meaning Ray doesn't restart the actor. If you set it to -1, Ray restarts the actor infinitely many times. When Ray restarts an actor, it recreates the actor's state by rerunning its constructor. After the specified number of restarts, subsequent actor methods raise a `RayActorError`.

By default, actor tasks execute with at-most-once semantics, which corresponds to `max_task_retries=0` in the `@ray.remote` {func}`decorator <ray.remote>`. If you submit an actor task to an unreachable actor, Ray reports the error with `RayActorError`, a Python-level exception that's raised when you call `ray.get` on the future the task returns. Ray might raise this exception even though the task executed successfully. For example, this can happen if the actor dies immediately after executing the task.

Ray also offers at-least-once execution semantics for actor tasks, which you get by setting `max_task_retries=-1` or `max_task_retries > 0`. With at-least-once semantics, if you submit an actor task to an unreachable actor, Ray automatically retries the task. With this option, Ray raises a `RayActorError` to the application only if one of the following two conditions occurs:

- The actor's `max_restarts` limit has been exceeded, and Ray can't restart the actor anymore.
- The `max_task_retries` limit has been exceeded for this particular task.

If the actor is restarting when you submit a task, that counts as one retry. To retry without limit, set `max_task_retries = -1`.

Run the following code to experiment with this behavior.

```{literalinclude} ../doc_code/actor_restart.py
:language: python
:start-after: __actor_restart_begin__
:end-before: __actor_restart_end__
```

For at-least-once actors, Ray still guarantees execution ordering according to the initial submission order. For example, any tasks submitted after a failed actor task don't execute on the actor until Ray successfully retries the failed actor task. Ray doesn't attempt to re-execute any tasks that executed successfully before the failure, unless `max_task_retries` is nonzero and Ray needs the task for {ref}`object reconstruction <fault-tolerance-objects-reconstruction>`.

:::{note}
For {ref}`async or threaded actors <async-actors>`, {ref}`tasks might execute out of order <actor-task-order>`. When the actor restarts, Ray retries only *incomplete* tasks. Ray doesn't re-execute previously completed tasks.
:::


At-least-once execution is best suited for read-only actors or actors with ephemeral state that doesn't need to be rebuilt after a failure. For actors that have critical state, your application is responsible for recovering the state, for example, by taking periodic checkpoints and recovering from the checkpoint when the actor restarts.


### Actor checkpointing

`max_restarts` automatically restarts the crashed actor, but it doesn't automatically restore application-level state in your actor. Instead, manually checkpoint your actor's state and recover it when the actor restarts.

For actors that you restart manually, the actor's creator should manage the checkpoint and manually restart and recover the actor on failure. Use this approach if you want the creator to decide when to restart the actor, or if the creator coordinates actor checkpoints with other execution:

```{literalinclude} ../doc_code/actor_checkpointing.py
:language: python
:start-after: __actor_checkpointing_manual_restart_begin__
:end-before: __actor_checkpointing_manual_restart_end__
```

Alternatively, if you use Ray's automatic actor restart, the actor can checkpoint itself manually and restore from a checkpoint in the constructor:

```{literalinclude} ../doc_code/actor_checkpointing.py
:language: python
:start-after: __actor_checkpointing_auto_restart_begin__
:end-before: __actor_checkpointing_auto_restart_end__
```

:::{note}
If you save the checkpoint to external storage, make sure it's accessible to the entire cluster, because Ray can restart the actor on a different node. For example, save the checkpoint to cloud storage such as S3, or to a shared directory such as one you access through NFS.
:::


## Actor creator failure

For {ref}`non-detached actors <actor-lifetimes>`, the owner of an actor is the worker that created it by calling `ActorClass.remote()`. As with {ref}`objects <fault-tolerance-objects>`, if the owner of an actor dies, the actor also fate-shares with the owner. Ray doesn't automatically recover an actor whose owner is dead, even if the actor has a nonzero `max_restarts`.

Because {ref}`detached actors <actor-lifetimes>` don't have an owner, Ray still restarts them even if their original creator dies. Ray continues to automatically restart a detached actor until the actor exceeds its maximum number of restarts, the actor is destroyed, or the Ray cluster is destroyed.

Try out this behavior with the following code.

```{literalinclude} ../doc_code/actor_creator_failure.py
:language: python
:start-after: __actor_creator_failure_begin__
:end-before: __actor_creator_failure_end__
```

## Force-killing a misbehaving actor

Sometimes application-level code can cause an actor to hang or leak resources. In these cases, you can recover from the failure by {ref}`manually terminating <ray-kill-actors>` the actor. To do so, call `ray.kill` on any handle to the actor. It doesn't need to be the original handle.

If you set `max_restarts`, you can also have Ray automatically restart the actor by passing `no_restart=False` to `ray.kill`.

## Unavailable actors

When an actor can't accept method calls, a `ray.get` on the method's returned object ref might raise `ActorUnavailableError`. This exception indicates the actor isn't accessible at the moment but might recover after waiting and retrying. Typical cases include the following:

- The actor is restarting. For example, it's waiting for resources or running the class constructor during the restart.
- The actor is experiencing transient network issues, such as connection outages.
- The actor is dead, but the death hasn't yet been reported to the system.

Ray executes actor method calls at most once. When a `ray.get()` call raises the `ActorUnavailableError` exception, there's no guarantee whether the actor executed the task. If the method has side effects, they might or might not be observable. Ray does guarantee that it doesn't execute the method twice, unless you configure the actor or the method with retries, as described in the next section.

The actor might or might not recover in the next calls. Those subsequent calls might raise `ActorDiedError` if the actor is confirmed dead, raise `ActorUnavailableError` if it's still unreachable, or return values normally if the actor recovered.

As a best practice, if the caller gets an `ActorUnavailableError`, have it quarantine the actor and stop sending traffic to it. The caller can then periodically ping the actor until the actor raises `ActorDiedError` or returns OK.

If a task has a nonzero `max_task_retries` and receives `ActorUnavailableError`, Ray retries the task up to `max_task_retries` times, or without limit if `max_task_retries` is `-1`. If the actor is restarting in its constructor, the task retry fails and consumes one retry. If retries remain, Ray retries again after `RAY_task_retry_delay_ms`, until it consumes all retries or the actor is ready to accept tasks. If the constructor takes a long time to run, consider increasing `max_task_retries` or `RAY_task_retry_delay_ms`.

## Actor method exceptions

To retry an actor method when it raises an exception, use `max_task_retries` with `retry_exceptions`.

By default, Ray doesn't retry on exceptions that user code raises. Before you enable retries, make sure the method is *idempotent*, meaning that invoking it multiple times is equivalent to invoking it only once.

You can set `retry_exceptions` in the `@ray.method(retry_exceptions=...)` decorator, or with `.options(retry_exceptions=...)` on the method call.

Retry behavior depends on the value of `retry_exceptions`:

- `False`: Ray doesn't retry on user exceptions. This value is the default.
- `True`: Ray retries a method on user exception up to `max_task_retries` times.
- A list of exceptions: Ray retries a method on user exception up to `max_task_retries` times, only if the method raises an exception from these specific classes.

`max_task_retries` applies to both exceptions and actor crashes. Set this option on an actor to apply it to all of the actor's methods, or on a method to override the actor's value for that method. Ray searches for the first non-default value of `max_task_retries` in the following order:

1. The method call's value, for example, `actor.method.options(max_task_retries=2)`. Ray ignores this value if you don't set it.
1. The method definition's value, for example, `@ray.method(max_task_retries=2)`. Ray ignores this value if you don't set it.
1. The actor creation call's value, for example, `Actor.options(max_task_retries=2)`. Ray ignores this value if you don't set it.
1. The actor class definition's value, for example, the `@ray.remote(max_task_retries=2)` decorator. Ray ignores this value if you don't set it.
1. The default value, which is `0`.

For example, if a method sets `max_task_retries=5` and `retry_exceptions=True`, and the actor sets `max_restarts=2`, Ray executes the method up to six times: once for the initial invocation, and five additional retries. The six invocations might include two actor crashes. After the sixth invocation, a `ray.get` call on the result's object ref raises the exception from the last invocation, or `ray.exceptions.RayActorError` if the actor crashed in the last invocation.
