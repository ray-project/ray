---
myst:
  html_meta:
    description: "Terminate Ray actors from a handle or from inside the actor, and how actor cleanup runs when ray.shutdown is called."
---

# Terminating actors

Ray terminates an actor process automatically when all copies of the actor handle go out of scope in Python, or when the original creator process dies. When an actor terminates gracefully, Ray calls the actor's `__ray_shutdown__()` method, if you defined one, so the actor can clean up its resources. See {ref}`actor-cleanup`.

Java and C++ don't support automatic actor termination yet.

(ray-kill-actors)=
(manual-termination-via-an-actor-handle)=
## Manual termination through an actor handle

In most cases, Ray terminates out-of-scope actors automatically, but you might sometimes need to kill an actor forcefully. Reserve forced termination for an actor that's unexpectedly hanging or leaking resources, and for {ref}`detached actors <actor-lifetimes>`, which you must destroy manually.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray

@ray.remote
class Actor:
    pass

actor_handle = Actor.remote()

ray.kill(actor_handle)
# Force kill: the actor exits immediately without cleanup.
# This will NOT call __ray_shutdown__() or atexit handlers.
```
:::


:::{tab-item} Java
```java
actorHandle.kill();
// This will not go through the normal Java System.exit teardown logic, so any
// shutdown hooks installed in the actor using ``Runtime.addShutdownHook(...)`` will
// not be called.
```
:::

:::{tab-item} C++
```c++
actor_handle.Kill();
// This will not go through the normal C++ std::exit
// teardown logic, so any exit handlers installed in
// the actor using ``std::atexit`` will not be called.
```
:::
::::


A forced kill makes the actor exit its process immediately, and any current, pending, and future tasks fail with a `RayActorError`. To have Ray {ref}`automatically restart <fault-tolerance-actors>` the actor, set a nonzero `max_restarts` in the actor's `@ray.remote` options, then pass `no_restart=False` to `ray.kill`.

For {ref}`named and detached actors <actor-lifetimes>`, calling `ray.kill` on an actor handle destroys the actor and frees its name for reuse.

To see the death cause of a dead actor, run `ray list actors --detail` from the {ref}`State API <state-api-overview-ref>`:

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list actors --detail
```

```bash
---
-   actor_id: e8702085880657b355bf7ef001000000
    class_name: Actor
    state: DEAD
    job_id: '01000000'
    name: ''
    node_id: null
    pid: 0
    ray_namespace: dbab546b-7ce5-4cbb-96f1-d0f64588ae60
    serialized_runtime_env: '{}'
    required_resources: {}
    death_cause:
        actor_died_error_context: # <---- You could see the error message w.r.t why the actor exits.
            error_message: The actor is dead because `ray.kill` killed it.
            owner_id: 01000000ffffffffffffffffffffffffffffffffffffffffffffffff
            owner_ip_address: 127.0.0.1
            ray_namespace: dbab546b-7ce5-4cbb-96f1-d0f64588ae60
            class_name: Actor
            actor_id: e8702085880657b355bf7ef001000000
            never_started: true
            node_ip_address: ''
            pid: 0
            name: ''
    is_detached: false
    placement_group_id: null
    repr_name: ''
```


## Manual termination within the actor

You can manually stop an actor from within one of its methods. Doing so kills the actor process and releases the resources associated with or assigned to the actor.

::::{tab-set}
:::{tab-item} Python
```{testcode}
@ray.remote
class Actor:
    def exit(self):
        ray.actor.exit_actor()

actor = Actor.remote()
actor.exit.remote()
```

You usually don't need this approach, because Ray garbage-collects actors automatically. To wait for the actor to exit, wait on the object ref that the task returns. Calling `ray.get()` on it raises a `RayActorError`.
:::

:::{tab-item} Java
```java
Ray.exitActor();
```

Ray doesn't garbage-collect actors in Java yet, so this is currently the only way to stop an actor gracefully. To wait for the actor to exit, wait on the object ref that the task returns. Calling `ObjectRef::get` on it throws a `RayActorException`.
:::

:::{tab-item} C++
```c++
ray::ExitActor();
```

Ray doesn't garbage-collect actors in C++ yet, so this is currently the only way to stop an actor gracefully. To wait for the actor to exit, wait on the object ref that the task returns. Calling `ObjectRef::Get` on it throws a `RayActorException`.
:::
::::

This method of termination waits for any previously submitted tasks to finish executing, then exits the process gracefully with `sys.exit`.



Run `ray list actors --detail` to confirm that the actor died because of your `exit_actor()` call:

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray list actors --detail
```

```bash
---
-   actor_id: 070eb5f0c9194b851bb1cf1602000000
    class_name: Actor
    state: DEAD
    job_id: '02000000'
    name: ''
    node_id: 47ccba54e3ea71bac244c015d680e202f187fbbd2f60066174a11ced
    pid: 47978
    ray_namespace: 18898403-dda0-485a-9c11-e9f94dffcbed
    serialized_runtime_env: '{}'
    required_resources: {}
    death_cause:
        actor_died_error_context:
            error_message: 'The actor is dead because its worker process has died.
                Worker exit type: INTENDED_USER_EXIT Worker exit detail: Worker exits
                by a user request. exit_actor() is called.'
            owner_id: 02000000ffffffffffffffffffffffffffffffffffffffffffffffff
            owner_ip_address: 127.0.0.1
            node_ip_address: 127.0.0.1
            pid: 47978
            ray_namespace: 18898403-dda0-485a-9c11-e9f94dffcbed
            class_name: Actor
            actor_id: 070eb5f0c9194b851bb1cf1602000000
            name: ''
            never_started: false
    is_detached: false
    placement_group_id: null
    repr_name: ''
```


(actor-cleanup)=

## Actor cleanup with `__ray_shutdown__`

When an actor terminates gracefully, Ray calls its `__ray_shutdown__()` method, if one exists. Use this method to clean up resources such as database connections or file handles.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray
import tempfile
import os

@ray.remote
class FileProcessorActor:
    def __init__(self):
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_file.write(b"processing data")
        self.temp_file.flush()

    def __ray_shutdown__(self):
        # Clean up temporary file
        if hasattr(self, 'temp_file'):
            self.temp_file.close()
            os.unlink(self.temp_file.name)

    def process(self):
        return "done"

actor = FileProcessorActor.remote()
ray.get(actor.process.remote())
del actor  # __ray_shutdown__() is called automatically
```
:::
::::

Ray calls `__ray_shutdown__()` in the following cases:

- **Automatic termination**: When all actor handles go out of scope, either through `del actor` or a natural scope exit.
- **Manual graceful termination**: When you call `actor.__ray_terminate__.remote()`.

Ray doesn't call `__ray_shutdown__()` in the following cases:

- **Force kill**: When you use `ray.kill(actor)`. Ray kills the actor immediately without cleanup.
- **Unexpected termination**: When the actor process crashes or exits unexpectedly, such as from a segfault or when the OOM killer kills it.

Keep the following behavior in mind when you implement `__ray_shutdown__()`:

- `__ray_shutdown__()` runs after all actor tasks complete.
- By default, Ray waits 30 seconds for the graceful shutdown procedure, including `__ray_shutdown__()`, to complete. If the actor doesn't exit within this timeout, Ray force kills it. To change the timeout, set `ray.init(_system_config={"actor_graceful_shutdown_timeout_ms": 60000})`.
- Ray catches and logs exceptions in `__ray_shutdown__()`, and they don't prevent actor termination.
- `__ray_shutdown__()` must be a synchronous method, even in async actors.
