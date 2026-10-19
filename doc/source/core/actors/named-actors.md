---
myst:
  html_meta:
    description: "Give an actor a unique name in its namespace so any job in the cluster can retrieve it, with get-or-create and actor lifetime options."
---

# Named actors

Give an actor a unique name within its {ref}`namespace <namespaces-guide>` to retrieve the actor from any job in the Ray cluster. A name is useful when you can't pass the actor handle directly to the task that needs it, or when you want to access an actor that another driver launched. Ray still garbage-collects a named actor when no handles to it exist. See {ref}`actor-lifetimes`.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray

@ray.remote
class Counter:
    pass

# Create an actor with a name
counter = Counter.options(name="some_name").remote()

# Retrieve the actor later somewhere
counter = ray.get_actor("some_name")
```
:::

:::{tab-item} Java
```java
// Create an actor with a name.
ActorHandle<Counter> counter = Ray.actor(Counter::new).setName("some_name").remote();

...

// Retrieve the actor later somewhere
Optional<ActorHandle<Counter>> counter = Ray.getActor("some_name");
Assert.assertTrue(counter.isPresent());
```
:::

:::{tab-item} C++
```c++
// Create an actor with a globally unique name
ActorHandle<Counter> counter = ray::Actor(CreateCounter).SetGlobalName("some_name").Remote();

...

// Retrieve the actor later somewhere
boost::optional<ray::ActorHandle<Counter>> counter = ray::GetGlobalActor("some_name");
```

C++ also supports non-global named actors. A non-global actor's name is valid only within its job, and other jobs can't access the actor.

```c++
// Create an actor with a job-scope-unique name
ActorHandle<Counter> counter = ray::Actor(CreateCounter).SetName("some_name").Remote();

...

// Retrieve the actor later somewhere in the same job
boost::optional<ray::ActorHandle<Counter>> counter = ray::GetActor("some_name");
```
:::
::::

:::{note}
Ray scopes named actors by namespace. If you don't assign a namespace, Ray places named actors in an anonymous namespace.
:::

::::{tab-set}
:::{tab-item} Python
```{testcode}
:skipif: True

import ray

@ray.remote
class Actor:
    pass

# driver_1.py
# Job 1 creates an actor, "orange" in the "colors" namespace.
ray.init(address="auto", namespace="colors")
Actor.options(name="orange", lifetime="detached").remote()

# driver_2.py
# Job 2 is now connecting to a different namespace.
ray.init(address="auto", namespace="fruit")
# This fails because "orange" was defined in the "colors" namespace.
ray.get_actor("orange")
# You can also specify the namespace explicitly.
ray.get_actor("orange", namespace="colors")

# driver_3.py
# Job 3 connects to the original "colors" namespace
ray.init(address="auto", namespace="colors")
# This returns the "orange" actor we created in the first job.
ray.get_actor("orange")
```
:::

:::{tab-item} Java
```java
import ray

class Actor {
}

// Driver1.java
// Job 1 creates an actor, "orange" in the "colors" namespace.
System.setProperty("ray.job.namespace", "colors");
Ray.init();
Ray.actor(Actor::new).setName("orange").remote();

// Driver2.java
// Job 2 is now connecting to a different namespace.
System.setProperty("ray.job.namespace", "fruits");
Ray.init();
// This fails because "orange" was defined in the "colors" namespace.
Optional<ActorHandle<Actor>> actor = Ray.getActor("orange");
Assert.assertFalse(actor.isPresent());  // actor.isPresent() is false.

// Driver3.java
System.setProperty("ray.job.namespace", "colors");
Ray.init();
// This returns the "orange" actor we created in the first job.
Optional<ActorHandle<Actor>> actor = Ray.getActor("orange");
Assert.assertTrue(actor.isPresent());  // actor.isPresent() is true.
```
:::
::::

## Get or create a named actor

To create an actor only if it doesn't exist, use the `get_if_exists` actor creation option. The option is available after you set a name for the actor through `.options()`.

If the actor already exists, Ray returns a handle to it and ignores the arguments. Otherwise, Ray creates a new actor with the specified arguments.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/get_or_create.py
```
:::

:::{tab-item} Java
```java
// This feature is not yet available in Java.
```
:::

:::{tab-item} C++
```c++
// This feature is not yet available in C++.
```
:::
::::


(actor-lifetimes)=

## Actor lifetimes

You can also decouple an actor's lifetime from the job, so the actor persists after the job's driver process exits. An actor with a decoupled lifetime is *detached*.

::::{tab-set}
:::{tab-item} Python
```{testcode}
counter = Counter.options(name="CounterActor", lifetime="detached").remote()
```

Ray keeps `CounterActor` alive after the driver that runs the preceding script exits, so you can run the following script in a different driver:

```{testcode}
counter = ray.get_actor("CounterActor")
```

An actor can be named but not detached. If you specify only the name, without `lifetime="detached"`, you can retrieve `CounterActor` only while the original driver is running.
:::

:::{tab-item} Java
```java
System.setProperty("ray.job.namespace", "lifetime");
Ray.init();
ActorHandle<Counter> counter = Ray.actor(Counter::new).setName("some_name").setLifetime(ActorLifetime.DETACHED).remote();
```

Ray keeps the actor alive after the driver that runs the preceding code exits, so you can run the following code in a different driver:

```java
System.setProperty("ray.job.namespace", "lifetime");
Ray.init();
Optional<ActorHandle<Counter>> counter = Ray.getActor("some_name");
Assert.assertTrue(counter.isPresent());
```
:::

:::{tab-item} C++
C++ doesn't support customizing an actor's lifetime yet.
:::
::::


Unlike normal actors, Ray doesn't automatically garbage-collect detached actors. When you're sure you no longer need a detached actor, use `ray.kill` to {ref}`manually terminate <ray-kill-actors>` it. After this call, you can reuse the actor's name.
