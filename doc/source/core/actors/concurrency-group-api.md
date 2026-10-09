---
myst:
  html_meta:
    description: "Limit concurrency per actor method with concurrency groups, including the default group and choosing a group at call time."
---

# Limiting concurrency per method with concurrency groups

Besides setting an actor's overall maximum concurrency, you can separate its methods into *concurrency groups*, each with one or more threads of its own. Use concurrency groups to limit concurrency per method. For example, give a health-check method its own concurrency quota, separate from the methods that serve requests.

:::{tip}
Concurrency groups work with both `asyncio` and threaded actors. The syntax is the same.
:::

(defining-concurrency-groups)=

## Defining concurrency groups

The following example defines two concurrency groups, `io` with a maximum concurrency of 2 and `compute` with a maximum concurrency of 4. It places the methods `f1` and `f2` in the `io` group and the methods `f3` and `f4` in the `compute` group. Actors always have a default concurrency group, which has a default concurrency limit of 1000 for `asyncio` actors and 1 otherwise.

::::{tab-set}
:::{tab-item} Python
Define concurrency groups for an actor with the `concurrency_groups` argument of `@ray.remote`:

```{testcode}
import ray

@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class AsyncIOActor:
    def __init__(self):
        pass

    @ray.method(concurrency_group="io")
    async def f1(self):
        pass

    @ray.method(concurrency_group="io")
    async def f2(self):
        pass

    @ray.method(concurrency_group="compute")
    async def f3(self):
        pass

    @ray.method(concurrency_group="compute")
    async def f4(self):
        pass

    async def f5(self):
        pass

a = AsyncIOActor.remote()
a.f1.remote()  # executed in the "io" group.
a.f2.remote()  # executed in the "io" group.
a.f3.remote()  # executed in the "compute" group.
a.f4.remote()  # executed in the "compute" group.
a.f5.remote()  # executed in the default group.
```
:::

:::{tab-item} Java
Define concurrency groups for a concurrent actor with the `setConcurrencyGroups()` method:

```java
class ConcurrentActor {
    public long f1() {
        return Thread.currentThread().getId();
    }

    public long f2() {
        return Thread.currentThread().getId();
    }

    public long f3(int a, int b) {
        return Thread.currentThread().getId();
    }

    public long f4() {
        return Thread.currentThread().getId();
    }

    public long f5() {
        return Thread.currentThread().getId();
    }
}

ConcurrencyGroup group1 =
    new ConcurrencyGroupBuilder<ConcurrentActor>()
        .setName("io")
        .setMaxConcurrency(2)
        .addMethod(ConcurrentActor::f1)
        .addMethod(ConcurrentActor::f2)
        .build();
ConcurrencyGroup group2 =
    new ConcurrencyGroupBuilder<ConcurrentActor>()
        .setName("compute")
        .setMaxConcurrency(4)
        .addMethod(ConcurrentActor::f3)
        .addMethod(ConcurrentActor::f4)
        .build();

ActorHandle<ConcurrentActor> myActor = Ray.actor(ConcurrentActor::new)
    .setConcurrencyGroups(group1, group2)
    .remote();

myActor.task(ConcurrentActor::f1).remote();  // executed in the "io" group.
myActor.task(ConcurrentActor::f2).remote();  // executed in the "io" group.
myActor.task(ConcurrentActor::f3, 3, 5).remote();  // executed in the "compute" group.
myActor.task(ConcurrentActor::f4).remote();  // executed in the "compute" group.
myActor.task(ConcurrentActor::f5).remote();  // executed in the "default" group.
```
:::
::::


(default-concurrency-group)=

## Default concurrency group

By default, Ray places methods in the default concurrency group, which has a concurrency limit of 1000 for `asyncio` actors and 1 otherwise. To change the default group's concurrency, set the `max_concurrency` actor option.

::::{tab-set}
:::{tab-item} Python
The following actor has two concurrency groups, `io` and `default`. The maximum concurrency of `io` is 2, and the maximum concurrency of `default` is 10.

```{testcode}
@ray.remote(concurrency_groups={"io": 2})
class AsyncIOActor:
    async def f1(self):
        pass

actor = AsyncIOActor.options(max_concurrency=10).remote()
```
:::

:::{tab-item} Java
The following concurrent actor has two concurrency groups, `io` and `default`. The maximum concurrency of `io` is 2, and the maximum concurrency of `default` is 10.

```java
class ConcurrentActor {
    public long f1() {
        return Thread.currentThread().getId();
    }
}

ConcurrencyGroup group =
    new ConcurrencyGroupBuilder<ConcurrentActor>()
        .setName("io")
        .setMaxConcurrency(2)
        .addMethod(ConcurrentActor::f1)
        .build();

ActorHandle<ConcurrentActor> myActor = Ray.actor(ConcurrentActor::new)
      .setConcurrencyGroups(group)
      .setMaxConcurrency(10)
      .remote();
```
:::
::::


(setting-the-concurrency-group-at-runtime)=

## Setting the concurrency group at runtime

You can also dispatch an actor method to a specific concurrency group at runtime. The following example sets the concurrency group of the `f2` method at runtime.

::::{tab-set}
:::{tab-item} Python
Pass `concurrency_group` to the `.options` method:

```{testcode}
# Executed in the "io" group (as defined in the actor class).
a.f2.options().remote()

# Executed in the "compute" group.
a.f2.options(concurrency_group="compute").remote()
```
:::

:::{tab-item} Java
Call the `setConcurrencyGroup` method:

```java
// Executed in the "io" group (as defined in the actor creation).
myActor.task(ConcurrentActor::f2).remote();

// Executed in the "compute" group.
myActor.task(ConcurrentActor::f2).setConcurrencyGroup("compute").remote();
```
:::
::::
