---
myst:
  html_meta:
    description: "Group jobs and named actors into namespaces so actor names need only be unique per namespace, including anonymous namespaces."
---

(namespaces-guide)=

# Use namespaces

A namespace is a logical grouping of jobs and named actors. When you name an actor, its name must be unique within the namespace.

Set your application's namespace when you first connect to the cluster.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/namespaces.py
:language: python
:start-after: __init_namespace_start__
:end-before: __init_namespace_end__
```
:::

:::{tab-item} Java
```java
System.setProperty("ray.job.namespace", "hello"); // set it before Ray.init()
Ray.init();
```
:::

:::{tab-item} C++
```c++
ray::RayConfig config;
config.ray_namespace = "hello";
ray::Init(config);
```
:::
::::

See {ref}`Driver options <java-driver-options>` for ways to configure a Java application.

You can access a named actor only within its namespace.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/namespaces.py
:language: python
:start-after: __actor_namespace_start__
:end-before: __actor_namespace_end__
```
:::

:::{tab-item} Java
```java
// `ray start --head` has been run to launch a local cluster.

// Job 1 creates two actors, "orange" and "purple" in the "colors" namespace.
System.setProperty("ray.address", "localhost:10001");
System.setProperty("ray.job.namespace", "colors");
try {
    Ray.init();
    Ray.actor(Actor::new).setName("orange").remote();
    Ray.actor(Actor::new).setName("purple").remote();
} finally {
    Ray.shutdown();
}

// Job 2 is now connecting to a different namespace.
System.setProperty("ray.address", "localhost:10001");
System.setProperty("ray.job.namespace", "fruits");
try {
    Ray.init();
    // This fails because "orange" was defined in the "colors" namespace.
    Ray.getActor("orange").isPresent(); // return false
    // This succeeds because the name "orange" is unused in this namespace.
    Ray.actor(Actor::new).setName("orange").remote();
    Ray.actor(Actor::new).setName("watermelon").remote();
} finally {
    Ray.shutdown();
}

// Job 3 connects to the original "colors" namespace.
System.setProperty("ray.address", "localhost:10001");
System.setProperty("ray.job.namespace", "colors");
try {
    Ray.init();
    // This fails because "watermelon" was in the fruits namespace.
    Ray.getActor("watermelon").isPresent(); // return false
    // This returns the "orange" actor we created in the first job, not the second.
    Ray.getActor("orange").isPresent(); // return true
} finally {
    Ray.shutdown();
}
```
:::

:::{tab-item} C++
```c++
// `ray start --head` has been run to launch a local cluster.

// Job 1 creates two actors, "orange" and "purple" in the "colors" namespace.
ray::RayConfig config;
config.ray_namespace = "colors";
ray::Init(config);
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("orange").Remote();
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("purple").Remote();
ray::Shutdown();

// Job 2 is now connecting to a different namespace.
ray::RayConfig config;
config.ray_namespace = "fruits";
ray::Init(config);
// This fails because "orange" was defined in the "colors" namespace.
ray::GetActor<Counter>("orange"); // return nullptr;
// This succeeds because the name "orange" is unused in this namespace.
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("orange").Remote();
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("watermelon").Remote();
ray::Shutdown();

// Job 3 connects to the original "colors" namespace.
ray::RayConfig config;
config.ray_namespace = "colors";
ray::Init(config);
// This fails because "watermelon" was in the fruits namespace.
ray::GetActor<Counter>("watermelon"); // return nullptr;
// This returns the "orange" actor we created in the first job, not the second.
ray::GetActor<Counter>("orange");
ray::Shutdown();
```
:::
::::

(specifying-namespace-for-named-actors)=

## Specify a namespace for a named actor

You can specify a namespace for a named actor when you create it. The actor belongs to that namespace, regardless of the current job's namespace.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/namespaces.py
:language: python
:start-after: __specify_actor_namespace_start__
:end-before: __specify_actor_namespace_end__
```
:::


:::{tab-item} Java
```java
// `ray start --head` has been run to launch a local cluster.

System.setProperty("ray.address", "localhost:10001");
try {
    Ray.init();
    // Create an actor with specified namespace.
    Ray.actor(Actor::new).setName("my_actor", "actor_namespace").remote();
    // It is accessible in its namespace.
    Ray.getActor("my_actor", "actor_namespace").isPresent(); // return true

} finally {
    Ray.shutdown();
}
```
:::

:::{tab-item} C++
```c++
// `ray start --head` has been run to launch a local cluster.
ray::RayConfig config;
ray::Init(config);
// Create an actor with specified namespace.
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("my_actor", "actor_namespace").Remote();
// It is accessible in its namespace.
ray::GetActor<Counter>("my_actor", "actor_namespace");
ray::Shutdown();
```
:::
::::


## Anonymous namespaces

When you don't specify a namespace, Ray places your job in an anonymous namespace. In an anonymous namespace, your job has its own namespace and can't access actors in other namespaces.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/namespaces.py
:language: python
:start-after: __anonymous_namespace_start__
:end-before: __anonymous_namespace_end__
```
:::

:::{tab-item} Java
```java
// `ray start --head` has been run to launch a local cluster.

// Job 1 connects to an anonymous namespace by default.
System.setProperty("ray.address", "localhost:10001");
try {
    Ray.init();
    Ray.actor(Actor::new).setName("my_actor").remote();
} finally {
    Ray.shutdown();
}

// Job 2 connects to a _different_ anonymous namespace by default
System.setProperty("ray.address", "localhost:10001");
try {
    Ray.init();
    // This succeeds because the second job is in its own namespace.
    Ray.actor(Actor::new).setName("my_actor").remote();
} finally {
    Ray.shutdown();
}
```
:::

:::{tab-item} C++
```c++
// `ray start --head` has been run to launch a local cluster.

// Job 1 connects to an anonymous namespace by default.
ray::RayConfig config;
ray::Init(config);
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("my_actor").Remote();
ray::Shutdown();

// Job 2 connects to a _different_ anonymous namespace by default
ray::RayConfig config;
ray::Init(config);
// This succeeds because the second job is in its own namespace.
ray::Actor(RAY_FUNC(Counter::FactoryCreate)).SetName("my_actor").Remote();
ray::Shutdown();
```
:::
::::

:::{note}
Ray implements each anonymous namespace as a UUID, so a future job can manually connect to an existing anonymous namespace. Avoid doing this.
:::


(getting-the-current-namespace)=

## Get the current namespace

Get the current namespace with the {ref}`runtime_context APIs <runtime-context-apis>`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/namespaces.py
:language: python
:start-after: __get_namespace_start__
:end-before: __get_namespace_end__
```
:::


:::{tab-item} Java
```java
System.setProperty("ray.job.namespace", "colors");
try {
    Ray.init();
    // Will print namespace name "colors".
    System.out.println(Ray.getRuntimeContext().getNamespace());
} finally {
    Ray.shutdown();
}
```
:::

:::{tab-item} C++
```c++
ray::RayConfig config;
config.ray_namespace = "colors";
ray::Init(config);
// Will print namespace name "colors".
std::cout << ray::GetNamespace() << std::endl;
ray::Shutdown();
```
:::
::::
