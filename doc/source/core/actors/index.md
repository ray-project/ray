---
myst:
  html_meta:
    description: "Turn Python classes into stateful Ray actors: declare resources, call methods, pass handles around, use generators, and cancel actor tasks."
---

(ray-remote-classes)=
(actor-guide)=

# Actors

Actors extend the Ray API from functions, which run as tasks, to classes. An actor is a stateful worker or service. When you instantiate an actor, Ray creates a new worker and schedules the actor's methods on that worker. The methods can access and mutate the worker's state.

::::{tab-set}
:::{tab-item} Python
The `ray.remote` decorator indicates that instances of the `Counter` class are actors. Each actor runs in its own Python process.

```{testcode}
import ray

@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def get_counter(self):
        return self.value

# Create an actor from this class.
counter = Counter.remote()
```
:::

:::{tab-item} Java
Use `Ray.actor` to create actors from regular Java classes.

```java
// A regular Java class.
public class Counter {

  private int value = 0;

  public int increment() {
    this.value += 1;
    return this.value;
  }
}

// Create an actor from this class.
// `Ray.actor` takes a factory method that can produce
// a `Counter` object. Here, we pass `Counter`'s constructor
// as the argument.
ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();
```
:::

:::{tab-item} C++
Use `ray::Actor` to create actors from regular C++ classes.

```c++
// A regular C++ class.
class Counter {

private:
    int value = 0;

public:
  int Increment() {
    value += 1;
    return value;
  }
};

// Factory function of Counter class.
static Counter *CreateCounter() {
    return new Counter();
};

RAY_REMOTE(&Counter::Increment, CreateCounter);

// Create an actor from this class.
// `ray::Actor` takes a factory method that can produce
// a `Counter` object. Here, we pass `Counter`'s factory function
// as the argument.
auto counter = ray::Actor(CreateCounter).Remote();
```
:::
::::



To see the states of your actors, run `ray list actors` from the {ref}`State API <state-api-overview-ref>`:

```bash
# This API is only available when you install Ray with `pip install "ray[default]"`.
ray list actors
```

```bash
======== List: 2023-05-25 10:10:50.095099 ========
Stats:
------------------------------
Total: 1

Table:
------------------------------
    ACTOR_ID                          CLASS_NAME    STATE      JOB_ID  NAME    NODE_ID                                                     PID  RAY_NAMESPACE
 0  9e783840250840f87328c9f201000000  Counter       ALIVE    01000000          13a475571662b784b4522847692893a823c78f1d3fd8fd32a2624923  38906  ef9de910-64fb-4575-8eb5-50573faa3ddf
```


## Specifying required resources

(actor-resource-guide)=

Specify an actor's resource requirements as the following examples show. See {ref}`resource-requirements` for details.

::::{tab-set}
:::{tab-item} Python
```{testcode}
# Specify required resources for an actor.
@ray.remote(num_cpus=2, num_gpus=0.5)
class Actor:
    pass
```
:::

:::{tab-item} Java
```java
// Specify required resources for an actor.
Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();
```
:::

:::{tab-item} C++
```c++
// Specify required resources for an actor.
ray::Actor(CreateCounter).SetResource("CPU", 2.0).SetResource("GPU", 0.5).Remote();
```
:::
::::


## Calling the actor

To interact with an actor, call its methods with the `remote` operator. Then call `get` on the returned object ref to retrieve the value.

::::{tab-set}
:::{tab-item} Python
```{testcode}
# Call the actor.
obj_ref = counter.increment.remote()
print(ray.get(obj_ref))
```

```{testoutput}
1
```
:::

:::{tab-item} Java
```java
// Call the actor.
ObjectRef<Integer> objectRef = counter.task(&Counter::increment).remote();
Assert.assertTrue(objectRef.get() == 1);
```
:::

:::{tab-item} C++
```c++
// Call the actor.
auto object_ref = counter.Task(&Counter::increment).Remote();
assert(*object_ref.Get() == 1);
```
:::
::::

Methods on different actors execute in parallel, and methods on the same actor execute serially in the order you call them. Methods on the same actor share state with one another, as the following example shows.

:::{note}
Actor state is per actor instance. Each actor runs in its own process, so class variables and static fields aren't shared across actor instances. Mutations to class-level state stay local to that actor process. To share mutable state across actors, store it in another actor and pass that actor's handle to the code that needs it. See {doc}`../patterns/global-variables` for an anti-pattern and a replacement pattern.
:::

::::{tab-set}
:::{tab-item} Python
```{testcode}
# Create ten Counter actors.
counters = [Counter.remote() for _ in range(10)]

# Increment each Counter once and get the results. These tasks all happen in
# parallel.
results = ray.get([c.increment.remote() for c in counters])
print(results)

# Increment the first Counter five times. These tasks are executed serially
# and share state.
results = ray.get([counters[0].increment.remote() for _ in range(5)])
print(results)
```

```{testoutput}
[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
[2, 3, 4, 5, 6]
```
:::

:::{tab-item} Java
```java
// Create ten Counter actors.
List<ActorHandle<Counter>> counters = new ArrayList<>();
for (int i = 0; i < 10; i++) {
    counters.add(Ray.actor(Counter::new).remote());
}

// Increment each Counter once and get the results. These tasks all happen in
// parallel.
List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
for (ActorHandle<Counter> counterActor : counters) {
    objectRefs.add(counterActor.task(Counter::increment).remote());
}
// prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
System.out.println(Ray.get(objectRefs));

// Increment the first Counter five times. These tasks are executed serially
// and share state.
objectRefs = new ArrayList<>();
for (int i = 0; i < 5; i++) {
    objectRefs.add(counters.get(0).task(Counter::increment).remote());
}
// prints [2, 3, 4, 5, 6]
System.out.println(Ray.get(objectRefs));
```
:::

:::{tab-item} C++
```c++
// Create ten Counter actors.
std::vector<ray::ActorHandle<Counter>> counters;
for (int i = 0; i < 10; i++) {
    counters.emplace_back(ray::Actor(CreateCounter).Remote());
}

// Increment each Counter once and get the results. These tasks all happen in
// parallel.
std::vector<ray::ObjectRef<int>> object_refs;
for (ray::ActorHandle<Counter> counter_actor : counters) {
    object_refs.emplace_back(counter_actor.Task(&Counter::Increment).Remote());
}
// prints 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
auto results = ray::Get(object_refs);
for (const auto &result : results) {
    std::cout << *result;
}

// Increment the first Counter five times. These tasks are executed serially
// and share state.
object_refs.clear();
for (int i = 0; i < 5; i++) {
    object_refs.emplace_back(counters[0].Task(&Counter::Increment).Remote());
}
// prints 2, 3, 4, 5, 6
results = ray::Get(object_refs);
for (const auto &result : results) {
    std::cout << *result;
}
```
:::
::::

## Passing around actor handles

You can pass actor handles into other tasks. You can also define remote functions or actor methods that use actor handles.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import time

@ray.remote
def f(counter):
    for _ in range(10):
        time.sleep(0.1)
        counter.increment.remote()
```
:::

:::{tab-item} Java
```java
public static class MyRayApp {

  public static void foo(ActorHandle<Counter> counter) throws InterruptedException {
    for (int i = 0; i < 1000; i++) {
      TimeUnit.MILLISECONDS.sleep(100);
      counter.task(Counter::increment).remote();
    }
  }
}
```
:::

:::{tab-item} C++
```c++
void Foo(ray::ActorHandle<Counter> counter) {
    for (int i = 0; i < 1000; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        counter.Task(&Counter::Increment).Remote();
    }
}
```
:::
::::

After you instantiate an actor, you can pass its handle to multiple tasks.

::::{tab-set}
:::{tab-item} Python
```{testcode}
counter = Counter.remote()

# Start some tasks that use the actor.
[f.remote(counter) for _ in range(3)]

# Print the counter value.
for _ in range(10):
    time.sleep(0.1)
    print(ray.get(counter.get_counter.remote()))
```

```{testoutput}
:options: +MOCK

0
3
8
10
15
18
20
25
30
30
```
:::

:::{tab-item} Java
```java
ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

// Start some tasks that use the actor.
for (int i = 0; i < 3; i++) {
  Ray.task(MyRayApp::foo, counter).remote();
}

// Print the counter value.
for (int i = 0; i < 10; i++) {
  TimeUnit.SECONDS.sleep(1);
  System.out.println(counter.task(Counter::getCounter).remote().get());
}
```
:::

:::{tab-item} C++
```c++
auto counter = ray::Actor(CreateCounter).Remote();

// Start some tasks that use the actor.
for (int i = 0; i < 3; i++) {
  ray::Task(Foo).Remote(counter);
}

// Print the counter value.
for (int i = 0; i < 10; i++) {
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::cout << *counter.Task(&Counter::GetCounter).Remote().Get() << std::endl;
}
```
:::
::::


## Type hints and static typing for actors

Ray supports Python type hints for remote functions and actors, which improves IDE support and static type checking. To get the best type inference and pass type checkers with actors, follow these patterns:

- Prefer `ray.remote(MyClass)` over `@ray.remote` for actors. Instead of decorating your class with `@ray.remote`, use `ActorClass = ray.remote(MyClass)`. This form preserves the original class type, so type checkers and IDEs can infer the correct types.

- Use `@ray.method` for actor methods. Decorate actor methods with `@ray.method` to get type hints for remote method calls on actor handles.

- Use the `ActorClass` and `ActorProxy` types. When you instantiate an actor, annotate the handle as `ActorProxy[MyClass]` to get type hints for remote methods.

The following example applies all three patterns:

```{testcode}
import ray
from ray.actor import ActorClass, ActorProxy

class Counter:
    def __init__(self):
        self.value = 0

    @ray.method
    def increment(self) -> int:
        self.value += 1
        return self.value

CounterActor: ActorClass[Counter] = ray.remote(Counter)
counter: ActorProxy[Counter] = CounterActor.remote()

# Type checkers and IDEs will now provide type hints for remote methods
obj_ref: ray.ObjectRef[int] = counter.increment.remote()
print(ray.get(obj_ref))
```

For details and advanced patterns, see {ref}`Type hints in Ray <core-type-hint>`.


## Generators
Ray is compatible with Python generator syntax. See {ref}`Ray generators <generators>` for details.

## Cancelling actor tasks

Cancel actor tasks by calling {func}`ray.cancel() <ray.cancel>` on the returned object ref.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/actors.py
:language: python
:start-after: __cancel_start__
:end-before: __cancel_end__
```
:::
::::


Cancellation behavior depends on the task's current state:

- **Unscheduled tasks**: If Ray hasn't scheduled an actor task yet, Ray attempts to cancel the scheduling. If the cancellation succeeds at this stage, calling `ray.get(actor_task_ref)` raises a {class}`TaskCancelledError <ray.exceptions.TaskCancelledError>`.
- **Running tasks on regular or threaded actors**: For tasks on a single-threaded or multi-threaded actor, Ray sets a cancellation flag that you can check with `ray.get_runtime_context().is_canceled()`. To cancel gracefully, check the cancellation status periodically within the task.
- **Running async actor tasks**: For tasks on {ref}`async actors <async-actors>`, Ray tries to cancel the associated `asyncio.Task`. This cancellation follows the semantics of [asyncio task cancellation](https://docs.python.org/3/library/asyncio-task.html#task-cancellation). If the async function doesn't `await`, the `asyncio.Task` isn't interrupted in the middle of execution. Async actors don't support `ray.get_runtime_context().is_canceled()`, and calling it raises a `RuntimeError`.
- **Cancellation guarantee**: Ray attempts to cancel tasks on a *best-effort* basis, so cancellation isn't always guaranteed. For example, if the cancellation request doesn't reach the executor, Ray might not cancel the task. To check whether Ray cancelled the task, call `ray.get(actor_task_ref)`.
- **Recursive cancellation**: Ray tracks all child tasks and actor tasks. When you pass `recursive=True`, Ray cancels all child tasks and actor tasks.

### Detecting cancellation in running actor tasks

In a non-async actor task, call `ray.get_runtime_context().is_canceled()` periodically to check for a cancellation request. When the task detects cancellation, it can run cleanup operations and exit gracefully.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/actors.py
:language: python
:start-after: __cancel_graceful_actor_start__
:end-before: __cancel_graceful_actor_end__
```
:::
::::

:::{note}
- Ray doesn't support direct interruption of non-async actor tasks. Check `is_canceled()` periodically to detect cancellation requests.
- Async actor tasks don't support `is_canceled()`, and calling it raises a `RuntimeError`.
:::

## Scheduling

For each actor, Ray chooses a node to run it on. Ray bases the scheduling decision on factors such as {ref}`the actor's resource requirements <ray-scheduling-resources>` and {ref}`the specified scheduling strategy <ray-scheduling-strategies>`. See {ref}`Ray scheduling <ray-scheduling>` for details.

## Fault tolerance

By default, Ray doesn't {ref}`restart <fault-tolerance-actors>` actors or retry actor tasks when actors crash unexpectedly. To change this behavior, set the `max_restarts` and `max_task_retries` options in {func}`ray.remote() <ray.remote>` and {meth}`.options() <ray.actor.ActorClass.options>`. See {ref}`Ray fault tolerance <fault-tolerance>` for details.

## FAQ: Actors, workers, and resources

What's the difference between a worker and an actor?

Each "Ray worker" is a Python process.

Ray treats workers differently for tasks and actors. For tasks, Ray uses one Ray worker to execute multiple tasks. For actors, Ray starts a Ray worker as a dedicated actor.

* **Tasks**: When Ray starts on a machine, a number of Ray workers start automatically, one per CPU by default. Ray uses them to execute tasks, much like a process pool. If the cluster has 16 CPUs, so that `ray.cluster_resources()["CPU"] == 16`, and you execute 8 tasks with `num_cpus=2`, you end up with 8 of your 16 workers idling.

* **Actors**: An actor is also a Ray worker, but you instantiate it at runtime with `actor_cls.remote()`. All of its methods run in the same process and use the same resources that Ray designates when you define the actor. Unlike with tasks, Ray doesn't reuse the Python processes that run actors. Ray terminates them when you delete the actor.

To make the most of your resources, maximize the time your workers spend working. Allocate enough cluster resources for Ray to run all the actors you need and any other tasks you define. Ray schedules tasks more flexibly than actors, so if you don't need an actor's state, use tasks.

## Task events

By default, Ray traces the execution of actor tasks, reporting task status events and profiling events that the Ray dashboard and the {ref}`State API <state-api-overview-ref>` use.

To disable task event reporting for an actor, set the `enable_task_events` option to `False` in {func}`ray.remote() <ray.remote>` and {meth}`.options() <ray.actor.ActorClass.options>`. This setting reduces task execution overhead by reducing the amount of data Ray sends to the Ray dashboard.

To disable task event reporting for individual actor methods, set the `enable_task_events` option to `False` in {func}`ray.remote() <ray.remote>` and {meth}`.options() <ray.remote_function.RemoteFunction.options>` on the actor method. Method settings override the actor setting:

```{literalinclude} ../doc_code/actors.py
:language: python
:start-after: __enable_task_events_start__
:end-before: __enable_task_events_end__
```


## More about Ray actors

```{toctree}
:maxdepth: 1

named-actors
terminating-actors
async-api
concurrency-group-api
actor-utils
out-of-band-communication
task-orders
```
