---
myst:
  html_meta:
    description: "Run Python functions asynchronously as Ray tasks: request resources, pass object refs, wait for partial results, and cancel tasks."
---

(ray-remote-functions)=

# Tasks

With Ray, you can run arbitrary functions asynchronously on separate worker processes. Such a function is a *Ray remote function*, and each asynchronous invocation of a remote function is a *Ray task*. The following example defines and invokes remote functions:

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __tasks_start__
:end-before: __tasks_end__
```

See the {func}`ray.remote<ray.remote>` API for more details.
:::

:::{tab-item} Java
```java
public class MyRayApp {
  // A regular Java static method.
  public static int myFunction() {
    return 1;
  }
}

// Invoke the above method as a Ray task.
// This will immediately return an object ref (a future) and then create
// a task that will be executed on a worker process.
ObjectRef<Integer> res = Ray.task(MyRayApp::myFunction).remote();

// The result can be retrieved with ``ObjectRef::get``.
Assert.assertTrue(res.get() == 1);

public class MyRayApp {
  public static int slowFunction() throws InterruptedException {
    TimeUnit.SECONDS.sleep(10);
    return 1;
  }
}

// Ray tasks are executed in parallel.
// All computation is performed in the background, driven by Ray's internal event loop.
for(int i = 0; i < 4; i++) {
  // This doesn't block.
  Ray.task(MyRayApp::slowFunction).remote();
}
```
:::

:::{tab-item} C++
```c++
// A regular C++ function.
int MyFunction() {
  return 1;
}
// Register as a remote function by `RAY_REMOTE`.
RAY_REMOTE(MyFunction);

// Invoke the above method as a Ray task.
// This will immediately return an object ref (a future) and then create
// a task that will be executed on a worker process.
auto res = ray::Task(MyFunction).Remote();

// The result can be retrieved with ``ray::ObjectRef::Get``.
assert(*res.Get() == 1);

int SlowFunction() {
  std::this_thread::sleep_for(std::chrono::seconds(10));
  return 1;
}
RAY_REMOTE(SlowFunction);

// Ray tasks are executed in parallel.
// All computation is performed in the background, driven by Ray's internal event loop.
for(int i = 0; i < 4; i++) {
  // This doesn't block.
  ray::Task(SlowFunction).Remote();
}
```
:::
::::

Use `ray summary tasks` from the {ref}`State API <state-api-overview-ref>` to see the running and finished tasks and their counts:

```bash
# This API is only available when you download Ray via `pip install "ray[default]"`
ray summary tasks
```


```bash
======== Tasks Summary: 2023-05-26 11:09:32.092546 ========
Stats:
------------------------------------
total_actor_scheduled: 0
total_actor_tasks: 0
total_tasks: 5


Table (group by func_name):
------------------------------------
    FUNC_OR_CLASS_NAME    STATE_COUNTS    TYPE
0   slow_function         RUNNING: 4      NORMAL_TASK
1   my_function           FINISHED: 1     NORMAL_TASK
```

## Specifying required resources

You can specify resource requirements for tasks. For more details, see {ref}`resource-requirements`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __resource_start__
:end-before: __resource_end__
```
:::

:::{tab-item} Java
```java
// Specify required resources.
Ray.task(MyRayApp::myFunction).setResource("CPU", 4.0).setResource("GPU", 2.0).remote();
```
:::

:::{tab-item} C++
```c++
// Specify required resources.
ray::Task(MyFunction).SetResource("CPU", 4.0).SetResource("GPU", 2.0).Remote();
```
:::
::::

(ray-object-refs)=

## Passing object refs to Ray tasks

In addition to values, you can pass {doc}`object refs <../objects/index>` into remote functions. When the task runs, the argument inside the function body is the underlying value, not the object ref. The following example passes the object ref that one task returns to a second task:

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __pass_by_ref_start__
:end-before: __pass_by_ref_end__
```
:::

:::{tab-item} Java
```java
public class MyRayApp {
    public static int functionWithAnArgument(int value) {
        return value + 1;
    }
}

ObjectRef<Integer> objRef1 = Ray.task(MyRayApp::myFunction).remote();
Assert.assertTrue(objRef1.get() == 1);

// You can pass an object ref as an argument to another Ray task.
ObjectRef<Integer> objRef2 = Ray.task(MyRayApp::functionWithAnArgument, objRef1).remote();
Assert.assertTrue(objRef2.get() == 2);
```
:::

:::{tab-item} C++
```c++
static int FunctionWithAnArgument(int value) {
    return value + 1;
}
RAY_REMOTE(FunctionWithAnArgument);

auto obj_ref1 = ray::Task(MyFunction).Remote();
assert(*obj_ref1.Get() == 1);

// You can pass an object ref as an argument to another Ray task.
auto obj_ref2 = ray::Task(FunctionWithAnArgument).Remote(obj_ref1);
assert(*obj_ref2.Get() == 2);
```
:::
::::

Note the following behaviors:

- Because the second task depends on the output of the first task, Ray doesn't execute the second task until the first task finishes.
- If Ray schedules the two tasks on different machines, it sends the output of the first task, which is the value of `obj_ref1/objRef1`, over the network to the machine where it scheduled the second task.

## Waiting for partial results

Calling `ray.get` on a task's result blocks until the task finishes. After you launch several tasks, you might want to know which ones have finished without blocking on all of them. Use {func}`ray.wait() <ray.wait>` for this, as the following example shows:

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __wait_start__
:end-before: __wait_end__
```
:::

:::{tab-item} Java
```java
WaitResult<Integer> waitResult = Ray.wait(objectRefs, /*num_returns=*/0, /*timeoutMs=*/1000);
System.out.println(waitResult.getReady());  // List of ready objects.
System.out.println(waitResult.getUnready());  // list of unready objects.
```
:::

:::{tab-item} C++
```c++
ray::WaitResult<int> wait_result = ray::Wait(object_refs, /*num_objects=*/0, /*timeout_ms=*/1000);
```
:::
::::

## Generators

Ray is compatible with Python generator syntax. For more details, see {ref}`Ray generators <generators>`.

(ray-task-returns)=

## Multiple returns

By default, a task returns a single object ref. To return multiple object refs, set the `num_returns` option.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __multiple_returns_start__
:end-before: __multiple_returns_end__
```
:::
::::

For tasks that return multiple objects, Ray also supports remote generators, which return one object at a time to reduce memory usage on the worker. You can also set the number of return values dynamically, which is useful when the caller doesn't know how many return values to expect. For use cases, see {ref}`Ray generators <generators>`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __generator_start__
:end-before: __generator_end__
```
:::
::::

(ray-task-cancel)=

## Cancelling tasks

To cancel a task, call {func}`ray.cancel() <ray.cancel>` on the object ref that the task returned.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/tasks.py
:language: python
:start-after: __cancel_start__
:end-before: __cancel_end__
```
:::
::::


## Scheduling

For each task, Ray chooses a node to run it on. Ray bases this decision on factors such as {ref}`the task's resource requirements <ray-scheduling-resources>`, {ref}`the specified scheduling strategy <ray-scheduling-strategies>`, and {ref}`the locations of task arguments <ray-scheduling-locality>`. For more details, see {ref}`Ray scheduling <ray-scheduling>`.

## Fault tolerance

By default, Ray {ref}`retries <task-retries>` tasks that fail because of system failures and specified application-level failures. To change this behavior, set the `max_retries` and `retry_exceptions` options in {func}`ray.remote() <ray.remote>` or {meth}`.options() <ray.remote_function.RemoteFunction.options>`. For more details, see {ref}`Ray fault tolerance <fault-tolerance>`.

(task-events)=

## Task events


By default, Ray traces the execution of tasks, reporting task status events and profiling events that the Ray dashboard and {ref}`State API <state-api-overview-ref>` use.

To disable task events, set the `enable_task_events` option in {func}`ray.remote() <ray.remote>` or {meth}`.options() <ray.remote_function.RemoteFunction.options>`. Disabling task events reduces the overhead of task execution and the amount of data the task sends to the Ray dashboard. Nested tasks don't inherit the task events settings from the parent task, so set them for each task separately.



## More about Ray tasks

```{toctree}
:maxdepth: 1

nested-tasks
```
