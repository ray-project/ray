---
myst:
  html_meta:
    description: "Internals of a Ray Core task from definition through invocation, scheduling, execution, and returning its value to the caller."
---

(task-lifecycle)=

# Task lifecycle

This page describes the lifecycle of a task in Ray Core, including how you define a task and how Ray schedules and executes it. The page uses the following code as an example, and the internals it describes are based on Ray 2.48.


```{testcode}
import ray

@ray.remote
def my_task(arg):
    return f"Hello, {arg}!"

obj_ref = my_task.remote("Ray")
print(ray.get(obj_ref))
```

```{testoutput}
Hello, Ray!
```


## Defining a remote function

The first step in the task lifecycle is defining a remote function with the {func}`ray.remote` decorator. {func}`ray.remote` wraps the Python function and returns an instance of [RemoteFunction](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L41). `RemoteFunction` stores the underlying function and all the Ray task {meth}`options <ray.remote_function.RemoteFunction.options>` that you specify, such as `num_cpus`.


## Invoking a remote function

After you define a remote function, you can invoke it with the `.remote()` method. Each invocation of a remote function creates a Ray task. This method submits the task for execution and returns an `ObjectRef` that you can use to retrieve the result later. The `.remote()` method does the following:

1. [Pickles the underlying function](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L366) into bytes and [stores the bytes in the GCS key-value store](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L372) with a [key](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_private/function_manager.py#L223), so that the remote executor can later get the bytes, unpickle them, and execute the function. The remote executor is the core worker process that executes the task. This step runs once per remote function definition instead of once per invocation.
1. [Calls](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L490) the Cython [`submit_task`](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L3692) function, which [prepares](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L901) the arguments and calls the C++ [`CoreWorker::SubmitTask`](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2514). Arguments fall into three types:

   1. A pass-by-reference argument is an `ObjectRef`.
   1. A pass-by-value inline argument is a [small](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L967) Python object, and the total size of such arguments so far is below the [threshold](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L968). In this case, Ray pickles the argument, sends it to the remote executor as part of the `PushTask` RPC, and unpickles it there. This is called *inlining*, and it doesn't involve the Plasma store.
   1. A pass-by-value non-inline argument is a normal Python object that doesn't meet the inline criteria, for example because it's too big. Ray [puts](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L987) it in the local Plasma store and replaces the argument with the generated `ObjectRef`, so it's effectively equivalent to `.remote(ray.put(arg))`.

1. `CoreWorker` [builds](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2542) a [TaskSpecification](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/common/task/task_spec.h#L258) that contains all the information about the task, including the [ID](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/includes/function_descriptor.pxi#L265) of the function, all the options you specify, and the arguments. Ray later sends this spec to the executor for execution.
1. `CoreWorker` [submits](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2587) the TaskSpecification to [NormalTaskSubmitter](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L28) asynchronously. This means the `.remote()` call returns immediately, and Ray schedules and executes the task asynchronously.

## Scheduling a task

After `CoreWorker` submits the task to `NormalTaskSubmitter`, Ray selects a worker process on some Ray node to execute the task. This selection process is called *scheduling*, and it works as follows:

1. `NormalTaskSubmitter` first [waits](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L33) for all the `ObjectRef` arguments to be available. An argument is available when the task that produces it has finished execution and its data is available somewhere in the cluster. Ray passes the argument to the executor based on where its object is:

   1. If the object that the `ObjectRef` points to is in the Plasma store, Ray sends the `ObjectRef` itself to the executor. The executor resolves the `ObjectRef` to the actual data before calling the user function. If the data is on another node, the executor's raylet pulls it into the local Plasma store before it grants the worker lease.
   1. If the object that the `ObjectRef` points to is in the caller's memory store, Ray [inlines](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/dependency_resolver.cc#L26) the data and sends it to the executor as part of the `PushTask` RPC, the same as other pass-by-value inline arguments.

1. After all the arguments are available, `NormalTaskSubmitter` tries to find an idle worker to execute the task. `NormalTaskSubmitter` gets workers for task execution from the raylet through a process called *worker lease*, and this is where scheduling happens. Specifically, it [sends](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L350) a `RequestWorkerLease` RPC for a worker lease to a [selected](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L339) raylet. The selected raylet is either the local raylet or a raylet that data locality favors.
1. The raylet [handles](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/raylet/node_manager.cc#L1754) the `RequestWorkerLease` RPC.
1. When the `RequestWorkerLease` RPC returns with a leased worker address in the response, the caller has a worker lease to execute the task. If the `RequestWorkerLease` response contains another raylet address instead, `NormalTaskSubmitter` then requests a worker lease from that raylet. This process continues until `NormalTaskSubmitter` obtains a worker lease.

## Executing a task

After `NormalTaskSubmitter` obtains a leased worker, task execution starts. Execution proceeds as follows:

1. `NormalTaskSubmitter` [sends](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L568) a `PushTask` RPC to the leased worker with the `TaskSpecification` to execute.
1. The executor [receives](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3885) the `PushTask` RPC and executes the task. Execution passes through these call sites in order: [1](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3948), [2](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/task_receiver.cc#L62), [3](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L520), [4](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3420), and [5](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L2318).
1. The first step of executing the task is [getting](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3789) all the pass-by-reference arguments from the local Plasma store. Ray already pulled the data from the remote Plasma store to the local Plasma store during scheduling.
1. Then the executor [gets](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L2206) the pickled function bytes from the GCS key-value store and unpickles them.
1. Next, the executor [unpickles](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L1871) the arguments.
1. Finally, the executor [calls](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L1925) the user function.

## Getting the return value

After the user function executes, the caller can get the return values. The process works as follows:

1. After the user function returns, the executor [gets and stores](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L4308) all the return values. If a return value is a [small](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3272) object and the total size of such return values so far is below the [threshold](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3274), the executor returns it directly to the caller as part of the `PushTask` RPC response. [Otherwise](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3279), the executor puts it in the local Plasma store and returns the reference to the caller.
1. When the caller [receives](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L579) the `PushTask` RPC response, it [stores](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/task_manager.cc#L511) the return values in the local memory store. For a small return value, it stores the actual data. For a big return value, it stores a special value indicating that the data is in the Plasma store.
1. When the caller [adds](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/task_manager.cc#L511) the return value to the local memory store, `ray.get()` [unblocks](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/store_provider/memory_store/memory_store.cc#L373). If the object is small, `ray.get()` returns the value directly. If the object is big, `ray.get()` [gets](https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L1965) it from the local Plasma store, and first pulls it from a remote Plasma store if needed.
