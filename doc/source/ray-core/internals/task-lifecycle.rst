.. _task-lifecycle:

Task Lifecycle
==============

This doc talks about the lifecycle of a task in Ray Core, including how tasks are defined, scheduled and executed.
We will use the following code as an example and the internals are based on Ray 2.48.


.. testcode::

  import ray

  @ray.remote
  def my_task(arg):
      return f"Hello, {arg}!"

  obj_ref = my_task.remote("Ray")
  print(ray.get(obj_ref))

.. testoutput::

  Hello, Ray!


Defining a remote function
--------------------------

The first step in the task lifecycle is defining a remote function using the :func:`ray.remote` decorator. :func:`ray.remote` wraps the Python function and returns an instance of `RemoteFunction <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L41>`__.
``RemoteFunction`` stores the underlying function and all the user specified Ray task :meth:`options <ray.remote_function.RemoteFunction.options>` such as ``num_cpus``.


Invoking a remote function
--------------------------

Once a remote function is defined, it can be invoked using the `.remote()` method. Each invocation of a remote function creates a Ray task. This method submits the task for execution and returns an object reference (``ObjectRef``) that can be used to retrieve the result later.
Under the hood, `.remote()` does the following:

1. `Pickles the underlying function <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L366>`__ into bytes and `stores the bytes in GCS key-value store <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L372>`__ with a `key <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_private/function_manager.py#L223>`__ so that, later on, the remote executor (the core worker process that will execute the task) can get the bytes, unpickle, and execute the function. This is done once per remote function definition instead of once per invocation.
2. `Calls <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L490>`__ Cython `submit_task <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L3692>`__ which `prepares <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L901>`__ the arguments (3 types) and calls the C++ `CoreWorker::SubmitTask <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2514>`__.

   1. Pass-by-reference argument: the argument is an ``ObjectRef``.
   2. Pass-by-value inline argument: the argument is a `small <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L967>`__ Python object and the total size of such arguments so far is below the `threshold <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L968>`__. In this case, it will be pickled, sent to the remote executor (as part of the ``PushTask`` RPC), and unpickled there. This is called inlining and plasma store is not involved in this case.
   3. Pass-by-value non-inline argument: the argument is a normal Python object but it doesn't meet the inline criteria (e.g. size is too big), it is `put <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L987>`__ in the local plasma store and the argument is replaced by the generated ``ObjectRef``, so it's effectively equivalent to ``.remote(ray.put(arg))``.

3. ``CoreWorker`` `builds <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2542>`__ a `TaskSpecification <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/common/task/task_spec.h#L258>`__ that contains all the information about the task including the `ID <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/includes/function_descriptor.pxi#L265>`__ of the function, all the user specified options and the arguments. This spec will be sent to the executor for execution.
4. The TaskSpecification is `submitted <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2587>`__ to `NormalTaskSubmitter <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L28>`__ asynchronously. This means the ``.remote()`` call returns immediately and the task is scheduled and executed asynchronously.

Scheduling a task
-----------------

Once the task is submitted to ``NormalTaskSubmitter``, a worker process on some Ray node is selected to execute the task and this process is called scheduling.

1. ``NormalTaskSubmitter`` first `waits <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L33>`__ for all the ``ObjectRef`` arguments to be available. Available means tasks that produce those ``ObjectRef``\s finished execution and the data is available somewhere in the cluster.

   1. If the object pointed to by the ``ObjectRef`` is in the plasma store, the ``ObjectRef`` itself is sent to the executor and the executor will resolve the ``ObjectRef`` to the actual data (pull from remote plasma store if needed) before calling the user function.
   2. If the object pointed to by the ``ObjectRef`` is in the caller memory store, the data is `inlined <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/dependency_resolver.cc#L26>`__ and sent to the executor as part of the ``PushTask`` RPC just like other pass-by-value inline arguments.

2. Once all the arguments are available, ``NormalTaskSubmitter`` will try to find an idle worker to execute the task. ``NormalTaskSubmitter`` gets workers for task execution from raylet via a process called worker lease and this is where scheduling happens.
   Specifically, it will `send <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L350>`__ a ``RequestWorkerLease`` RPC to a `selected <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L339>`__ (it's either the local raylet or a data-locality-favored raylet) raylet for a worker lease.
3. Raylet `handles <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/raylet/node_manager.cc#L1754>`__ the ``RequestWorkerLease`` RPC.
4. When the ``RequestWorkerLease`` RPC returns with a leased worker address in the response, a worker lease is granted to the caller to execute the task. If the ``RequestWorkerLease`` response contains another raylet address instead, ``NormalTaskSubmitter`` will then request a worker lease from the specified raylet. This process continues until a worker lease is obtained.

Executing a task
----------------

Once a leased worker is obtained, the task execution starts.

1. ``NormalTaskSubmitter`` `sends <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L568>`__ a ``PushTask`` RPC to the leased worker with the ``TaskSpecification`` to execute.
2. The executor `receives <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3885>`__ the ``PushTask`` RPC and executes (`1 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3948>`__ -> `2 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/task_receiver.cc#L62>`__ -> `3 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L520>`__ -> `4 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3420>`__ -> `5 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L2318>`__) the task.
3. First step of executing the task is `getting <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3789>`__ all the pass-by-reference arguments from the local plasma store (data is already pulled from remote plasma store to the local plasma store during scheduling).
4. Then the executor `gets <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L2206>`__ the pickled function bytes from GCS key-value store and unpickles it.
5. The next step is `unpickling <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L1871>`__ the arguments.
6. Finally, the user function is `called <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L1925>`__.

Getting the return value
------------------------

After the user function is executed, the caller can get the return values.

1. After the user function returns, the executor `gets and stores <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L4308>`__ all the return values. If the return value is a `small <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3272>`__ object and the total size of such return values so far is below the `threshold <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3274>`__, it is returned directly to the caller as part of the ``PushTask`` RPC response. `Otherwise <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3279>`__, it is put in the local plasma store and the reference is returned to the caller.
2. When the caller `receives <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L579>`__ the ``PushTask`` RPC response, it `stores <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/task_manager.cc#L511>`__ the return values (actual data if the return value is small or a special value indicating the data is in plasma store if the return value is big) in the local memory store.
3. When the return value is `added <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/task_manager.cc#L511>`__ to the local memory store, ``ray.get()`` is `unblocked <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/store_provider/memory_store/memory_store.cc#L373>`__ and returns the value directly if the object is small, or it will `get <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L1965>`__ from the local plasma store (pull from remote plasma store first if needed) if the object is big.
