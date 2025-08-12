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

The first step in the task lifecycle is defining a remote function using the :func:`ray.remote` decorator. :func:`ray.remote` wraps the Python function and returns an instance of :class:`~ray.remote_function.RemoteFunction`.
``RemoteFunction`` stores the underlying function and all the user specified Ray task options such as ``num_cpus``.


Invoking a remote function
--------------------------

Once a remote function is defined, it can be invoked using the `.remote()` method. Each invocation of a remote function is a Ray task. This method submits the task for execution and returns an object reference (``ObjectRef``) that can be used to retrieve the result later.
Under the hood, `.remote()` does the following:

1. `Pickles the underlying function <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L366>`__ into bytes and `stores the bytes in GCS key-value store <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L372>`__ with a `key <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_private/function_manager.py#L223>`__ so that, later on, the remote executor can get the bytes, unpickle, and execute the function. This is done once per remote function definition instead of once per invocation.
2. `Calls <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/remote_function.py#L490>`__ Cython `submit_task <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L3692>`__.
3. Cython ``submit_task`` pickles the arguments and calls the C++ `CoreWorker::SubmitTask <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2514>`__.
   An argument can be pass-by-reference or pass-by-value. Pass-by-reference means the argument is an ``ObjectRef`` itself and the data is in the plasma store. The executor will resolve the ``ObjectRef`` to the actual data (pull from remote plasma store if needed) before calling the user function.
   Pass-by-value means the argument is a normal Python object and it will be pickled, sent to the remote executor (as part of the ``PushTask`` RPC), and unpickled there. Plasma store is not involved in this case.
   If the argument is a `big <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L985>`__ Python object, it is put in the local plasma store and the argument is replaced by the generated ``ObjectRef``, so it's effectively equivalent to ``.remote(ray.put(big_arg))``.
4. ``CoreWorker`` `builds <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2542>`__ a `TaskSpecification <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/common/task/task_spec.h#L258>`__ that contains all the information about the task including the `ID <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/includes/function_descriptor.pxi#L265>`__ of the function, all the user specified options and the arguments. This spec will be sent to the executor for execution.
5. The TaskSpecification is `submitted <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L2587>`__ to `NormalTaskSubmitter <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L28>`__ asynchronously. This means the ``.remote()`` call returns immediately and the task is scheduled and executed asynchronously.

Scheduling a task
-----------------

Once the task is submitted to ``NormalTaskSubmitter``, a worker process on some Ray node is selected to execute the task and this process is called scheduling.

1. ``NormalTaskSubmitter`` first `waits <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L33>`__ for all the ``ObjectRef`` arguments (i.e. pass-by-reference) to be available. Available means tasks that produce those ``ObjectRef``\s finished execution and the data is available somewhere in the cluster.
2. Once all the arguments are available, ``NormalTaskSubmitter`` will try to find an idle worker to execute the task. ``NormalTaskSubmitter`` gets workers for task execution from raylet via a process called worker lease and this is where scheduling happens.
   Specifically, it will `send <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L350>`__ a ``RequestWorkerLease`` RPC to a `selected <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L339>`__ (it's either the local raylet or a locality-favored raylet) raylet for a worker lease.
3. Raylet `handles <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/raylet/node_manager.cc#L1754>`__ the ``RequestWorkerLease`` RPC. It either returns a lease to a local worker to the caller or asks the caller to retry worker lease from a different raylet.
4. When the ``RequestWorkerLease`` RPC returns and a leased worker is included in the response, the worker is granted to execute the task. If the ``RequestWorkerLease`` response contains other raylet to retry worker lease, ``NormalTaskSubmitter`` will then `retry <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L451>`__ worker lease from the specified raylet. This process continues until a worker lease is obtained.

Executing a task
----------------

Once a leased worker is obtained, the task execution starts.

1. ``NormalTaskSubmitter`` `sends <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L568>`__ a ``PushTask`` RPC to the leased worker with the ``TaskSpecification`` to execute.
2. The executor `receives <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3885>`__ the ``PushTask`` RPC and executes (`1 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3948>`__ -> `2 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/task_receiver.cc#L62>`__ -> `3 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L520>`__ -> `4 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3420>`__ -> `5 <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L2318>`__) the task.
3. First step of executing the task is `getting <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3789>`__ all the pass-by-reference arguments from the local plasma store.
4. Then the executor `gets <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L2206>`__ the pickled function bytes from GCS key-value store and unpickles it.
5. The next step is `unpickling <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L1871>`__ the arguments.
6. Finally, the user function is `called <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L1925>`__.

Geting the return value
-----------------------

After the user function is executed, the caller can get the return values.

1. After the user function returns, the executor `gets and stores <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/python/ray/_raylet.pyx#L4308>`__ all the return values. If the return value is a `small <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3271>`__ object, it is returned directly to the caller as part of the ``PushTask`` RPC response. If the return value is a `big <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/core_worker.cc#L3279>`__ object, it is put in the local plasma store and the reference is returned to the caller.
2. When the caller `receives <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/transport/normal_task_submitter.cc#L579>`__ the ``PushTask`` RPC response, it `stores <https://github.com/ray-project/ray/blob/e832bd843870cde7e66e7019ea82a366836f24d5/src/ray/core_worker/task_manager.cc#L511>`__ the return values (actual data if the return value is small or a special value indicating the data is in plasma store if the return value is big) in the local memory store.
3. When the return value is added to the local memory store, ``ray.get()`` is unblocked and returns the value directly if the object is small, or it will get from the local plasma store if the object is big.
