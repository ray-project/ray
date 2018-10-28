An Overview of the Internals
============================

In this document, we trace through in more detail what happens at the system
level when certain API calls are made.

Connecting to Ray
-----------------

There are two ways that a Ray script can be initiated. It can either be run in a
standalone fashion or it can be connect to an existing Ray cluster.

Running Ray standalone
~~~~~~~~~~~~~~~~~~~~~~

Ray can be used standalone by calling ``ray.init()`` within a script. When the
call to ``ray.init()`` happens, all of the relevant processes are started.
These include a local scheduler, an object store and manager, a Redis server,
and a number of worker processes.

When the script exits, these processes will be killed.

**Note:** This approach is limited to a single machine.

Connecting to an existing Ray cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To connect to an existing Ray cluster, simply pass the argument address of the
Redis server as the ``redis_address=`` keyword argument into ``ray.init``. In
this case, no new processes will be started when ``ray.init`` is called, and
similarly the processes will continue running when the script exits. In this
case, all processes except workers that correspond to actors are shared between
different driver processes.

Defining a remote function
--------------------------

A central component of this system is the **centralized control plane**. This is
implemented using one or more Redis servers. `Redis`_ is an in-memory key-value
store.

.. _`Redis`: https://github.com/antirez/redis

We use the centralized control plane in two ways. First, as persistent store of
the system's control state. Second, as a message bus for communication between
processes (using Redis's publish-subscribe functionality).

Now, consider a remote function definition as below.

.. code-block:: python

  @ray.remote
  def f(x):
      return x + 1

When the remote function is defined as above, the function is immediately
pickled, assigned a unique ID, and stored in a Redis server. You can view the
remote functions in the centralized control plane as below.

.. code-block:: python

  TODO: Fill this in.

Each worker process has a separate thread running in the background that
listens for the addition of remote functions to the centralized control state.
When a new remote function is added, the thread fetches the pickled remote
function, unpickles it, and can then execute that function.

Notes and limitations
~~~~~~~~~~~~~~~~~~~~~

- Because we export remote functions as soon as they are defined, that means
  that remote functions can't close over variables that are defined after the
  remote function is defined. For example, the following code gives an error.

  .. code-block:: python

    @ray.remote
    def f(x):
        return helper(x)

    def helper(x):
        return x + 1

  If you call ``f.remote(0)``, it will give an error of the form.

  .. code-block:: python

    Traceback (most recent call last):
        File "<ipython-input-3-12a5beeb2306>", line 3, in f
    NameError: name 'helper' is not defined

  On the other hand, if ``helper`` is defined before ``f``, then it will work.

Calling a remote function
-------------------------

When a driver or worker invokes a remote function, a number of things happen.

- First, a task object is created. The task object includes the following.

  - The ID of the function being called.
  - The IDs or values of the arguments to the function. Python primitives like
    integers or short strings will be pickled and included as part of the task
    object. Larger or more complex objects will be put into the object store
    with an internal call to ``ray.put``, and the resulting IDs are included in
    the task object. Object IDs that are passed directly as arguments are also
    included in the task object.
  - The ID of the task. This is generated uniquely from the above content.
  - The IDs for the return values of the task. These are generated uniquely
    from the above content.
- The task object is then sent to the local scheduler on the same node as the
  driver or worker.
- The local scheduler makes a decision to either schedule the task locally or to
  pass the task on to another local scheduler.

  - If all of the task's object dependencies are present in the local object
    store and there are enough CPU and GPU resources available to execute the
    task, then the local scheduler will assign the task to one of its
    available workers.
  - If those conditions are not met, the task will be passed on to a global
    scheduler. This is done by adding the task to the **task table**, which is
    part of the centralized control state.
    The task table can be inspected as follows.

    .. code-block:: python

      TODO: Fill this in.

    A global scheduler will be notified of the update and will assign the task
    to a local scheduler by updating the task's state in the task table. The
    local scheduler will be notified and pull the task object.
- Once a task has been scheduled to a local scheduler, whether by itself or by
  a global scheduler, the local scheduler queues the task for execution. A task
  is assigned to a worker when enough resources become available and the object
  dependencies are available locally, in first-in, first-out order.
- When the task has been assigned to a worker, the worker executes the task and
  puts the task's return values into the object store. The object store will
  then update the **object table**, which is part of the centralized control
  state, to reflect the fact that it contains the newly created objects. The
  object table can be viewed as follows.

  .. code-block:: python

    TODO: Fill this in.

  When the task's return values are placed into the object store, they are first
  serialized into a contiguous blob of bytes using the `Apache Arrow`_ data
  layout, which is helpful for efficiently sharing data between processes using
  shared memory.

.. _`Apache Arrow`: https://arrow.apache.org/

Notes and limitations
~~~~~~~~~~~~~~~~~~~~~

- When an object store on a particular node fills up, it will begin evicting
  objects in a least-recently-used manner. If an object that is needed later is
  evicted, then the call to ``ray.get`` for that object will initiate the
  reconstruction of the object. The local scheduler will attempt to reconstruct
  the object by replaying its task lineage.

TODO: Limitations on reconstruction.

Getting an object ID
--------------------

Several things happen when a driver or worker calls ``ray.get`` on an object ID.

.. code-block:: python

  ray.get(x_id)

- The driver or worker goes to the object store on the same node and requests
  the relevant object. Each object store consists of two components, a
  shared-memory key-value store of immutable objects, and a manager to
  coordinate the transfer of objects between nodes.

  - If the object is not present in the object store, the manager checks the
    object table to see which other object stores, if any, have the object. It
    then requests the object directly from one of those object stores, via its
    manager. If the object doesn't exist anywhere, then the centralized control
    state will notify the requesting manager when the object is created. If the
    object doesn't exist anywhere because it has been evicted from all object
    stores, the worker will also request reconstruction of the object from the
    local scheduler. These checks repeat periodically until the object is
    available in the local object store, whether through reconstruction or
    through object transfer.
- Once the object is available in the local object store, the driver or worker
  will map the relevant region of memory into its own address space (to avoid
  copying the object), and will deserialize the bytes into a Python object.
  Note that any numpy arrays that are part of the object will not be copied.
