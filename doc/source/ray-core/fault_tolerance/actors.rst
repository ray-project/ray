Actor tolerance
===============

Similar to objects, there are two kinds of failures in actors, 1) the actor
process dies; 2) the owner of the actor dies.


Actor process dies
------------------

Ray will automatically restart actors that crash unexpectedly.
This behavior is controlled using ``max_restarts``,
which sets the maximum number of times that an actor will be restarted.
If 0, the actor won't be restarted. If -1, it will be restarted infinitely.
The default value of ``max_restarts`` is 0.
When an actor is restarted, its state will be recreated by rerunning its
constructor.
After the specified number of restarts, subsequent actor methods will
raise a ``RayActorError``.
You can experiment with this behavior by running the following code.

By default, actor tasks execute with at-most-once semantics
(``max_task_retries=0`` in the ``@ray.remote`` decorator). This means that if an
actor task is submitted to an actor that is unreachable, Ray will report the
error with ``RayActorError``, a Python-level exception that is thrown when
``ray.get`` is called on the future returned by the task. Note that this
exception may be thrown even though the task did indeed execute successfully.
For example, this can happen if the actor dies immediately after executing the
task.

Ray also offers at-least-once execution semantics for actor tasks
(``max_task_retries=-1`` or ``max_task_retries > 0``). This means that if an
actor task is submitted to an actor that is unreachable, the system will
automatically retry the task until it receives a reply from the actor. With
this option, the system will only throw a ``RayActorError`` to the application
if one of the following occurs: (1) the actor’s ``max_restarts`` limit has been
exceeded and the actor cannot be restarted anymore, or (2) the
``max_task_retries`` limit has been exceeded for this particular task. The
limit can be set to infinity with ``max_task_retries = -1``.

You can experiment with this behavior by running the following code.

.. code-block:: python

    import os
    import ray

    ray.init()

    @ray.remote(max_restarts=5, max_task_retries=-1)
    class Actor:
        def __init__(self):
            self.counter = 0

        def increment_and_possibly_fail(self):
            # Exit after every 10 tasks.
            if self.counter == 10:
                os._exit(0)
            self.counter += 1
            return self.counter

    actor = Actor.remote()

    # The actor will be reconstructed up to 5 times. The actor is
    # reconstructed by rerunning its constructor. Methods that were
    # executing when the actor died will be retried and will not
    # raise a `RayActorError`. Retried methods may execute twice, once
    # on the failed actor and a second time on the restarted actor.
    for _ in range(50):
        counter = ray.get(actor.increment_and_possibly_fail.remote())
        print(counter)  # Prints the sequence 1-10 5 times.

    # After the actor has been restarted 5 times, all subsequent methods will
    # raise a `RayActorError`.
    for _ in range(10):
        try:
            counter = ray.get(actor.increment_and_possibly_fail.remote())
            print(counter)  # Unreachable.
        except ray.exceptions.RayActorError:
            print('FAILURE')  # Prints 10 times.

For at-least-once actors, the system will still guarantee execution ordering
according to the initial submission order. For example, any tasks submitted
after a failed actor task will not execute on the actor until the failed actor
task has been successfully retried. The system will not attempt to re-execute
any tasks that executed successfully before the failure
(unless :ref:`object reconstruction <object-reconstruction>` is enabled). 

At-least-once execution is best suited for read-only actors or actors with
ephemeral state that does not need to be rebuilt after a failure. For actors
that have critical state, it is best to take periodic checkpoints and either
manually restart the actor or automatically restart the actor with at-most-once
semantics. If the actor’s exact state at the time of failure is needed, the
application is responsible for resubmitting all tasks since the last
checkpoint.

.. note::
    For :ref:`async or threaded actors <async-actors>`, the tasks might
    be executed out of order. Upon actor restart, the system will only retry
    *incomplete* tasks. Previously completed tasks will not be
    re-executed.


One thing needed to point out is that When the actor is restarting, by default
the task will fail immediately, but if the ``max_task_retries`` doesn't equal 0,
it'll wail until the actor is ready:

.. code-block:: python

    import os
    import signal
    from filelock import FileLock

    import ray
    ray.init()

    l = FileLock("tmp.lock")

    @ray.remote(max_restarts=1)
    class Actor:
        def __init__(self):
            l = FileLock("tmp.lock")
            with l:
                pass
        def pid(self):
            return os.getpid()

    a = Actor.remote()
    # Get the pid of the actor
    pid = ray.get(a.pid.remote())

    # Actuire the lock the that the actor will be in restarting
    # status forever
    l.acquire()

    os.kill(pid, signal.SIGKILL)

    # This will fail immediately since the actor is in restarting
    # status.
    # But if we change to `max_task_retries=1`, it'll wait
    # until the actor is created.
    ray.get(a.pid.remote())


The owner of the actor dies
---------------------------

The :ref:`owner <fault-tolerance_ownership>` is the worker which creates the
actor, except for the detached actors whose owner is the GCS. Similar to
objects, if the owner died, the actor will be dead, even if the actor is alive
when the owner died. Ray can't recover an actor whose owner is dead.

.. code-block:: python

    import ray
    import os
    import signal
    ray.init()

    @ray.remote(max_restarts=-1)
    class Actor:
        def pid(self):
            return "hello"

    @ray.remote
    def gen_actors():
        actor = Actor.remote()
        detached_actor = Actor.options(name="actor", lifetime="detached").remote()


        return actor, detached_actor, os.getpid()

    actor, detached_actor, pid = ray.get(gen_actors.remote())

    os.kill(pid, signal.SIGKILL)

    try:
        print("actor.pid:", ray.get(actor.pid.remote()))
    except ray.exceptions.RayActorError as e:
        print("failed to submit actor call", e)

    try:
        print("detached_actor.pid:", ray.get(detached_actor.pid.remote()))
    except ray.exceptions.RayActorError as e:
        print("failed to submit detached actor call", e)


In the above example, we can see, if the owner died, even if the actor itself is
still alive, it'll fail.