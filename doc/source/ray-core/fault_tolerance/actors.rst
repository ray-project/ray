.. _fault-tolerance-actors:

Actor fault tolerance
=====================

Actors can fail if the actor process dies, or if the **owner** of the actor
dies. The owner of an actor is the worker that originally created the actor by
calling ``ActorClass.remote()``. :ref:`*Detached* actors <actor-lifetimes>` do
not have an owner process and are cleaned up when the Ray cluster is destroyed.


Actor process failure
---------------------

Ray can automatically restart actors that crash unexpectedly.
This behavior is controlled using ``max_restarts``,
which sets the maximum number of times that an actor will be restarted.
The default value of ``max_restarts`` is 0, meaning that the actor won't be
restarted. If set to -1, the actor will be restarted infinitely many times.
When an actor is restarted, its state will be recreated by rerunning its
constructor.
After the specified number of restarts, subsequent actor methods will
raise a ``RayActorError``.

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
automatically retry the task. With this option, the system will only throw a
``RayActorError`` to the application if one of the following occurs: (1) the
actor’s ``max_restarts`` limit has been exceeded and the actor cannot be
restarted anymore, or (2) the ``max_task_retries`` limit has been exceeded for
this particular task. Note that if the actor is currently restarting when a
task is submitted, this will count for one retry. The retry limit can be set to
infinity with ``max_task_retries = -1``.

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
(unless ``max_task_retries`` is nonzero and the task is needed for :ref:`object
reconstruction <object-reconstruction>`). 

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


Actor creator failure
---------------------

For :ref:`non-detached actors <actor-lifetimes>`, the owner of an actor is the
worker that created it, e.g., by calling ``ActorClass.remote()``. Similar to
:ref:`objects <fault-tolerance-objects>`, if the owner of an actor dies, then
the actor will also fate-share with the owner.  Ray will not automatically
recover an actor whose owner is dead, even if it has a nonzero
``max_restarts``.

Since detached actors do not have an owner, they will still be restarted by Ray
even if their original creator dies. Detached actors will continue to be
automatically restarted until the maximum restarts is exceeded, the actor is
destroyed, or until the Ray cluster is destroyed.

You can try out this behavior in the following code.

.. code-block:: python

    import ray
    import os
    import signal
    ray.init()

    @ray.remote(max_restarts=-1)
    class Actor:
        def ping(self):
            return "hello"

    @ray.remote
    class Parent:
        def generate_actors(self):
            self.child = Actor.remote()
            self.detached_actor = Actor.options(name="actor", lifetime="detached").remote()
            return self.child, self.detached_actor, os.getpid()

    parent = Parent.remote()
    actor, detached_actor, pid = ray.get(parent.generate_actors.remote())

    os.kill(pid, signal.SIGKILL)

    try:
        print("actor.ping:", ray.get(actor.ping.remote()))
    except ray.exceptions.RayActorError as e:
        print("Failed to submit actor call", e)
    # Failed to submit actor call The actor died unexpectedly before finishing this task.
    # 	class_name: Actor
    # 	actor_id: 56f541b178ff78470f79c3b601000000
    # 	namespace: ea8b3596-7426-4aa8-98cc-9f77161c4d5f
    # The actor is dead because because all references to the actor were removed.

    try:
        print("detached_actor.ping:", ray.get(detached_actor.ping.remote()))
    except ray.exceptions.RayActorError as e:
        print("Failed to submit detached actor call", e)
    # detached_actor.ping: hello

Force-killing a misbehaving actor
---------------------------------

Sometimes application-level code can cause an actor to hang or leak resources.
In these cases, Ray allows you to recover from the failure by :ref:`manually
terminating <ray-kill-actors>` the actor. You can do this by calling
``ray.kill`` on any handle to the actor. Note that it does not need to be the
original handle to the actor.

If ``max_restarts`` is set, you can also allow Ray to automatically restart the actor by passing ``no_restart=False`` to ``ray.kill``.
