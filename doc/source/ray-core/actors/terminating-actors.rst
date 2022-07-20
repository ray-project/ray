Terminating Actors
==================

Automatic termination
^^^^^^^^^^^^^^^^^^^^^

.. tabbed:: Python

    Actor processes will be terminated automatically when all copies of the
    actor handle have gone out of scope in Python, or if the original creator
    process dies.

.. tabbed:: Java

    Terminating an actor automatically when the initial actor handle goes out of scope hasn't been implemented in Java yet.

.. tabbed:: C++

    Terminating an actor automatically when the initial actor handle goes out of scope hasn't been implemented in C++ yet.

Manual termination within the actor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If necessary, you can manually terminate an actor from within one of the actor methods.
This will kill the actor process and release resources associated/assigned to the actor.

.. tabbed:: Python

    .. code-block:: python

        ray.actor.exit_actor()

    This approach should generally not be necessary as actors are automatically garbage
    collected. The ``ObjectRef`` resulting from the task can be waited on to wait
    for the actor to exit (calling ``ray.get()`` on it will raise a ``RayActorError``).

.. tabbed:: Java

    .. code-block:: java

        Ray.exitActor();

    Garbage collection for actors haven't been implemented yet, so this is currently the
    only way to terminate an actor gracefully. The ``ObjectRef`` resulting from the task
    can be waited on to wait for the actor to exit (calling ``ObjectRef::get`` on it will
    throw a ``RayActorException``).

.. tabbed:: C++

    .. code-block:: c++

        ray::ExitActor();

    Garbage collection for actors haven't been implemented yet, so this is currently the
    only way to terminate an actor gracefully. The ``ObjectRef`` resulting from the task
    can be waited on to wait for the actor to exit (calling ``ObjectRef::Get`` on it will
    throw a ``RayActorException``).

Note that this method of termination will wait until any previously submitted
tasks finish executing and then exit the process gracefully with sys.exit.

Manual termination via an actor handle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can terminate an actor forcefully.

.. tabbed:: Python

    .. code-block:: python

        ray.kill(actor_handle)

.. tabbed:: Java

    .. code-block:: java

        actorHandle.kill();

.. tabbed:: C++

    .. code-block:: c++

        actor_handle.Kill();

This will call the exit syscall from within the actor, causing it to exit
immediately and any pending tasks to fail.

.. tabbed:: Python

    This will not go through the normal
    Python sys.exit teardown logic, so any exit handlers installed in the actor using
    ``atexit`` will not be called.

.. tabbed:: Java

    This will not go through the normal Java System.exit teardown logic, so any
    shutdown hooks installed in the actor using ``Runtime.addShutdownHook(...)`` will
    not be called.

.. tabbed:: C++

    This will not go through the normal
    C++ std::exit teardown logic, so any exit handlers installed in the actor using
    ``std::atexit`` will not be called.
