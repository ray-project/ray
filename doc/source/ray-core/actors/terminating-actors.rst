Terminating Actors
==================

Actor processes will be terminated automatically when all copies of the
actor handle have gone out of scope in Python, or if the original creator
process dies.

Note that automatic termination of actors is not yet supported in Java or C++.

.. _ray-kill-actors:

Manual termination via an actor handle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In most cases, Ray will automatically terminate actors that have gone out of
scope, but you may sometimes need to terminate an actor forcefully. This should
be reserved for cases where an actor is unexpectedly hanging or leaking
resources, and for :ref:`detached actors <actor-lifetimes>`, which must be
manually destroyed.

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            ray.kill(actor_handle)
            # This will not go through the normal Python sys.exit
            # teardown logic, so any exit handlers installed in
            # the actor using ``atexit`` will not be called.


    .. tab-item:: Java

        .. code-block:: java

            actorHandle.kill();
            // This will not go through the normal Java System.exit teardown logic, so any
            // shutdown hooks installed in the actor using ``Runtime.addShutdownHook(...)`` will
            // not be called.

    .. tab-item:: C++

        .. code-block:: c++

            actor_handle.Kill();
            // This will not go through the normal C++ std::exit
            // teardown logic, so any exit handlers installed in
            // the actor using ``std::atexit`` will not be called.


This will cause the actor to immediately exit its process, causing any current,
pending, and future tasks to fail with a ``RayActorError``. If you would like
Ray to :ref:`automatically restart <fault-tolerance-actors>` the actor, make sure to set a nonzero
``max_restarts`` in the ``@ray.remote`` options for the actor, then pass the
flag ``no_restart=False`` to ``ray.kill``.

For :ref:`named and detached actors <actor-lifetimes>`, calling ``ray.kill`` on
an actor handle will destroy the actor and allow the name to be reused.


Manual termination within the actor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If necessary, you can manually terminate an actor from within one of the actor methods.
This will kill the actor process and release resources associated/assigned to the actor.

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            ray.actor.exit_actor()

        This approach should generally not be necessary as actors are automatically garbage
        collected. The ``ObjectRef`` resulting from the task can be waited on to wait
        for the actor to exit (calling ``ray.get()`` on it will raise a ``RayActorError``).

    .. tab-item:: Java

        .. code-block:: java

            Ray.exitActor();

        Garbage collection for actors haven't been implemented yet, so this is currently the
        only way to terminate an actor gracefully. The ``ObjectRef`` resulting from the task
        can be waited on to wait for the actor to exit (calling ``ObjectRef::get`` on it will
        throw a ``RayActorException``).

    .. tab-item:: C++

        .. code-block:: c++

            ray::ExitActor();

        Garbage collection for actors haven't been implemented yet, so this is currently the
        only way to terminate an actor gracefully. The ``ObjectRef`` resulting from the task
        can be waited on to wait for the actor to exit (calling ``ObjectRef::Get`` on it will
        throw a ``RayActorException``).

Note that this method of termination will wait until any previously submitted
tasks finish executing and then exit the process gracefully with sys.exit.

