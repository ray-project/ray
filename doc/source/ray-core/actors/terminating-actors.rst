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

        .. testcode::

            import ray

            @ray.remote
            class Actor:
                pass

            actor_handle = Actor.remote()

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
an actor handle destroys the actor and allow the name to be reused.

Use `ray list actors --detail` from :ref:`State API <state-api-overview-ref>` to see the death cause of dead actors:

.. code-block:: bash

  # This API is only available when you download Ray via `pip install "ray[default]"`
  ray list actors --detail

.. code-block:: bash

  ---
  -   actor_id: e8702085880657b355bf7ef001000000
      class_name: Actor
      state: DEAD
      job_id: '01000000'
      name: ''
      node_id: null
      pid: 0
      ray_namespace: dbab546b-7ce5-4cbb-96f1-d0f64588ae60
      serialized_runtime_env: '{}'
      required_resources: {}
      death_cause:
          actor_died_error_context: # <---- You could see the error message w.r.t why the actor exits. 
              error_message: The actor is dead because `ray.kill` killed it.
              owner_id: 01000000ffffffffffffffffffffffffffffffffffffffffffffffff
              owner_ip_address: 127.0.0.1
              ray_namespace: dbab546b-7ce5-4cbb-96f1-d0f64588ae60
              class_name: Actor
              actor_id: e8702085880657b355bf7ef001000000
              never_started: true
              node_ip_address: ''
              pid: 0
              name: ''
      is_detached: false
      placement_group_id: null
      repr_name: ''


Manual termination within the actor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If necessary, you can manually terminate an actor from within one of the actor methods.
This will kill the actor process and release resources associated/assigned to the actor.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            @ray.remote
            class Actor:
                def exit(self):
                    ray.actor.exit_actor()

            actor = Actor.remote()
            actor.exit.remote()

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

Note that this method of termination waits until any previously submitted
tasks finish executing and then exits the process gracefully with sys.exit.


    
You could see the actor is dead as a result of the user's `exit_actor()` call:

.. code-block:: bash

  # This API is only available when you download Ray via `pip install "ray[default]"`
  ray list actors --detail

.. code-block:: bash

  ---
  -   actor_id: 070eb5f0c9194b851bb1cf1602000000
      class_name: Actor
      state: DEAD
      job_id: '02000000'
      name: ''
      node_id: 47ccba54e3ea71bac244c015d680e202f187fbbd2f60066174a11ced
      pid: 47978
      ray_namespace: 18898403-dda0-485a-9c11-e9f94dffcbed
      serialized_runtime_env: '{}'
      required_resources: {}
      death_cause:
          actor_died_error_context:
              error_message: 'The actor is dead because its worker process has died.
                  Worker exit type: INTENDED_USER_EXIT Worker exit detail: Worker exits
                  by an user request. exit_actor() is called.'
              owner_id: 02000000ffffffffffffffffffffffffffffffffffffffffffffffff
              owner_ip_address: 127.0.0.1
              node_ip_address: 127.0.0.1
              pid: 47978
              ray_namespace: 18898403-dda0-485a-9c11-e9f94dffcbed
              class_name: Actor
              actor_id: 070eb5f0c9194b851bb1cf1602000000
              name: ''
              never_started: false
      is_detached: false
      placement_group_id: null
      repr_name: ''