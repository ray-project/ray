.. _actor-lifecycle:

Actor Lifecycle
===============

This document explains the internal lifecycle of actors in Ray Core, including how actors are defined, created, scheduled, executed, restarted, and destroyed.

Unlike tasks which are stateless and short-lived, actors are stateful and long-lived. An actor is essentially a dedicated worker process that maintains state between method calls.

.. testcode::

  import ray

  @ray.remote
  class Counter:
      def __init__(self):
          self.value = 0

      def increment(self):
          self.value += 1
          return self.value

  counter = Counter.remote()
  print(ray.get(counter.increment.remote()))
  print(ray.get(counter.increment.remote()))

.. testoutput::

  1
  2


Actor types
-----------

Actors can be classified along two dimensions: **lifetime semantics** and **concurrency model**.

Lifetime semantics
^^^^^^^^^^^^^^^^^^

1. **Regular (non-detached) actors**: Lifetime is tied to their owner (the creator process). When the owner dies or all references go out of scope, the actor is terminated.

2. **Detached actors**: Lifetime is independent of their creator. They persist until explicitly killed with ``ray.kill()`` or the cluster shuts down. Created with ``lifetime="detached"``.

3. **Named actors**: Actors registered with a name in a namespace. Can be retrieved using ``ray.get_actor(name)``. Can be either detached or non-detached.

Concurrency model
^^^^^^^^^^^^^^^^^

1. **Sync actors** (default): Execute one task at a time. Methods are regular Python functions. This is the default when ``max_concurrency=1``.

2. **Threaded actors**: Execute multiple tasks concurrently using a thread pool. Created with ``max_concurrency > 1`` and synchronous methods. Each task runs in its own thread.

3. **Async actors**: Execute multiple tasks concurrently using Python's asyncio. Created when the actor class has ``async def`` methods. Tasks are scheduled on an event loop and yield during ``await`` expressions.

The concurrency model affects how tasks are executed within the actor worker but doesn't change the lifecycle management (creation, restart, termination) described in this document.

Actor states
------------

An actor transitions through the following states during its lifecycle. The state is defined in `gcs.proto <https://github.com/ray-project/ray/blob/master/src/ray/protobuf/gcs.proto#L79>`__ and managed by `GcsActorManager <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.h#L50>`__.

.. code-block:: none

                                                          (3)
   (0)                    (1)                  (2)        ───►
  ────► DEPENDENCIES_UNREADY ──► PENDING_CREATION ──► ALIVE      RESTARTING
              │                        │               │    ◄───      ▲
            (8)                      (7)             (6)     (4)      │ (9)
              │                        ▼               │              │
              └─────────────────────► DEAD ◄──────────┴──────────────┘
                                             (5)

**Transitions:**

0. `RegisterActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L333>`__: Actor is registered, initial state is set in `GcsActor constructor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor.h#L112>`__.

1. `CreateActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L884>`__: Dependencies resolved, actor is scheduled.

2. `OnActorCreationSuccess <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1781>`__: Actor successfully created on a worker.

3. `RestartActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1602>`__: Worker died, actor has remaining restarts.

4. `OnActorCreationSuccess <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1781>`__: Actor successfully restarted.

5. `RestartActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1622>`__: Actor in RESTARTING exhausted restarts or can't restart.

6. `DestroyActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1137>`__ / `OnWorkerDead <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1331>`__: Actor terminated (out of scope, ray.kill, etc.).

7. Owner died while actor was in PENDING_CREATION.

8. Creator died while actor had unresolved dependencies.

9. `HandleRestartActorForLineageReconstruction <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L360>`__: Dead actor restarted for lineage reconstruction.

**States:**

1. **DEPENDENCIES_UNREADY**: Actor is registered in GCS but its constructor arguments (dependencies) aren't ready yet.

2. **PENDING_CREATION**: Dependencies are resolved and the actor is being scheduled/created. GCS is finding a node and worker to run the actor.

3. **ALIVE**: Actor is running and can accept method calls. The actor has a dedicated worker process.

4. **RESTARTING**: Actor worker died but the actor is configured for restarts (``max_restarts > 0``). GCS is scheduling a new worker.

5. **DEAD**: Actor is terminated. This can be permanent or temporary (if lineage reconstruction is possible). Causes include:

   - All references go out of scope (``OUT_OF_SCOPE``) - may be reconstructed later
   - All references including lineage refs are deleted (``REF_DELETED``) - permanent
   - ``ray.kill()`` is called (``RAY_KILL``)
   - Actor exhausted its restart budget
   - Actor's owner died (for non-detached actors)
   - Actor's node died and it can't be restarted


Defining an actor class
-----------------------

The first step is defining an actor class using the :func:`ray.remote` decorator. This wraps the Python class and returns an `ActorClass <https://github.com/ray-project/ray/blob/master/python/ray/actor.py#L1188>`__ instance.

``ActorClass`` stores:

- The underlying class and its methods
- Default options like ``num_cpus``, ``max_restarts``, ``max_task_retries``
- Metadata for serialization and remote execution

.. testcode::

  @ray.remote(num_cpus=1, max_restarts=3)
  class MyActor:
      def __init__(self, value):
          self.value = value

      def get_value(self):
          return self.value


Creating an actor
-----------------

When you call ``.remote()`` on an actor class, Ray creates an actor instance. This involves multiple components:

1. **Python driver/worker**: Prepares the actor creation request
2. **Core Worker**: Builds the task specification and communicates with GCS
3. **GCS (Global Control Store)**: Manages actor metadata and coordinates scheduling
4. **GCS Actor Scheduler**: Finds a node and leases a worker for the actor
5. **Raylet**: Provides workers and manages local resources
6. **Actor Worker**: The dedicated worker process that runs the actor

The creation flow is:

1. `ActorClass._remote() <https://github.com/ray-project/ray/blob/master/python/ray/actor.py#L1499>`__ is called, which performs validation and prepares options.

2. The Cython layer `create_actor <https://github.com/ray-project/ray/blob/master/python/ray/_raylet.pyx#L3458>`__ prepares arguments and calls the C++ ``CoreWorker``.

3. `CoreWorker::CreateActor <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/core_worker.cc#L2048>`__ does the following:

   a. Generates a unique ``ActorID`` derived from the job ID, task ID, and task index.
   b. Builds an ``ActorCreationTaskSpec`` containing the serialized actor handle, scheduling strategy, resource requirements, and options.
   c. Creates an ``ActorHandle`` and adds it to the local ``ActorManager``.
   d. For named actors, synchronously registers with GCS to check name availability.
   e. For unnamed actors, asynchronously registers with GCS.

4. After registration, `ActorTaskSubmitter::SubmitActorCreationTask <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L92>`__ submits the creation task:

   a. Resolves any dependencies in the constructor arguments.
   b. Calls ``AsyncCreateActor`` to send the creation request to GCS.

5. GCS `HandleRegisterActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L333>`__ registers the actor:

   a. Validates the request and checks for name conflicts.
   b. Creates a ``GcsActor`` object and stores it in ``registered_actors_``.
   c. For named actors, registers the name in ``named_actors_``.
   d. Sets actor state to ``DEPENDENCIES_UNREADY``.
   e. Persists actor data to the backend storage.

6. GCS `HandleCreateActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L451>`__ initiates creation:

   a. Updates state to ``PENDING_CREATION``.
   b. Calls `GcsActorScheduler::Schedule <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L55>`__ to find a worker.


Scheduling an actor
-------------------

GCS Actor Scheduler is responsible for finding a node and worker to run the actor:

1. `GcsActorScheduler::Schedule <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L55>`__ chooses between GCS-based or Raylet-based scheduling:

   - **GCS-based scheduling** (when ``gcs_actor_scheduling_enabled`` is true): GCS directly selects a node based on resource availability using the cluster lease manager.
   - **Raylet-based scheduling**: GCS forwards the scheduling decision to a raylet (typically the owner's node).

2. For Raylet-based scheduling, `ScheduleByRaylet <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L114>`__:

   a. Selects an initial node (usually the owner's node or one with data locality).
   b. Sends a ``RequestWorkerLease`` RPC to that node's raylet.

3. `LeaseWorkerFromNode <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L293>`__ requests a worker:

   a. Creates a lease request with resource requirements.
   b. The raylet either grants a worker, rejects, or suggests a spillback node.

4. On successful lease, `HandleWorkerLeaseGrantedReply <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L355>`__:

   a. Records the resource mapping.
   b. Calls `CreateActorOnWorker <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L439>`__ to start the actor on the leased worker.

5. The worker receives a ``CreateActor`` RPC and executes the actor's ``__init__`` method.


Executing the actor creation task
---------------------------------

Once a worker is assigned, the actor creation task executes:

1. GCS sends a `CreateActor RPC <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L439>`__ to the worker.

2. The worker's `CoreWorker <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/core_worker.cc#L2903>`__ handles the request:

   a. Deserializes the actor handle and task specification.
   b. Fetches the pickled class definition from GCS key-value store.
   c. Unpickles and instantiates the actor class.
   d. Calls the ``__init__`` method with the provided arguments.

3. On success, `GcsActorManager::OnActorCreationSuccess <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1761>`__:

   a. Updates actor state to ``ALIVE``.
   b. Records the worker and node IDs.
   c. Publishes the actor info so subscribers can learn the actor's address.
   d. Invokes the creation callback to notify the creator.

4. On failure (e.g., ``__init__`` raises an exception), the actor may be:

   - Marked as ``DEAD`` if it's a permanent failure
   - Rescheduled if it's a transient failure and restarts are available


Submitting actor tasks
----------------------

Once an actor is ``ALIVE``, you can submit tasks to it:

.. testcode::

  result = counter.increment.remote()  # Submit actor task
  print(ray.get(result))  # Get result

.. testoutput::

  3

Actor task submission differs from regular task submission:

1. `ActorMethod._remote() <https://github.com/ray-project/ray/blob/master/python/ray/actor.py#L248>`__ prepares the method call.

2. `ActorTaskSubmitter::SubmitTask <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L166>`__:

   a. Assigns a sequence number to maintain task ordering (unless ``allow_out_of_order_execution`` is true).
   b. Resolves any ``ObjectRef`` dependencies.
   c. Sends the task directly to the actor's worker via ``PushTask`` RPC.

3. The actor worker receives tasks and executes them:

   a. For sync actors: Tasks execute sequentially in order.
   b. For async actors: Tasks can execute concurrently up to ``max_concurrency``.
   c. For threaded actors: Tasks can run in parallel threads up to ``max_concurrency``.

4. Return values are stored in the object store and can be retrieved with ``ray.get()``.


Actor failure and restart
-------------------------

When an actor worker dies, Ray can automatically restart it if ``max_restarts > 0``:

1. `GcsActorManager::OnWorkerDead <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1249>`__ is called when a worker exits:

   a. Determines the death cause (crash, intentional exit, node failure, etc.).
   b. Checks if the actor should be rescheduled based on ``disconnect_type``.

2. `RestartActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1516>`__ handles the restart decision:

   a. Calculates remaining restarts (node preemption restarts don't count).
   b. Checks if restart is blocked (e.g., explicit termination like ``OUT_OF_SCOPE`` or ``REF_DELETED``).
   c. If ``should_restart`` is true:

      - Updates state to ``RESTARTING``
      - Increments ``num_restarts``
      - Calls ``GcsActorScheduler::Schedule`` to find a new worker

   d. If not restartable:

      - Updates state to ``DEAD``
      - Cleans up from ``registered_actors_`` (unless kept for lineage reconstruction)
      - Removes actor name registration
      - Publishes the final state

3. `IsActorRestartable <https://github.com/ray-project/ray/blob/master/src/ray/common/protobuf_utils.cc#L164>`__ determines if a DEAD actor can be restarted for lineage reconstruction:

   - Must be ``OUT_OF_SCOPE`` death cause
   - Must have remaining restarts OR be preempted with ``max_restarts > 0``


Actor termination
-----------------

Actors can be terminated in several ways:

**Automatic termination (out of scope)**

When all ``ActorHandle`` references go out of scope:

1. The reference counting system detects no live references.
2. Core Worker notifies GCS via ``WaitForActorRefDeleted``.
3. GCS calls `DestroyActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1007>`__ with ``OUT_OF_SCOPE`` death cause.
4. For graceful shutdown, the worker receives a signal and can run cleanup (``__ray_shutdown__``).
5. After worker confirms exit, actor is marked ``DEAD``.

**Explicit termination (ray.kill)**

When ``ray.kill(actor)`` is called:

1. GCS `HandleKillActorViaGcs <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L662>`__ processes the request.
2. If ``no_restart=True``: Calls ``DestroyActor`` with ``RAY_KILL`` death cause.
3. If ``no_restart=False``: Calls ``KillActor`` which allows the actor to restart.

**Owner death**

For non-detached actors, when the owner process dies:

1. GCS detects owner death and iterates through owned actors.
2. Each owned actor is destroyed with ``OWNER_DIED`` death cause.
3. Owned actors cannot restart even if they have remaining restarts.

**Node death**

When a node dies:

1. `GcsActorManager::OnNodeDead <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1365>`__ is called.
2. All actors on that node are processed:

   - Owned actors whose owners were on the node are destroyed
   - Other actors are restarted if they have remaining restarts


Named actor registration
------------------------

Named actors have special registration handling:

1. Names are stored in ``named_actors_`` map keyed by ``(namespace, name)``.

2. `GetActorIDByName <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L289>`__ looks up actors by name.

3. Name conflicts are checked during registration:

   - ``RegisterActor`` rejects duplicate names in the same namespace.
   - Names are released when the actor is marked ``DEAD`` (for intentional terminations).

4. For failures, names are preserved while references exist to prevent premature reuse.


Lineage reconstruction
----------------------

Actors with ``max_restarts > 0`` can be reconstructed when their outputs are needed:

1. When an ``ObjectRef`` from a dead actor is accessed, the object is re-computed.

2. `HandleRestartActorForLineageReconstruction <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L360>`__ handles restart requests:

   a. Verifies the actor is in ``registered_actors_`` (not fully cleaned up).
   b. Checks ``IsActorRestartable`` returns true.
   c. Calls ``RestartActor`` with ``need_reschedule=true`` to restart the actor.

3. Actors are kept in ``registered_actors_`` for lineage reconstruction if:

   - Death cause is ``OUT_OF_SCOPE`` (not ``REF_DELETED``)
   - Has remaining restarts OR was preempted with ``max_restarts > 0``


Detached actor semantics
------------------------

Detached actors have special lifecycle handling:

1. **No owner tracking**: Detached actors aren't added to the owner's children list.

2. **Persist across driver exit**: The actor continues running when its creator exits.

3. **Must be explicitly killed**: Use ``ray.kill(actor)`` to terminate.

4. **Root detached actor ID**: Non-detached actors created by a detached actor inherit the root detached actor ID for lifecycle tracking.

5. **Name required for retrieval**: Since detached actors outlive their creator, you typically give them names to retrieve later:

.. testcode::
  :skipif: True

  # In driver 1
  @ray.remote
  class DetachedActor:
      pass

  actor = DetachedActor.options(name="my_actor", lifetime="detached").remote()

  # In driver 2 (later)
  actor = ray.get_actor("my_actor")
  ray.kill(actor)  # Must explicitly kill


Actor data persistence
----------------------

Actor metadata is persisted to survive GCS restarts:

1. **ActorTable**: Stores ``ActorTableData`` including state, address, death cause.

2. **ActorTaskSpecTable**: Stores ``TaskSpec`` for actor creation, needed to reconstruct actors.

3. On GCS startup, `GcsActorManager::Initialize <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L256>`__ loads persisted actors:

   - ``ALIVE`` actors are tracked and their workers are expected to reconnect.
   - ``DEAD`` actors that are restartable are kept for lineage reconstruction.
   - Detached actors in non-DEAD states are restored.


Summary diagram
---------------

.. code-block:: none

  ┌──────────────┐
  │   Driver     │
  │              │
  │ Actor.remote │────┐
  └──────────────┘    │
                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                           Core Worker                                  │
  │                                                                       │
  │  1. Generate ActorID                                                  │
  │  2. Build TaskSpec                                                    │
  │  3. Register with GCS (sync for named, async for unnamed)             │
  │  4. Submit creation task                                              │
  └───────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                              GCS                                       │
  │                                                                       │
  │  ┌─────────────────────┐    ┌────────────────────────────────────┐   │
  │  │   GcsActorManager   │    │      GcsActorScheduler              │   │
  │  │                     │    │                                     │   │
  │  │ • Register actor    │───►│ • Select node (GCS or Raylet)      │   │
  │  │ • Track states      │    │ • Lease worker                      │   │
  │  │ • Handle failures   │◄───│ • Handle spillback                  │   │
  │  │ • Manage restarts   │    │ • Create actor on worker           │   │
  │  └─────────────────────┘    └────────────────────────────────────┘   │
  └───────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                        Raylet (Node Manager)                           │
  │                                                                       │
  │  • Handle worker lease requests                                       │
  │  • Manage local resources                                             │
  │  • Start/stop workers                                                 │
  │  • Report worker deaths to GCS                                        │
  └───────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                         Actor Worker                                   │
  │                                                                       │
  │  • Execute __init__                                                   │
  │  • Receive and execute actor tasks                                    │
  │  • Maintain actor state                                               │
  │  • Handle graceful shutdown                                           │
  └───────────────────────────────────────────────────────────────────────┘


See also
--------

- :ref:`actors-guide`: User guide for working with actors
- :ref:`actor-lifetimes`: Named and detached actors
- :ref:`fault-tolerance-actors`: Actor fault tolerance and restarts
- :ref:`task-lifecycle`: Task lifecycle internals
