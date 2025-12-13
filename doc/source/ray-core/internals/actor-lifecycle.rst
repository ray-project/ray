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

   - **GCS-based scheduling** (when ``gcs_actor_scheduling_enabled`` is true): GCS directly selects a node based on resource availability using the cluster lease manager, then calls ``LeaseWorkerFromNode``.
   - **Raylet-based scheduling** (default): GCS forwards the scheduling decision to a raylet.

2. For Raylet-based scheduling, `ScheduleByRaylet <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L114>`__:

   a. Calls `SelectForwardingNode <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L142>`__ to pick an initial node (owner's node if it has resources, otherwise random).
   b. Calls ``LeaseWorkerFromNode`` to request a worker from that node.

3. `LeaseWorkerFromNode <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L293>`__ sends a ``RequestWorkerLease`` RPC to the target raylet with the actor's resource requirements.

4. `HandleWorkerLeaseReply <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L578>`__ processes the raylet's response:

   - If **rejected** (insufficient resources): Calls `HandleWorkerLeaseRejectedReply <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L660>`__ which reschedules the actor.
   - If **spillback** (worker_address empty but retry_at_raylet_address set): `HandleWorkerLeaseGrantedReply <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L355>`__ retries on the suggested node.
   - If **granted** (worker_address set): Proceeds to create the actor on the leased worker.

5. On successful lease, `HandleWorkerLeaseGrantedReply <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L355>`__:

   a. Records the resource mapping on the actor.
   b. Creates a ``GcsLeasedWorker`` and calls `CreateActorOnWorker <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L441>`__.

6. `CreateActorOnWorker <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L441>`__ sends a ``PushNormalTask`` RPC to the worker with the actor creation task. The worker executes the actor's ``__init__`` method.


Executing the actor creation task
---------------------------------

Once a worker is assigned, the actor creation task executes:

1. GCS `CreateActorOnWorker <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L441>`__ sends a ``PushNormalTask`` RPC to the worker with the actor creation task spec.

2. The worker's `CoreWorker::HandlePushTask <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/core_worker.cc#L3391>`__ receives the request:

   a. For actor creation tasks, `sets the actor context <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/core_worker.cc#L3402>`__ (actor ID, job info).
   b. Queues the task for execution via ``task_receiver_->HandleTask``.

3. The `task_execution_handler <https://github.com/ray-project/ray/blob/master/python/ray/_raylet.pyx#L2131>`__ (Python/Cython) executes the task:

   a. Fetches the pickled class definition from GCS key-value store.
   b. Unpickles and instantiates the actor class.
   c. Calls the ``__init__`` method with the provided arguments.

4. On success, the ``PushNormalTask`` RPC reply triggers `schedule_success_handler <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_scheduler.cc#L489>`__ which calls `GcsActorManager::OnActorCreationSuccess <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1748>`__:

   a. `Updates actor state to ALIVE <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1781>`__.
   b. Records the worker and node IDs in ``created_actors_``.
   c. Persists to storage and publishes actor info so subscribers learn the actor's address.
   d. Invokes creation callbacks to notify the creator.

5. On failure (e.g., ``__init__`` raises an exception):

   - The worker returns a ``CreationTaskError`` status.
   - GCS marks the actor as ``DEAD`` (creation task failures are permanent and don't trigger restarts).


Submitting actor tasks
----------------------

Once an actor is ``ALIVE``, you can submit tasks to it:

.. testcode::

  result = counter.increment.remote()  # Submit actor task
  print(ray.get(result))  # Get result

.. testoutput::

  3

Actor task submission differs from regular task submission:

1. `ActorMethod._remote() <https://github.com/ray-project/ray/blob/master/python/ray/actor.py#L792>`__ prepares the method call, then calls `_actor_method_call <https://github.com/ray-project/ray/blob/master/python/ray/actor.py#L858>`__.

2. `ActorTaskSubmitter::SubmitTask <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L167>`__:

   a. Assigns a `sequence number <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L190>`__ to maintain task ordering.
   b. Queues the task in ``actor_submit_queue_``.
   c. Posts a job to resolve ``ObjectRef`` dependencies.

3. `SendPendingTasks <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L529>`__ pops ready tasks from the queue and calls `PushActorTask <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L576>`__, which sends them directly to the actor's worker via `PushActorTask RPC <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L633>`__.

4. The actor worker receives tasks and executes them:

   a. For sync actors: Tasks execute sequentially in order.
   b. For async actors: Tasks can execute concurrently up to ``max_concurrency``.
   c. For threaded actors: Tasks can run in parallel threads up to ``max_concurrency``.

5. Return values are stored in the object store and can be retrieved with ``ray.get()``.


Actor failure and restart
-------------------------

When an actor worker dies, Ray can automatically restart it if ``max_restarts > 0``:

1. `GcsActorManager::OnWorkerDead <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1251>`__ is called when a worker exits:

   a. Determines `need_reconstruct <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1274>`__ based on ``disconnect_type`` (false for ``INTENDED_USER_EXIT`` or ``USER_ERROR``).
   b. Destroys all actors owned by the dead worker.
   c. `Finds the actor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1311>`__ in ``created_actors_`` or via ``CancelOnWorker``.
   d. Marks actor state to ``DEAD`` immediately (transition 6 in state machine).
   e. Determines death cause and calls ``RestartActor``.

2. `RestartActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1519>`__ handles the restart decision:

   a. `Calculates remaining_restarts <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1559>`__ (node preemption restarts don't count toward max).
   b. Checks if restart is blocked by explicit termination (``OUT_OF_SCOPE``, ``REF_DELETED``) or creation failure.
   c. If `should_restart <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1591>`__ is true:

      - `Updates state to RESTARTING <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1602>`__ (transition 3)
      - Increments ``num_restarts``
      - Calls ``GcsActorScheduler::Schedule`` to find a new worker

   d. If not restartable (`else branch at line 1621 <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1621>`__):

      - `Updates state to DEAD <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1622>`__ (transition 5)
      - If not kept for lineage: removes from ``registered_actors_``, removes actor name
      - Publishes the final state

3. `IsActorRestartable <https://github.com/ray-project/ray/blob/master/src/ray/common/protobuf_utils.cc#L164>`__ determines if a ``DEAD`` actor can be restarted for lineage reconstruction:

   - Must have ``OUT_OF_SCOPE`` death cause (not ``REF_DELETED`` or creation failure)
   - Must have remaining restarts OR be preempted with ``max_restarts > 0``


Actor termination
-----------------

Actors can be terminated in several ways:

**Automatic termination (out of scope)**

When all ``ActorHandle`` references go out of scope:

1. The reference counting system detects no live references.
2. Core Worker sends `ReportActorOutOfScope <https://github.com/ray-project/ray/blob/master/src/ray/gcs/grpc_services.cc#L40>`__ RPC to GCS.
3. `HandleReportActorOutOfScope <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L301>`__ calls `DestroyActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1009>`__ with ``OUT_OF_SCOPE`` death cause.
4. For graceful shutdown, the worker receives a signal and can run cleanup (``__del__``).
5. After worker confirms exit, actor is marked ``DEAD``.

Alternatively, GCS uses ``WaitForActorRefDeleted`` to monitor when references are deleted from the owner:

1. When actor is registered, GCS `calls WaitForActorRefDeleted <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L981>`__ on the owner worker.
2. When owner deletes refs, the callback triggers `DestroyActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1000>`__ with ``REF_DELETED`` death cause.

**Explicit termination (ray.kill)**

When ``ray.kill(actor)`` is called:

1. GCS `HandleKillActorViaGcs <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L660>`__ processes the request.
2. If `no_restart=True <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L668>`__: Calls ``DestroyActor`` with ``RAY_KILL`` death cause (permanent).
3. If `no_restart=False <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L671>`__: Calls `KillActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1991>`__ which terminates the worker but allows restart.

**Owner death**

For non-detached actors, when the owner process dies:

1. GCS detects owner death via ``OnWorkerDead`` and iterates through `owned actors <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1277>`__.
2. Each owned actor is destroyed with ``OWNER_DIED`` death cause.
3. Owned actors don't automatically restart (owner death is an explicit termination).

**Node death**

When a node dies:

1. `GcsActorManager::OnNodeDead <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1362>`__ is called.
2. `Owned actors <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1367>`__ whose owners were on the node are destroyed with ``OWNER_DIED``.
3. `Actors being scheduled <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1401>`__ on the dead node are rescheduled.
4. Created actors on the dead node are handled via ``OnWorkerDead`` callbacks.


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

**Client-side trigger:**

1. When a task is `submitted to a DEAD actor <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L181>`__ that is owned and restartable:

   a. `ActorTaskSubmitter::RestartActorForLineageReconstruction <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L347>`__ is called.
   b. Sets local state to ``RESTARTING``.
   c. Sends ``RestartActorForLineageReconstruction`` RPC to GCS.

2. Also triggered when actor `becomes DEAD with pending tasks <https://github.com/ray-project/ray/blob/master/src/ray/core_worker/task_submission/actor_task_submitter.cc#L417>`__.

**GCS-side handling:**

3. `HandleRestartActorForLineageReconstruction <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L360>`__ handles restart requests:

   a. `Verifies actor exists <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L367>`__ in ``registered_actors_`` (not fully cleaned up).
   b. `Checks IsActorRestartable <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L414>`__ returns true.
   c. `Calls RestartActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L420>`__ with ``need_reschedule=true`` to restart the actor (transition 9 in state machine).

**Restartability conditions:**

4. `IsActorRestartable <https://github.com/ray-project/ray/blob/master/src/ray/common/protobuf_utils.cc#L164>`__ returns true only if:

   - Death cause is ``OUT_OF_SCOPE`` (not ``REF_DELETED``, not creation failure)
   - AND one of:

     - ``max_restarts == -1`` (infinite restarts)
     - OR ``max_restarts > 0 && preempted``
     - OR has remaining restarts (accounting for preemption restarts)

5. Actors are kept in ``registered_actors_`` for lineage reconstruction based on the same conditions.


Detached actor semantics
------------------------

Detached actors have special lifecycle handling:

1. **No owner tracking**: `PollOwnerForActorRefDeleted <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L963>`__ is `only called for non-detached actors <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L762>`__. Detached actors skip owner-based lifecycle tracking.

2. **Persist across driver exit**: Since detached actors don't have owner tracking, they survive when the creator driver exits. The actor continues running independently.

3. **Explicit termination required**: Detached actors `must be explicitly destroyed <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1678>`__ via ``ray.kill(actor)``. They don't automatically terminate when references go out of scope.

4. **Root detached actor ID**: The `ShouldLoadActor <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L183>`__ function shows the lifecycle hierarchy:

   - Non-detached actors inherit the root detached actor ID from their creator
   - If root is nil: actor dies with its job
   - If root is itself: actor lives independently (true detached actor)
   - Otherwise: actor dies when its root detached actor dies

5. **Runtime env tracking**: For detached actors, GCS `registers the runtime env <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L768>`__ for garbage collection instead of owner tracking.

6. **Name recommended for retrieval**: Since detached actors outlive their creator, naming them enables retrieval from other processes:

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

**Storage tables:**

1. **ActorTable**: Stores ``ActorTableData`` including state, address, death cause. `Written on state changes <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1179>`__.

2. **ActorTaskSpecTable**: Stores ``TaskSpec`` for actor creation, `written during registration <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L773>`__. Needed to reconstruct actors after GCS restart. `Deleted <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1692>`__ when actor is permanently dead and not restartable.

**GCS restart recovery:**

3. On GCS startup, `GcsActorManager::Initialize <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1827>`__ loads persisted actors using `OnInitializeActorShouldLoad <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L167>`__:

   - `ALIVE actors <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1856>`__: Added to ``created_actors_``, workers expected to reconnect.
   - `DEPENDENCIES_UNREADY actors <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1849>`__: Added to ``unresolved_actors_``.
   - `PENDING_CREATION/RESTARTING actors <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1896>`__: Rescheduled via ``gcs_actor_scheduler_->Reschedule``.
   - DEAD but restartable actors: Kept in ``registered_actors_`` for lineage reconstruction.
   - Non-detached actors: `PollOwnerForActorRefDeleted <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1864>`__ is called to resume owner tracking.

4. Actors that fail `OnInitializeActorShouldLoad <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L162>`__ (dead job, dead root owner, non-restartable DEAD):

   - Added to ``destroyed_actors_`` cache.
   - Their `ActorTaskSpec is batch deleted <https://github.com/ray-project/ray/blob/master/src/ray/gcs/gcs_actor_manager.cc#L1881>`__.


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
  │                           Core Worker                                 │
  │                                                                       │
  │  1. Generate ActorID                                                  │
  │  2. Build TaskSpec                                                    │
  │  3. Register with GCS (sync for named, async for unnamed)             │
  │  4. Submit creation task via ActorTaskSubmitter                       │
  └───────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                              GCS                                      │
  │                                                                       │
  │  ┌─────────────────────┐    ┌────────────────────────────────────┐    │
  │  │   GcsActorManager   │    │      GcsActorScheduler             │    │
  │  │                     │    │                                    │    │
  │  │ • Register actor    │───►│ • Select node (GCS or Raylet)      │    │
  │  │ • Track states      │    │ • Lease worker from Raylet         │    │
  │  │ • Handle failures   │◄───│ • Handle spillback/rejection       │    │
  │  │ • Manage restarts   │    │ • Create actor on worker           │    │
  │  └─────────────────────┘    └────────────────────────────────────┘    │
  │                 ▲                                                     │
  │                 │ AsyncReportWorkerFailure                            │
  └─────────────────│─────────────────────────────────────────────────────┘
                    │
                    │
  ┌─────────────────│─────────────────────────────────────────────────────┐
  │                 │           Raylet (Node Manager)                     │
  │                 │                                                     │
  │  • Handle worker lease requests (HandleRequestWorkerLease)            │
  │  • Manage local resources                                             │
  │  • Start/stop workers                                                 │
  │  • Report worker deaths to GCS                                        │
  └───────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                         Actor Worker                                  │
  │                                                                       │
  │  • Execute __init__ (via task_execution_handler)                      │
  │  • Receive and execute actor tasks (HandlePushTask)                   │
  │  • Maintain actor state                                               │
  │  • Handle graceful shutdown                                           │
  └───────────────────────────────────────────────────────────────────────┘


See also
--------

- :ref:`actor-guide`: User guide for working with actors
- :ref:`actor-lifetimes`: Named and detached actors
- :ref:`fault-tolerance-actors`: Actor fault tolerance and restarts
- :ref:`task-lifecycle`: Task lifecycle internals
