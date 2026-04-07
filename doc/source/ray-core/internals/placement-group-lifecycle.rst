.. _placement-group-lifecycle:

Placement Group Lifecycle
=========================

This doc will talk about the lifecycle of a placement group in Ray Core, such as how placement groups are created, the two-phase commit schedule, and execution. First of all, what is a placement group? It is a way to schedule a group of bundles of resources atomically across some number of nodes. For more details it's better to first read up on the outside docs for `Placement Groups <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html>`__.
The following code is an example of calling a placement group with internals based on Ray 2.54.

.. code-block:: python

    import ray

    from ray.util.placement_group import (
        placement_group,
        placement_group_table,
        remove_placement_group,
    )

    ray.init(num_cpus=8)

    pg = ray.util.placement_group(
        bundles = [{"CPU": 1}] * 2,
    )

    ray.get(pg.ready(), timeout=10)
    print(placement_group_table(pg))

.. code-block:: output

    {'placement_group_id': '15f363223a4fad493eedb9942b0401000000', 'name': '', 'bundles': {0: {'CPU': 1.0}, 1: {'CPU': 1.0}}, 'bundles_to_node_id': {0: '89b6a82aa91b22daac8ddca0087f8bc63267020e8b2ff8107aae2ab1', 1: '89b6a82aa91b22daac8ddca0087f8bc63267020e8b2ff8107aae2ab1'}, 'strategy': 'PACK', 'state': 'CREATED', 'stats': {'end_to_end_creation_latency_ms': 2.937, 'scheduling_latency_ms': 2.862, 'scheduling_attempt': 1, 'highest_retry_delay_ms': 0.0, 'scheduling_state': 'FINISHED'}}

Creating a placement group
--------------------------

Python / Cython side
~~~~~~~~~~~~~~~~~~~~

Placement group creation on the python side is **synchronous** — the Python thread blocks until the GCS responds. This helps ensure that the GCS has at least stored the placement group into the ``gcs_table_storage`` backend table and thus will be fault tolerant.

The entry point for placement group creation is the `placement_group() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/util/placement_group.py#L126>`__ function in ``python/ray/util/placement_group.py``. This function:

1. `Validates <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/util/placement_group.py#L172>`__ all input parameters via ``validate_placement_group()`` — checks that the strategy is one of PACK, SPREAD, STRICT_PACK, or STRICT_SPREAD, validates bundles and label selectors, and checks that ``lifetime`` is either ``None`` or ``"detached"``.

2. Converts the ``lifetime`` parameter to a boolean ``detached`` flag (``"detached"`` → ``True``, otherwise ``False``). Denotes whether the placement group is detached or not.

3. `Calls <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/util/placement_group.py#L188>`__ into the Cython layer via ``worker.core_worker.create_placement_group(name, bundles, strategy, detached, soft_target_node_id, bundle_label_selector)``.

4. Wraps the returned ``PlacementGroupID`` in a ``PlacementGroup`` python object and returns it to the caller.

The Cython method `create_placement_group() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/_raylet.pyx#L3693>`__ in ``python/ray/_raylet.pyx`` converts Python types to C++ types:

- Converts the strategy string (e.g. ``b"PACK"``) to a ``CPlacementStrategy`` enum.
- Converts ``soft_target_node_id`` from a hex string to a ``CNodeID``.
- `Releases the GIL <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/_raylet.pyx#L3721>`__ with ``with nogil:`` and calls ``CCoreWorkerProcess.GetCoreWorker().CreatePlacementGroup()`` with a ``CPlacementGroupCreationOptions`` struct containing all the parameters. Releasing the GIL allows other threads to run Cython as the function executes.
- Returns the resulting ``PlacementGroupID`` back to Python.

C++ side
~~~~~~~~

`CoreWorker::CreatePlacementGroup() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/core_worker/core_worker.cc#L2244>`__ in ``src/ray/core_worker/core_worker.cc`` does the following:

1. Validates that no bundle uses the reserved ``"bundle"`` resource label. This will be used later to identify specific bundle resources.
2. Generates a ``PlacementGroupID`` from the current ``JobID`` — this ties the placement group to its creator for lifecycle management.
3. Uses `PlacementGroupSpecBuilder <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/common/placement_group.h#L72>`__ to `construct <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/core_worker/core_worker.cc#L2259>`__ the protobuf `PlacementGroupSpec <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/common.proto#L688>`__. The spec includes: placement group ID, name, strategy, `bundles <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/common.proto#L674>`__ (each with resources and label selectors), creator job/actor IDs, detached flag, and soft target node ID.
4. `Sends <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/core_worker/core_worker.cc#L2275>`__ a synchronous ``CreatePlacementGroup`` gRPC to the GCS via ``gcs_client_->PlacementGroups().SyncCreatePlacementGroup(spec)``.

The GCS service defines the `CreatePlacementGroup <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/gcs_service.proto#L510>`__ RPC which accepts a `CreatePlacementGroupRequest <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/gcs_service.proto#L437>`__ containing the ``PlacementGroupSpec``.

On the GCS side, `HandleCreatePlacementGroup() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L353>`__ in ``GcsPlacementGroupManager`` receives the request:

1. Creates a ``GcsPlacementGroup`` object with state ``PENDING``.
2. Calls `RegisterPlacementGroup() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L107>`__ which:

   a. Checks for duplicate placement group names within the namespace.
   b. Stores the placement group in the ``registered_placement_groups_`` map.
   c. Adds it to the ``pending_placement_groups_`` priority queue via ``AddToPendingQueue()``.
   d. Persists the placement group to storage asynchronously.
   e. Once persisted, **sends the RPC reply back to the worker** and **triggers** `SchedulePendingPlacementGroups() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L300>`__.

The RPC response goes back to the CoreWorker **before** scheduling completes. The worker receives the ``PlacementGroupID`` immediately after the GCS persists the placement group to storage; actual resource scheduling happens asynchronously on the GCS. This means the Python function would be no longer blocked after the GCS finishes persisting. The user can block for readiness of scheduling via ``pg.ready()``.

Scheduling a placement group
----------------------------

The placement group follows a simulation + **two-phase commit** protocol: the GCS first simulates where bundles should go, then prepares (locks) resources on each raylet, and finally commits them. The placement group transitions through several states during this process, defined in `PlacementGroupTableData <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/gcs.proto#L632>`__. Placement groups follow these states as they get scheduled on the placement group granularity. For example, if a node goes down with a bundle of placment group i, then placement group i state goes from created -> rescheduling. However, when it is the placement group's turn to get off the pending queue, it only schedules any unplaced bundles. At any state, if remove placement group is invoked and it is deleted from the table, each state will return its current resources and remove the placement group. 

.. code-block:: text

    ┌──────────┐    all prepares     ┌──────────┐     all commits      ┌──────────┐
    │ PENDING  │──── succeed ───────▶│ PREPARED │────── succeed ──────▶│ CREATED  │
    │   (0)    │                     │   (1)    │                      │   (2)    │
    └──┬───┬───┘                     └────┬──▲──┘                      └────┬─────┘
       │   ▲                              │  │                              │
       │   │ scheduling or                │  │ re-schedule +                │
       │   │ prepare fails                │  │ prepare succeeds             │
       │   │ (retry with                  │  │                              │
       │   │ exponential backoff)         │  │                         node death
       │   │                              │  │                              │
       └───┘                  commit fails│  │                              │
                              (retry with │  │                              │
                              priority)   │  │                              │
                                     ┌────▼──┴──────────┐                   │
                                     │  RESCHEDULING    │◀──────────────────┘
                                     │      (4)         │
                                     └──┬────┬──────────┘
                                        │    ▲
                                        │    │ scheduling or prepare fails
                                        │    │ (retry with priority, rank=0)
                                        └────┘

    remove_placement_group() from any state
                    │
                    ▼
             ┌──────────┐
             │ REMOVED  │
             │   (3)    │
             └──────────┘

1. Simulating bundle placements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`SchedulePendingPlacementGroups() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L300>`__ pulls placement groups from the priority queue. Only one placement group is actively scheduled at a time. For each placement group, it creates a ``SchedulePgRequest`` with two callbacks:

- `OnPlacementGroupCreationFailed() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L205>`__ — handles scheduling/prepare/commit failures.
- `OnPlacementGroupCreationSuccess() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L246>`__ — handles successful creation.

It then calls `ScheduleUnplacedBundles() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L41>`__ in ``GcsPlacementGroupScheduler``, which extracts the bundles, builds a list of ``ResourceRequest`` objects, and calls `CreateSchedulingOptions() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L472>`__ to map the placement strategy to the corresponding scheduling type.

The scheduling options are passed to `ClusterResourceScheduler::SchedulePlacementGroup() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/scheduling/cluster_resource_scheduler.cc#L397>`__, which delegates to `CompositeBundleSchedulingPolicy::Schedule() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/scheduling/policy/composite_scheduling_policy.cc#L46>`__. The composite policy routes to one of four policies based on strategy:

- `BundlePackSchedulingPolicy <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L156>`__ (PACK)
- `BundleSpreadSchedulingPolicy <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L238>`__ (SPREAD)
- `BundleStrictPackSchedulingPolicy <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L304>`__ (STRICT_PACK)
- `BundleStrictSpreadSchedulingPolicy <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L383>`__ (STRICT_SPREAD)

For details on what each strategy means, see the user-facing `Placement Strategies <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html#placement-strategy>`__ documentation. All policies share common logic: they filter available candidate nodes, check feasibility (can every bundle fit on at least one node), sort bundles by resource scarcity (GPU-heavy bundles first), and score nodes using a ``LeastResourceScorer``.

The scheduler returns a ``SchedulingResult`` with a status and a ``selected_nodes`` vector mapping each bundle to a node:

- **SUCCESS**: Valid bundle-to-node mapping found. Proceed to prepare phase.
- **INFEASIBLE**: No valid placement exists (e.g. a bundle requests more GPUs than any node has). ``OnPlacementGroupCreationFailed(is_feasible=false)`` moves the placement group to the ``infeasible_placement_groups_`` queue, where it waits until a new node is added to the cluster.
- **FAILED**: Temporary resource shortage (resources are valid but currently occupied). ``OnPlacementGroupCreationFailed(is_feasible=true)`` re-queues the placement group with exponential backoff.

2. Preparing bundle placements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On a successful scheduling result, ``ScheduleUnplacedBundles()`` proceeds to the prepare phase:

1. Calls `AcquireBundleResources() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L641>`__ to tentatively deduct the scheduled resources from the GCS's cluster resource view. This prevents other concurrent scheduling decisions from double-booking the same resources.

2. Creates a `LeaseStatusTracker <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.h#L126>`__ to track the prepare/commit state for each bundle across all nodes.

3. Groups bundles by their destination node. For each node, sends a ``PrepareBundleResources`` gRPC via `PrepareResources() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L178>`__ to its corresponding raylet.

On the raylet side, `HandlePrepareBundleResources() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/node_manager.cc#L1896>`__ in ``NodeManager`` calls `PrepareBundles() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/placement_group_resource_manager.cc#L83>`__ in ``NewPlacementGroupResourceManager``, which calls ``LocalResourceManager::AllocateLocalTaskResources()`` to **lock** the original resources (CPU, GPU, etc.) on that node. The resources are reserved but not yet converted to placement-group-specific names. The raylet returns success or failure.

When all prepare responses arrive, `OnAllBundlePrepareRequestReturned() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L366>`__ is called:

- **If any node failed to prepare**: All successfully-prepared bundles are cancelled, resources are returned to the GCS's cluster view via `ReturnBundleResources() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L716>`__, and ``OnPlacementGroupCreationFailed()`` is invoked to retry.
- **If all nodes succeeded**: The placement group state transitions to ``PREPARED``, is persisted to storage, and proceeds to the commit phase.

3. Committing bundle placements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`CommitAllBundles() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L298>`__ groups the prepared bundles by node and sends a ``CommitBundleResources`` gRPC to each raylet via `CommitResources() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L209>`__.

On the raylet side, `HandleCommitBundleResources() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/node_manager.cc#L1913>`__ calls `CommitBundles() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/raylet/placement_group_resource_manager.cc#L150>`__ in ``NewPlacementGroupResourceManager``. This converts the locked resources into **placement-group-specific resource names** and adds them to the ``LocalResourceManager``. Two special types of PG resources are also created, in addition to other resources specified by the user:

- **Bundle-specific**: ``CPU_group_<bundle_index>_<PG_ID>`` — used when a task targets a specific bundle index.
- **Wildcard**: ``CPU_group_<PG_ID>`` — used when a task targets the placement group but any bundle is acceptable.

These resources are initialized with a value of 1000 and can be targeted with tasks with value 0.001, essentially allowing for unlimited tasks to be targeted to either specific bundles or specific placement groups through this mechanism.

Tasks scheduled with a ``PlacementGroupSchedulingStrategy`` can now be matched against these PG-specific resources.

When all commit responses arrive, `OnAllBundleCommitRequestReturned() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_scheduler.cc#L413>`__ is called:

- **If commit fails**: The placement group state transitions to ``RESCHEDULING``, failed bundles have their node assignments cleared, resources are returned, and ``OnPlacementGroupCreationFailed(is_feasible=true)`` is invoked with high priority (rank=0) for immediate retry.
- **If commit succeeds**: `OnPlacementGroupCreationSuccess() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L246>`__ is called, which:

  1. Records scheduling latency and creation latency metrics.
  2. Transitions the placement group state to ``CREATED``.
  3. Persists the final state to storage.
  4. Invokes all ``WaitPlacementGroupUntilReady`` callbacks — this is what ``pg.ready()`` awaits on the client side.
  5. Triggers ``SchedulePendingPlacementGroups()`` to process the next placement group in the queue.

Interaction with Autoscaler
---------------------------

The autoscaler needs to know about pending placement groups so it can scale up the cluster to satisfy their resource demands. This information flows from the GCS to the autoscaler via a periodic reporting mechanism.

**GCS side — collecting placement group load:**

`GcsPlacementGroupManager::Tick() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L805>`__ runs every 1 second and calls `UpdatePlacementGroupLoad() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L865>`__, which calls `GetPlacementGroupLoad() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_placement_group_manager.cc#L817>`__. This collects all placement groups in ``PENDING`` or ``RESCHEDULING`` state (from both the ``pending_placement_groups_`` queue and the ``infeasible_placement_groups_`` deque) into a ``PlacementGroupLoad`` proto, which is stored in ``GcsResourceManager``.

**GCS side — converting to gang resource requests:**

When the autoscaler polls via the ``GetClusterResourceState`` RPC, `MakeClusterResourceStateInternal() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_autoscaler_state_manager.cc#L178>`__ calls `GetPendingGangResourceRequests() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/gcs/gcs_autoscaler_state_manager.cc#L192>`__, which converts each pending/rescheduling placement group into a `GangResourceRequest <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/autoscaler.proto#L85>`__ message. Only **unplaced bundles** (those with nil ``node_id``) are included to avoid double-counting resources already allocated. Placement constraints (anti-affinity for STRICT_SPREAD, affinity for STRICT_PACK) are attached to each request. These gang requests are populated into the `ClusterResourceState <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/src/ray/protobuf/autoscaler.proto#L206>`__ message's ``pending_gang_resource_requests`` field.

**Autoscaler side — processing gang requests:**

The Python autoscaler's `update_autoscaling_state() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/autoscaler/v2/autoscaler.py#L176>`__ periodically fetches the ``ClusterResourceState`` and passes ``pending_gang_resource_requests`` to the ``ResourceDemandScheduler``. The scheduler's `_sched_gang_resource_requests() <https://github.com/ray-project/ray/blob/3ee7c9d6df4b098fa74df2d089342621cd7a7d2c/python/ray/autoscaler/v2/scheduler.py#L1429>`__ method tests for whether it is possible to fit the current gang resource requests on either existing nodes or adding new nodes:

1. Sorts gang requests by constraint complexity (most constrained first).
2. For each gang request, tests if all bundles can be placed on existing nodes or with new nodes.
3. If any bundle in a gang can't be satisfied, the entire gang request is marked infeasible.
4. Based on the results, the scheduler decides which node types to launch to satisfy demands.

Infeasible gang requests are reported back to the GCS via the ``AutoscalingState.infeasible_gang_resource_requests`` field.
