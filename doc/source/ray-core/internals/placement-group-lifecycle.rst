.. _placement-group-lifecycle:

Placement Group Lifecycle
=========================

This document describes the lifecycle of a placement group in Ray Core: creation, the two-phase-commit scheduling protocol, and interaction with the autoscaler. Readers are assumed to already be familiar with the user-facing `Placement Groups <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html>`__ concept. The following code is used as a running example; internals are pinned to Ray master as of commit `b65060749588 <https://github.com/ray-project/ray/commit/b65060749588ae653091f1903a6256a6f3c44174>`__ (2026-04-06), post Ray 2.54.

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

.. code-block:: text

    {
        'placement_group_id': '15f36322...',
        'bundles': {0: {'CPU': 1.0}, 1: {'CPU': 1.0}},
        'bundles_to_node_id': {0: '89b6a82a...', 1: '89b6a82a...'},
        'strategy': 'PACK',
        'state': 'CREATED',
        'stats': {'end_to_end_creation_latency_ms': 2.937, ...},
    }

Creating a placement group
--------------------------

Python / Cython side
~~~~~~~~~~~~~~~~~~~~

Placement group creation on the Python side is **synchronous**: the GCS persists the placement group spec to the ``gcs_table_storage`` backend table before replying, so on return the placement group is guaranteed to survive a GCS restart.

The entry point for placement group creation is the `placement_group() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/util/placement_group.py#L126>`__ function in ``python/ray/util/placement_group.py``. This function:

1. `Validates <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/util/placement_group.py#L172>`__ all input parameters via ``validate_placement_group()`` — checks that the strategy is one of PACK, SPREAD, STRICT_PACK, or STRICT_SPREAD, validates bundles and label selectors, and checks that ``lifetime`` is either ``None`` or ``"detached"``.

2. Converts the ``lifetime`` parameter to a boolean ``detached`` flag (``"detached"`` → ``True``, otherwise ``False``). Denotes whether the placement group is detached or not.

3. `Calls <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/util/placement_group.py#L188>`__ into the Cython layer via ``worker.core_worker.create_placement_group(name, bundles, strategy, detached, soft_target_node_id, bundle_label_selector)``.

4. Wraps the returned ``PlacementGroupID`` in a ``PlacementGroup`` python object and returns it to the caller.

The Cython method `create_placement_group() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/_raylet.pyx#L3693>`__ in ``python/ray/_raylet.pyx`` converts Python types to C++ types:

- Converts the strategy string (e.g. ``b"PACK"``) to a ``CPlacementStrategy`` enum.
- Converts ``soft_target_node_id`` from a hex string to a ``CNodeID``.
- `Releases the GIL <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/_raylet.pyx#L3721>`__ with ``with nogil:`` and calls ``CCoreWorkerProcess.GetCoreWorker().CreatePlacementGroup()`` with a ``CPlacementGroupCreationOptions`` struct containing all the parameters. Releasing the GIL lets other Python threads in the process keep running while this thread is blocked on the GCS round-trip.
- Returns the resulting ``PlacementGroupID`` back to Python.

C++ side
~~~~~~~~

`CoreWorker::CreatePlacementGroup() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/core_worker/core_worker.cc#L2244>`__ in ``src/ray/core_worker/core_worker.cc`` does the following:

1. Validates that no bundle uses the reserved ``"bundle"`` resource label. This will be used later to identify specific bundle resources.
2. Generates a ``PlacementGroupID`` from the current ``JobID`` — this ties the placement group to its creator for lifecycle management.
3. Uses `PlacementGroupSpecBuilder <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/common/placement_group.h#L72>`__ to `construct <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/core_worker/core_worker.cc#L2259>`__ the protobuf `PlacementGroupSpec <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/common.proto#L688>`__. The spec includes: placement group ID, name, strategy, `bundles <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/common.proto#L674>`__ (each with resources and label selectors), creator job/actor IDs, detached flag, and soft target node ID.
4. `Sends <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/core_worker/core_worker.cc#L2275>`__ a synchronous ``CreatePlacementGroup`` gRPC to the GCS via ``gcs_client_->PlacementGroups().SyncCreatePlacementGroup(spec)``.

The GCS service defines the `CreatePlacementGroup <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/gcs_service.proto#L510>`__ RPC which accepts a `CreatePlacementGroupRequest <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/gcs_service.proto#L437>`__ containing the ``PlacementGroupSpec``.

On the GCS side, `HandleCreatePlacementGroup() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L353>`__ in ``GcsPlacementGroupManager`` receives the request:

1. Creates a ``GcsPlacementGroup`` object with state ``PENDING``.
2. Calls `RegisterPlacementGroup() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L107>`__, which:

   a. Checks for duplicate placement group names within the namespace.
   b. Stores the placement group in the ``registered_placement_groups_`` map.
   c. Adds it to the ``pending_placement_groups_`` priority queue via ``AddToPendingQueue()``.
   d. Kicks off an asynchronous write to persist the placement group to storage, with a completion callback supplied by the handler.

3. When the persist callback fires, the handler **sends the RPC reply back to the worker** and **triggers** `SchedulePendingPlacementGroups() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L300>`__.

The RPC returns as soon as the GCS has persisted the spec — **before** scheduling completes. Actual resource scheduling happens asynchronously on the GCS, and the caller can block for readiness via ``pg.ready()``.

Scheduling a placement group
----------------------------

The placement group transitions through several states during this process, defined in `PlacementGroupTableData <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/gcs.proto#L632>`__. State is tracked per placement group, not per bundle — for example, if a node hosting one bundle of placement group ``pg`` goes down, all of ``pg`` transitions from ``CREATED`` to ``RESCHEDULING``, though only its unplaced bundles are re-scheduled when its turn comes up in the pending queue. From any state, ``remove_placement_group()`` releases the placement group's resources and transitions it to ``REMOVED``.

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

`SchedulePendingPlacementGroups() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L300>`__ pulls placement groups from the priority queue. Only one placement group is actively scheduled at a time. For each placement group, it creates a ``SchedulePgRequest`` with two callbacks:

- `OnPlacementGroupCreationFailed() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L205>`__ — handles scheduling/prepare/commit failures.
- `OnPlacementGroupCreationSuccess() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L246>`__ — handles successful creation.

It then calls `ScheduleUnplacedBundles() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L42>`__ in ``GcsPlacementGroupScheduler``, which extracts the bundles, builds a list of ``ResourceRequest`` objects, and calls `CreateSchedulingOptions() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L493>`__ to map the placement strategy to the corresponding scheduling type.

The scheduling options are passed to `ClusterResourceScheduler::SchedulePlacementGroup() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/cluster_resource_scheduler.cc#L395>`__, which delegates to `CompositeBundleSchedulingPolicy::Schedule() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/policy/composite_scheduling_policy.cc#L46>`__. The composite policy routes to one of four policies based on strategy:

- `BundlePackSchedulingPolicy <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L144>`__ (PACK)
- `BundleSpreadSchedulingPolicy <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L225>`__ (SPREAD)
- `BundleStrictPackSchedulingPolicy <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L290>`__ (STRICT_PACK)
- `BundleStrictSpreadSchedulingPolicy <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc#L385>`__ (STRICT_SPREAD)

For details on what each strategy means, see the user-facing `Placement Strategies <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html#placement-strategy>`__ documentation. All policies share common logic: they filter available candidate nodes, check feasibility (can every bundle fit on at least one node), sort bundles by resource scarcity (GPU-heavy bundles first), and score nodes using a ``LeastResourceScorer``.

Label-domain pre-check
^^^^^^^^^^^^^^^^^^^^^^

Before dispatching on strategy, ``CompositeBundleSchedulingPolicy::Schedule()`` checks the scheduling options for a label-domain constraint. If one is present (``label_domain_scheduling_strategy_ != NONE``), the composite policy wraps the per-strategy path above with an additional scheduling layer: `LabelDomainStrictPackSchedulingPolicy::Schedule() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/scheduling/policy/label_domain_bundle_scheduling_policy.cc#L55>`__. The label-domain constraint is set by ``CreateSchedulingOptions`` when the placement group has a ``label_domain_key``. Currently, this key is only auto-assigned when `ComputeLabelDomainKey() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group.cc#L158>`__ detects a GPU-typed accelerator label on the first bundle. Currently, only ``STRICT_PACK`` is supported at the label-domain level, which means all bundles of a placement group must land in the same label-domain value (e.g. all on ``GB200`` nodes or all on ``GB300`` nodes, but not a mix). In the future, there will be an external python placement group API that allows arbitrarily specifying a label domain for bundles.

The label-domain policy has two modes:

- **No pinned domain value**: Groups candidate nodes by the value of the label key, tests feasibility per group, then tries each feasible group by invoking a callback that recurses back into ``CompositeBundleSchedulingPolicy::Schedule()`` with the label-domain strategy cleared — so the recursion falls through to the normal per-strategy policy on that group's nodes. Returns on the first success, and records the chosen domain value on the ``SchedulingResult`` so later reschedulings stay pinned to it.
- **Pinned domain value**: Prunes candidate nodes to the pinned domain, then recurses into the per-strategy policy on that subset directly. This guarantees a rescheduled placement group never migrates to a different label domain.

The scheduler returns a ``SchedulingResult`` with a status and a ``selected_nodes`` vector mapping each bundle to a node:

- **SUCCESS**: Valid bundle-to-node mapping found. Proceed to prepare phase.
- **INFEASIBLE**: No valid placement exists (e.g. a bundle requests more GPUs than any node has). ``OnPlacementGroupCreationFailed(is_feasible=false)`` moves the placement group to the ``infeasible_placement_groups_`` queue, where it waits until a new node is added to the cluster.
- **FAILED**: Temporary resource shortage (resources are valid but currently occupied). ``OnPlacementGroupCreationFailed(is_feasible=true)`` re-queues the placement group with exponential backoff.

Feasibility
^^^^^^^^^^^

Feasibility determines *whether* a failed placement group gets retried. The classification is made by `OnPlacementGroupCreationFailed() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L205>`__ through its ``is_feasible`` parameter:

.. list-table::
   :header-rows: 1
   :widths: 22 33 45

   * - Placement group state
     - Typical cause
     - Retry behavior
   * - feasible + ``PENDING``
     - Resources exist in the cluster but are currently occupied
     - Re-added to ``pending_placement_groups_`` with exponential backoff
   * - feasible + ``RESCHEDULING``
     - A node hosting a bundle of a previously-placed placement group died
     - Re-added to ``pending_placement_groups_`` at ``rank=0`` — priority over all other pending placement groups
   * - infeasible
     - Aggregate cluster resources clearly cannot satisfy the placement group's resource request (e.g. total GPUs across all nodes is less than what the bundles ask for)
     - Parked in the `infeasible_placement_groups_ <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.h#L314>`__ deque. Only two events wake it up: (1) `OnNodeAdd() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L733>`__ when a new node joins, or (2) in label-domain mode, when all bundles of the placement group become unplaced — allowing a different label-domain value to be attempted.

.. warning::

   The boundary between feasible and infeasible requires careful management. Err too strict (marking feasible placement groups as infeasible) and they sit in ``infeasible_placement_groups_`` until one of the two wake-up events, wasting resources. Err too lax (marking infeasible ones as feasible) and you can redundantly schedule duplicates — and hit a rare starvation case:

   If a placement group in ``RESCHEDULING`` is truly infeasible but marked as feasible, it is re-queued at ``rank=0`` forever, blocking every other pending placement group behind it. This is not straightforward to solve — nodes can legitimately spin back up, which *does* justify priority scheduling, and for a placement group to be in ``RESCHEDULING`` it must have been feasible at some point, so being permanently infeasible again is rare in practice. For now this edge case is documented here and in the code rather than worked around.

2. Preparing bundle placements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On a successful scheduling result, ``ScheduleUnplacedBundles()`` proceeds to the prepare phase:

1. Calls `AcquireBundleResources() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L674>`__ to tentatively deduct the scheduled resources from the GCS's cluster resource view. This prevents other concurrent scheduling decisions from double-booking the same resources.

2. Creates a `LeaseStatusTracker <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.h#L126>`__ to track the prepare/commit state for each bundle across all nodes.

3. Groups bundles by their destination node. For each node, sends a ``PrepareBundleResources`` gRPC via `PrepareResources() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L199>`__ to its corresponding raylet.

On the raylet side, `HandlePrepareBundleResources() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/node_manager.cc#L1921>`__ in ``NodeManager`` calls `PrepareBundles() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/placement_group_resource_manager.cc#L83>`__ in ``NewPlacementGroupResourceManager``, which calls ``LocalResourceManager::AllocateLocalTaskResources()`` to **lock** the original resources (CPU, GPU, etc.) on that node. The resources are reserved but not yet converted to placement-group-specific names. The raylet returns success or failure.

When all prepare responses arrive, `OnAllBundlePrepareRequestReturned() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L387>`__ is called:

- **If any node failed to prepare**: All successfully-prepared bundles are cancelled, resources are returned to the GCS's cluster view via `ReturnBundleResources() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L746>`__, and ``OnPlacementGroupCreationFailed()`` is invoked to retry.
- **If all nodes succeeded**: The placement group state transitions to ``PREPARED``, is persisted to storage, and proceeds to the commit phase.

3. Committing bundle placements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`CommitAllBundles() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L319>`__ groups the prepared bundles by node and sends a ``CommitBundleResources`` gRPC to each raylet via `CommitResources() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L230>`__.

On the raylet side, `HandleCommitBundleResources() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/node_manager.cc#L1938>`__ calls `CommitBundles() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/raylet/placement_group_resource_manager.cc#L150>`__ in ``NewPlacementGroupResourceManager``. For each resource the user requested in the bundle (e.g. ``CPU``, ``GPU``, custom resources), this renames the locked resource into two placement-group-specific forms and adds them to the ``LocalResourceManager``:

- **Bundle-specific**: ``<resource>_group_<bundle_index>_<placement_group_id>`` (e.g. ``CPU_group_0_<placement_group_id>``) — matched when a task targets a specific bundle index.
- **Wildcard**: ``<resource>_group_<placement_group_id>`` — matched when a task targets the placement group but any bundle is acceptable.

In addition, two bookkeeping resources, ``bundle_group_<bundle_index>_<placement_group_id>`` and ``bundle_group_<placement_group_id>``, are each created with a quantity of ``1000``. Tasks scheduled with a ``PlacementGroupSchedulingStrategy`` are rewritten to request ``0.001`` of the appropriate bookkeeping resource, so the pool effectively never runs out — the bookkeeping resource exists only to route the task to the right node, while the actual capacity limit comes from the renamed user resources above.

Tasks scheduled with a ``PlacementGroupSchedulingStrategy`` can now be matched against these placement-group-specific resources.

When all commit responses arrive, `OnAllBundleCommitRequestReturned() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_scheduler.cc#L434>`__ is called:

- **If commit fails**: The placement group state transitions to ``RESCHEDULING``, failed bundles have their node assignments cleared, resources are returned, and ``OnPlacementGroupCreationFailed(is_feasible=true)`` is invoked with high priority (rank=0) for immediate retry.
- **If commit succeeds**: `OnPlacementGroupCreationSuccess() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L246>`__ is called, which:

  1. Records scheduling latency and creation latency metrics.
  2. Transitions the placement group state to ``CREATED``.
  3. Persists the final state to storage.
  4. Invokes all ``WaitPlacementGroupUntilReady`` callbacks — this is what ``pg.ready()`` awaits on the client side.
  5. Triggers ``SchedulePendingPlacementGroups()`` to process the next placement group in the queue.

Interaction with Autoscaler
---------------------------

The autoscaler needs to know about pending placement groups so it can scale up the cluster to satisfy their resource demands. This information flows from the GCS to the autoscaler via a periodic reporting mechanism.

**GCS side — collecting placement group load:**

`GcsPlacementGroupManager::Tick() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L805>`__ runs every 1 second and calls `UpdatePlacementGroupLoad() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L865>`__, which calls `GetPlacementGroupLoad() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_placement_group_manager.cc#L817>`__. This collects all placement groups in ``PENDING`` or ``RESCHEDULING`` state (from both the ``pending_placement_groups_`` queue and the ``infeasible_placement_groups_`` deque) into a ``PlacementGroupLoad`` proto, which is stored in ``GcsResourceManager``.

**GCS side — converting to gang resource requests:**

When the autoscaler polls via the ``GetClusterResourceState`` RPC, `MakeClusterResourceStateInternal() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_autoscaler_state_manager.cc#L178>`__ calls `GetPendingGangResourceRequests() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/gcs/gcs_autoscaler_state_manager.cc#L192>`__, which converts each pending/rescheduling placement group into a `GangResourceRequest <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/autoscaler.proto#L109>`__ message. Only **unplaced bundles** (those with nil ``node_id``) are included to avoid double-counting resources already allocated. Placement constraints (anti-affinity for STRICT_SPREAD, affinity for STRICT_PACK) are attached to each request. These gang requests are populated into the `ClusterResourceState <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/src/ray/protobuf/autoscaler.proto#L230>`__ message's ``pending_gang_resource_requests`` field.

**Autoscaler side — processing gang requests:**

The Python autoscaler's `update_autoscaling_state() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/autoscaler/v2/autoscaler.py#L176>`__ periodically fetches the ``ClusterResourceState`` and passes ``pending_gang_resource_requests`` to the ``ResourceDemandScheduler``. The scheduler's `_sched_gang_resource_requests() <https://github.com/ray-project/ray/blob/b65060749588ae653091f1903a6256a6f3c44174/python/ray/autoscaler/v2/scheduler.py#L1429>`__ method tests for whether it is possible to fit the current gang resource requests on either existing nodes or adding new nodes:

1. Sorts gang requests by constraint complexity (most constrained first).
2. For each gang request, tests if all bundles can be placed on existing nodes or with new nodes.
3. If any bundle in a gang can't be satisfied, the entire gang request is marked infeasible.
4. Based on the results, the scheduler decides which node types to launch to satisfy demands.

Infeasible gang requests are reported back to the GCS via the ``AutoscalingState.infeasible_gang_resource_requests`` field. For more information on the autoscaler itself, see the :ref:`autoscaler-v2` internals doc.
