# Ray C++ Architecture Guide

This document provides detailed architecture information about Ray's C++ components, including state machines, component relationships, and design patterns. For a quick reference of key files, see [CLAUDE.md](CLAUDE.md).

## Table of Contents
- [Task Lifecycle](#task-lifecycle)
- [Actor Lifecycle](#actor-lifecycle)
- [Object Lifecycle](#object-lifecycle)
- [Resource Management & Scheduling](#resource-management--scheduling)
- [Placement Groups](#placement-groups)
- [Global Control Service (GCS)](#global-control-service-gcs)
- [Fault Tolerance](#fault-tolerance)

---

## Task Lifecycle

### Overview

Tasks in Ray go through multiple components: the submitting CoreWorker, the Raylet scheduler, and the executing worker. The `TaskManager` on the owner side tracks task state, while the Raylet handles scheduling and worker assignment.

### Task State Machine (Owner Side)

```
                    ┌─────────────────────┐
                    │  PENDING_ARGS_AVAIL │  Task submitted, waiting for dependencies
                    └──────────┬──────────┘
                               │ Dependencies resolved
                               ▼
                    ┌─────────────────────┐
                    │ SUBMITTED_TO_WORKER │  Task sent to worker for execution
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
       ┌──────────┐     ┌──────────┐     ┌──────────┐
       │ FINISHED │     │  FAILED  │     │ RETRYING │
       └──────────┘     └──────────┘     └────┬─────┘
                                              │
                                              └──► Back to PENDING_ARGS_AVAIL
```

### Task Execution Flow

```
User Code                CoreWorker (Owner)              Raylet                    CoreWorker (Executor)
    │                          │                           │                              │
    │ ray.remote(f).remote()   │                           │                              │
    │─────────────────────────►│                           │                              │
    │                          │                           │                              │
    │                          │ SubmitTask()              │                              │
    │                          │──────────────────────────►│                              │
    │                          │                           │                              │
    │                          │                           │ Schedule task                │
    │                          │                           │ (find node, lease worker)    │
    │                          │                           │                              │
    │                          │ WorkerLease granted       │                              │
    │                          │◄──────────────────────────│                              │
    │                          │                           │                              │
    │                          │ PushTask RPC              │                              │
    │                          │─────────────────────────────────────────────────────────►│
    │                          │                           │                              │
    │                          │                           │                              │ Execute task
    │                          │                           │                              │
    │                          │ PushTaskReply (result)    │                              │
    │                          │◄─────────────────────────────────────────────────────────│
    │                          │                           │                              │
    │ ray.get() returns        │                           │                              │
    │◄─────────────────────────│                           │                              │
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| TaskSpecification | `common/task/task_spec.h` | Task metadata, dependencies, resources |
| TaskManager | `core_worker/task_manager.h` | Owner-side task state, retries, lineage |
| NormalTaskSubmitter | `core_worker/task_submission/normal_task_submitter.h` | Worker leasing, task queuing |
| DependencyResolver | `core_worker/task_submission/dependency_resolver.h` | Resolve task arguments |
| TaskReceiver | `core_worker/task_execution/task_receiver.h` | Worker-side task handling |
| ClusterLeaseManager | `raylet/scheduling/cluster_lease_manager.h` | Cluster-wide worker leasing |

### Dependency Resolution

Before a task can execute, all its arguments must be available:

1. **Inline arguments**: Small values passed directly in the task spec
2. **ObjectRef arguments**: References to objects that must be fetched
3. **Actor dependencies**: For actor tasks, previous task must complete

The `LocalDependencyResolver` waits for all ObjectRefs to be available in the local object store before marking the task ready for submission.

---

## Actor Lifecycle

### Overview

Actors are long-running stateful workers. The GCS manages cluster-wide actor state, while each CoreWorker tracks local actor handles.

### Actor State Machine

```
                        ┌────────────────────────┐
                        │  DEPENDENCIES_UNREADY  │  Waiting for actor creation args
                        └───────────┬────────────┘
                                    │ Args available
                                    ▼
                        ┌────────────────────────┐
                        │   PENDING_CREATION     │  Waiting for worker assignment
                        └───────────┬────────────┘
                                    │ Worker assigned, actor started
                                    ▼
                        ┌────────────────────────┐
            ┌──────────►│        ALIVE           │◄──────────┐
            │           └───────────┬────────────┘           │
            │                       │                        │
            │                       │ Worker/node failure    │
            │                       ▼                        │
            │           ┌────────────────────────┐           │
            │           │      RESTARTING        │───────────┘
            │           └───────────┬────────────┘   (if restarts remaining)
            │                       │
            │                       │ Max restarts exceeded OR
            │                       │ Owner died OR
            │                       │ Explicit kill
            │                       ▼
            │           ┌────────────────────────┐
            └───────────│        DEAD            │
              (detached └────────────────────────┘
               actors
               can restart)
```

### Actor Creation Flow

```
User Code           CoreWorker              GCS                      Raylet              Worker
    │                   │                    │                          │                   │
    │ @ray.remote       │                    │                          │                   │
    │ class Foo         │                    │                          │                   │
    │                   │                    │                          │                   │
    │ Foo.remote()      │                    │                          │                   │
    │──────────────────►│                    │                          │                   │
    │                   │                    │                          │                   │
    │                   │ RegisterActor      │                          │                   │
    │                   │───────────────────►│                          │                   │
    │                   │                    │                          │                   │
    │                   │                    │ Schedule actor           │                   │
    │                   │                    │─────────────────────────►│                   │
    │                   │                    │                          │                   │
    │                   │                    │                          │ Start worker      │
    │                   │                    │                          │──────────────────►│
    │                   │                    │                          │                   │
    │                   │                    │ ActorCreated             │                   │
    │                   │                    │◄─────────────────────────────────────────────│
    │                   │                    │                          │                   │
    │                   │ Actor ready        │                          │                   │
    │                   │◄───────────────────│                          │                   │
    │                   │                    │                          │                   │
    │ actor.method()    │                    │                          │                   │
    │──────────────────►│                    │                          │                   │
    │                   │                    │                          │                   │
    │                   │ Direct RPC (bypasses Raylet)                  │                   │
    │                   │──────────────────────────────────────────────────────────────────►│
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| GcsActorManager | `gcs/gcs_actor_manager.h` | Cluster-wide actor lifecycle |
| GcsActorScheduler | `gcs/gcs_actor_scheduler.h` | Actor placement decisions |
| ActorManager | `core_worker/actor_manager.h` | Per-worker actor tracking |
| ActorCreator | `core_worker/actor_creator.h` | Actor creation requests |
| ActorHandle | `core_worker/actor_handle.h` | Handle serialization |
| ActorTaskSubmitter | `core_worker/task_submission/actor_task_submitter.h` | Actor method calls |

### Actor Task Ordering

Actor tasks have sequence numbers to ensure ordering:
- **Sequential actors**: Tasks execute in strict sequence number order
- **Threaded actors**: Tasks can execute concurrently but still track sequence numbers
- **Async actors**: Use out-of-order scheduling queues

---

## Object Lifecycle

### Overview

Ray objects are immutable values stored in the distributed object store (Plasma). Reference counting determines when objects can be deleted.

### Reference Types

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Reference Types                               │
├─────────────────────┬───────────────────────────────────────────────┤
│ Local Reference     │ Python/Java variable holding ObjectRef        │
├─────────────────────┼───────────────────────────────────────────────┤
│ Submitted Task Arg  │ Task using this object as argument            │
├─────────────────────┼───────────────────────────────────────────────┤
│ Contained Reference │ Object nested inside another object           │
├─────────────────────┼───────────────────────────────────────────────┤
│ Borrower Reference  │ Object passed to another worker               │
├─────────────────────┼───────────────────────────────────────────────┤
│ Lineage Reference   │ For reconstruction of lost objects            │
└─────────────────────┴───────────────────────────────────────────────┘
```

### Object Resolution Flow

```
                    ┌─────────────────┐
                    │   ray.get(ref)  │
                    └────────┬────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  Check local memory store    │
              │  (for small/inlined objects) │
              └──────────────┬───────────────┘
                             │ Not found
                             ▼
              ┌──────────────────────────────┐
              │  Check local plasma store    │
              └──────────────┬───────────────┘
                             │ Not found
                             ▼
              ┌──────────────────────────────┐
              │  Get object location from    │
              │  owner via RPC               │
              └──────────────┬───────────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌────────────┐   ┌────────────────┐  ┌────────────────┐
    │Pull from   │   │Restore from    │  │Reconstruct via │
    │remote node │   │spilled storage │  │lineage         │
    └────────────┘   └────────────────┘  └────────────────┘
```

### Memory Management & Spilling

When memory pressure is detected:

1. **Eviction**: Objects not pinned by any worker are evicted from plasma
2. **Spilling**: Primary copies are written to external storage (disk/S3)
3. **Restoration**: Spilled objects are restored on demand

```
┌──────────────────────────────────────────────────────────────────┐
│                    Memory Pressure Flow                           │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│   Memory Monitor detects pressure                                 │
│           │                                                       │
│           ▼                                                       │
│   LocalObjectManager::SpillObjectsOfSize()                        │
│           │                                                       │
│           ▼                                                       │
│   Select objects to spill (LRU, not pinned)                       │
│           │                                                       │
│           ▼                                                       │
│   Write to external storage (filesystem/S3)                       │
│           │                                                       │
│           ▼                                                       │
│   Update object directory with spilled URL                        │
│           │                                                       │
│           ▼                                                       │
│   Release plasma memory                                           │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| ReferenceCounter | `core_worker/reference_counter.h` | Distributed ref counting |
| ObjectRecoveryManager | `core_worker/object_recovery_manager.h` | Lineage reconstruction |
| LocalObjectManager | `raylet/local_object_manager.h` | Spilling, pinning |
| ObjectManager | `object_manager/object_manager.h` | Push/pull coordination |
| PullManager | `object_manager/pull_manager.h` | Remote object fetching |
| PlasmaStore | `object_manager/plasma/object_store.h` | Shared memory storage |
| MemoryMonitor | `common/memory_monitor.h` | OOM detection |

---

## Resource Management & Scheduling

### Overview

Ray uses a hierarchical scheduling system: the local Raylet handles per-node scheduling, while the GCS coordinates cluster-wide decisions for actors and placement groups.

### Scheduling Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                       GCS (Cluster-wide)                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │ Actor Scheduler │  │ PG Scheduler    │  │ Resource Manager│   │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
└───────────┼────────────────────┼────────────────────┼─────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌───────────────────────────────────────────────────────────────────┐
│                        Raylet (Per-node)                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              ClusterResourceScheduler                        │ │
│  │  ┌──────────────────┐  ┌──────────────────┐                 │ │
│  │  │ ClusterResourceMgr│  │ LocalResourceMgr │                 │ │
│  │  │ (cluster view)    │  │ (local resources)│                 │ │
│  │  └──────────────────┘  └──────────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              ClusterLeaseManager                             │ │
│  │  (Worker lease requests and assignment)                      │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Scheduling Policies                             │ │
│  │  ┌────────┐ ┌────────┐ ┌──────────┐ ┌────────────┐         │ │
│  │  │ Hybrid │ │ Spread │ │ Affinity │ │ NodeLabel  │         │ │
│  │  └────────┘ └────────┘ └──────────┘ └────────────┘         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
```

### Scheduling Policies

| Policy | File | Description |
|--------|------|-------------|
| **Hybrid** | `hybrid_scheduling_policy.h` | Default. Balances locality and load spreading. Prefers local node, then spreads based on resource utilization. |
| **Spread** | `spread_scheduling_policy.h` | Round-robin across available nodes. Good for load balancing. |
| **NodeAffinity** | `node_affinity_scheduling_policy.h` | Schedule on specific node (soft or hard constraint). |
| **NodeLabel** | `node_label_scheduling_policy.h` | Filter nodes by label key-value pairs. |
| **BundleAffinity** | `affinity_with_bundle_scheduling_policy.h` | Schedule on node with specific placement group bundle. |

### Lease-Based Execution Model

```
CoreWorker                  Raylet                       Worker Pool
    │                          │                              │
    │ RequestWorkerLease       │                              │
    │─────────────────────────►│                              │
    │                          │                              │
    │                          │ Find suitable node           │
    │                          │ (via scheduling policy)      │
    │                          │                              │
    │                          │ GetOrCreateWorker            │
    │                          │─────────────────────────────►│
    │                          │                              │
    │                          │ Worker ready                 │
    │                          │◄─────────────────────────────│
    │                          │                              │
    │ WorkerLeaseReply         │                              │
    │ (worker address)         │                              │
    │◄─────────────────────────│                              │
    │                          │                              │
    │ PushTask to worker       │                              │
    │──────────────────────────────────────────────────────────►
```

---

## Placement Groups

### Overview

Placement groups reserve resources across nodes with specific placement strategies. They use a 2-phase commit protocol for atomicity.

### Placement Strategies

| Strategy | Description |
|----------|-------------|
| **PACK** | Pack bundles on fewest nodes possible |
| **SPREAD** | Spread bundles across different nodes |
| **STRICT_PACK** | All bundles must be on same node |
| **STRICT_SPREAD** | Each bundle on different node (fails if not enough nodes) |

### Placement Group State Machine

```
              ┌──────────────┐
              │   PENDING    │  Waiting in queue
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │  PREPARING   │  2-phase commit: prepare phase
              └──────┬───────┘
                     │
              ┌──────┴──────┐
              │             │
              ▼             ▼
       ┌──────────┐  ┌──────────────┐
       │ PREPARED │  │  RESCHEDULING│──► Back to PENDING
       └────┬─────┘  └──────────────┘
            │
            ▼
       ┌──────────┐
       │  PLACED  │  Resources committed
       └────┬─────┘
            │
            ▼
       ┌──────────┐
       │ REMOVED  │
       └──────────┘
```

### 2-Phase Commit Protocol

```
GCS PG Scheduler              Raylet 1                    Raylet 2
       │                          │                           │
       │ PrepareBundle(bundle1)   │                           │
       │─────────────────────────►│                           │
       │                          │                           │
       │ PrepareBundle(bundle2)   │                           │
       │──────────────────────────────────────────────────────►│
       │                          │                           │
       │ PrepareReply(success)    │                           │
       │◄─────────────────────────│                           │
       │                          │                           │
       │ PrepareReply(success)    │                           │
       │◄──────────────────────────────────────────────────────│
       │                          │                           │
       │ (All prepared - proceed to commit)                   │
       │                          │                           │
       │ CommitBundle(bundle1)    │                           │
       │─────────────────────────►│                           │
       │                          │                           │
       │ CommitBundle(bundle2)    │                           │
       │──────────────────────────────────────────────────────►│
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| GcsPlacementGroupManager | `gcs/gcs_placement_group_manager.h` | PG lifecycle management |
| GcsPlacementGroupScheduler | `gcs/gcs_placement_group_scheduler.h` | Bundle scheduling, 2PC |
| PlacementGroupResourceManager | `raylet/placement_group_resource_manager.h` | Local PG resources |
| BundleSchedulingPolicy | `raylet/scheduling/policy/bundle_scheduling_policy.h` | Bundle placement strategies |

---

## Global Control Service (GCS)

### Overview

The GCS is the centralized control plane for Ray clusters. It manages metadata, coordinates actors and placement groups, and provides fault tolerance.

### GCS Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           GCS Server                                 │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │  NodeManager    │  │  ActorManager   │  │  JobManager     │     │
│  │  - Registration │  │  - Lifecycle    │  │  - Job tracking │     │
│  │  - Health check │  │  - Scheduling   │  │  - Cleanup      │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │ ResourceManager │  │  PGManager      │  │  WorkerManager  │     │
│  │  - Cluster view │  │  - PG lifecycle │  │  - Worker track │     │
│  │  - Autoscaler   │  │  - 2PC commit   │  │  - Failure      │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │   KVManager     │  │  TaskManager    │  │HealthCheckMgr  │     │
│  │  - Metadata     │  │  - Task events  │  │  - Node health  │     │
│  │  - Internal KV  │  │  - Observability│  │  - Failure det  │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     Storage Backend                          │   │
│  │  ┌─────────────────┐              ┌─────────────────┐       │   │
│  │  │  Redis Client   │      OR      │  In-Memory Store│       │   │
│  │  └─────────────────┘              └─────────────────┘       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| GcsServer | `gcs/gcs_server.h` | Main server, orchestrates all managers |
| GcsNodeManager | `gcs/gcs_node_manager.h` | Node registration, cluster membership |
| GcsActorManager | `gcs/gcs_actor_manager.h` | Actor lifecycle across cluster |
| GcsPlacementGroupManager | `gcs/gcs_placement_group_manager.h` | Placement group management |
| GcsResourceManager | `gcs/gcs_resource_manager.h` | Cluster resource aggregation |
| GcsJobManager | `gcs/gcs_job_manager.h` | Job lifecycle, cleanup |
| GcsHealthCheckManager | `gcs/gcs_health_check_manager.h` | Node health monitoring |
| GcsKVManager | `gcs/gcs_kv_manager.h` | Key-value metadata storage |
| RedisStoreClient | `gcs/store_client/redis_store_client.h` | Redis persistence |

---

## Fault Tolerance

### Node Failure Handling

```
┌──────────────────────────────────────────────────────────────────┐
│                    Node Failure Detection                         │
│                                                                   │
│   GcsHealthCheckManager                                           │
│           │                                                       │
│           │ Heartbeat timeout                                     │
│           ▼                                                       │
│   Mark node as DEAD                                               │
│           │                                                       │
│           ├──► GcsActorManager: Restart affected actors           │
│           │                                                       │
│           ├──► GcsPlacementGroupManager: Reschedule PG bundles    │
│           │                                                       │
│           ├──► GcsResourceManager: Remove node resources          │
│           │                                                       │
│           └──► Notify all Raylets to update cluster view          │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Actor Reconstruction

When an actor dies unexpectedly:
1. GCS detects death (via worker failure report or health check)
2. If restarts remaining: state → RESTARTING → PENDING_CREATION
3. GCS schedules actor on new node
4. Callers with pending tasks get notified of new actor address
5. If max restarts exceeded: state → DEAD, notify all callers

### Object Reconstruction (Lineage)

When an object is lost and cannot be fetched:
1. `ObjectRecoveryManager` checks if object is reconstructable
2. If lineage available: re-execute the task that created the object
3. Recursively reconstruct any missing dependencies
4. Store new result in plasma

```
Lost Object                         Task Spec (from lineage)
    │                                        │
    │                                        │
    └──────────► ObjectRecoveryManager ◄─────┘
                        │
                        │ Re-submit task
                        ▼
                   TaskManager
                        │
                        │ Execute
                        ▼
                 New Object Created
```

---

## Appendix: Key Data Structures

### TaskSpecification (`common/task/task_spec.h`)
- Task ID, Job ID, Parent Task ID
- Function descriptor (module, class, function name)
- Arguments (inline or ObjectRef)
- Resource requirements
- Scheduling strategy
- Actor ID (for actor tasks)

### ActorTableData (`protobuf/gcs.proto`)
- Actor ID, Job ID
- Owner address
- State (PENDING, ALIVE, DEAD, etc.)
- Resource requirements
- Restart count, max restarts
- Death cause (if dead)

### ObjectTableData
- Object ID
- Owner address
- Size
- Locations (node IDs where object exists)
- Spilled URL (if spilled to external storage)

### NodeResources (`raylet/scheduling/cluster_resource_manager.h`)
- Total resources (CPU, GPU, memory, custom)
- Available resources
- Load metrics
- Labels
