---
myst:
  html_meta:
    description: "Internals of Ray object spilling: primary and secondary copies, reactive and threshold-based triggers, object pinning, and data flow."
---

(object-spilling-internals)=

# Object spilling

This page explains how Ray's object spilling mechanism works and outlines its high-level architecture, components, and end-to-end data flow.


## Overview

Ray stores task outputs and `ray.put()` values as *objects* in the Plasma object store, a shared-memory region on each node. An object goes through the following four lifecycle stages:

1. **Creation**: `ray.put()` or a task return triggers a `Create` RPC to the Plasma store, which allocates space in shared memory for the serialized object.
1. **Pinning**: Once the object is in Plasma, the CoreWorker sends a `PinObjectIDs` RPC to the local raylet. The raylet then holds a reference to the object, which prevents eviction for as long as the object might be needed, for example until it's consumed or spilled.
1. **Consumption**: Other tasks and `ray.get()` calls read the pinned object directly from shared memory through zero-copy access.
1. **Deletion**: When the object owner determines the object is no longer referenced, the raylet unpins it and frees the shared memory.

This lifecycle works well when the working set fits in memory. When the Plasma store is full and new objects need to be created, allocation fails. The failure blocks `ray.put()` and task returns until space is freed.

*Object spilling* solves this problem by adding an external storage tier to the object lifecycle. When Ray detects memory pressure, it automatically *spills* pinned objects from shared memory to external storage, such as local disk or S3. When a spilled object is needed again, Ray transparently *restores* it into Plasma. As a result, the effective object store capacity can exceed physical memory, at the cost of I/O latency when accessing spilled objects.

:::{note}
Object spilling is transparent to your application. Ray spills pinned objects to disk under Plasma store memory pressure without any changes to your application code.
:::


## Architecture

The object spilling architecture is designed to minimize interference with the critical path of task execution. It decouples three stages from each other. The latency-sensitive Plasma store detects memory pressure, the raylet orchestrates spills, and separate worker processes execute the I/O.

The system consists of three main interaction layers:

1. **Detection in the Plasma store thread**: The `CreateRequestQueue` within the Plasma store monitors memory usage. When an allocation fails with an out-of-memory (OOM) error, it triggers a callback to the raylet. This design ensures that I/O operations never block the single-threaded object store.

1. **Orchestration in the raylet main thread**: The `LocalObjectManager` in the raylet receives the spill request. It decides *what* to spill, based on least recently used (LRU) ordering and pinning status. It also decides *when* to spill, batching requests for efficiency. It manages the state of all local objects, which can be Pinned, PendingSpill, or Spilled.

1. **Execution in IO worker processes**: A pool of Python `IO Workers` performs the disk or network I/O. The raylet communicates with these workers through gRPC. This separation keeps the raylet's main loop responsive to other cluster events, such as heartbeats and scheduling, even if I/O is slow, for example when writing to S3.

The following diagram shows this layered architecture and its data flow:

```{image} ../images/object_spilling_architecture.png
:alt: Layered architecture of object spilling, from the Plasma store thread through the raylet main thread to Python IO worker processes that write to external storage
```

%    Mermaid source (generate image from this):
%
%    flowchart TD
%        %% Node Definitions for Parallelism
%        A1["User Application 1"]
%        A2["User Application 2"]
%        B1["CoreWorker 1"]
%        B2["CoreWorker 2"]
%
%        %% Entry point connections
%        A1 -- "ray.put()" --> B1
%        A2 -- "ray.put()" --> B2
%
%        %% Main Logic paths (Parallel)
%        B1 -- "Step 1. Create" --> C["PlasmaStore"]
%        B2 -- "Step 1. Create" --> C
%
%        B1 -- "Step 2. Pin RPC" --> NM["NodeManager"]
%        B2 -- "Step 2. Pin RPC" --> NM
%
%        %% Alignment constraint
%        C ~~~ NM
%
%        %% Left: Memory Allocation Logic
%        subgraph PlasmaThread["Plasma Store Thread"]
%            C --> E["CreateRequestQueue<br/>ProcessRequests()"]
%        end
%
%        %% Right: Scheduling & Management
%        subgraph RayletThread["Raylet Main Thread"]
%            NM -- "PinObjectsAndWaitForFree()" --> F["LocalObjectManager"]
%            F -- "TryToSpillObjects()<br/>→ PopSpillWorker()" --> G["WorkerPool<br/>(IO Worker Pool)"]
%        end
%
%        %% Spilling Link (Cross-thread callback)
%        E -- "OOM: spill_objects_callback()<br/>→ main_service.post()" --> F
%
%        %% Parallel IO Workers
%        subgraph IOWorkerProcesses["Python IO Worker Processes"]
%            H1["Python IO Worker 1"]
%            H2["Python IO Worker 2"]
%            Hn["Python IO Worker N"]
%        end
%        G -- "gRPC" --> H1
%        G -- "gRPC" --> H2
%        G -- "gRPC" --> Hn
%
%        %% Storage Destination
%        H1 & H2 & Hn --> I[("External Storage<br/>(Filesystem / S3)")]
%
%        %% Styling
%        classDef memory fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
%        classDef logic fill:#fff3e0,stroke:#e65100,stroke-width:2px;
%        classDef storage fill:#f1f8e9,stroke:#33691e,stroke-width:2px;
%        classDef core fill:#f3e5f5,stroke:#4a148c,stroke-width:2px;
%
%        class C,E memory;
%        class NM,F,G logic;
%        class H1,H2,Hn,I storage;
%        class A1,A2,B1,B2 core;

:::{note}
The Plasma store and the raylet main event loop run in separate threads. The spill callback bridges them by posting work from the store thread to the main thread. The only cross-thread call is `IsSpillingInProgress()`, which uses `std::atomic`.
:::


(primary-vs-secondary-copies)=

## Primary versus secondary copies

Ray distinguishes between two types of object copies in the cluster, and the type determines how Ray handles a copy under memory pressure:

- **Primary copy**: The initial copy of an object, created by a task or `ray.put`. The owner of the object, which is the CoreWorker that created it, manages its lifetime. The primary copy is the source of truth and can't be evicted. If memory is needed, Ray must spill it to external storage.
- **Secondary copy**: A copy of an object transferred to another node, for example as a dependency for a remote task or through `ray.get`. Ray treats these copies as cached replicas.


In the context of spilling, "primary copy" and "pinned object" are closely related but distinct concepts:

- A primary copy is the initial object that the owner creates. The owner explicitly registers it with the raylet's [LocalObjectManager](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L31) through `PinObjectIDs`, which makes it a *pinned object* that's eligible for spilling.
- A secondary copy, or cached replica, is [pinned in the Plasma store only *while actively referenced*](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/eviction_policy.cc#L136), for example by a running task or a worker. The LocalObjectManager doesn't manage it, and [the Plasma store's LRU policy](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/eviction_policy.cc#L82) evicts it once the reference count drops.

Therefore, the raylet's spilling mechanism only sees and operates on primary copies.

When memory pressure builds up as objects are created in or moved into the Plasma store, Ray prioritizes evicting secondary copies to free space, because they can be re-fetched from the primary copy. If memory pressure persists, Ray then resorts to spilling primary copies to external storage.


## Triggering spilling

Any operation that adds objects to the Plasma store can trigger object spilling. At the user API level, these operations include the following:

- `ray.put(obj)`: Explicitly places an object into the object store.
- **Task return values**: The return value of a remote task is serialized and stored in Plasma.
- **Object transfer**: When `ray.get()` fetches a remote object, the object is copied into the local Plasma store on the receiving node.

Internally, three code paths trigger spilling. The first is *reactive*, triggering spilling because allocation has already failed. The other two are *proactive*. They check a memory threshold and spill preemptively to avoid OOM in the first place.

```{list-table}
:widths: 15 30 55
:header-rows: 1

* - Trigger
  - When it fires
  - Condition
* - **OOM on Create**
  - A `Create` RPC to Plasma [fails with `OutOfMemory`](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/create_request_queue.cc#L117).
  - Reactive. Allocation already failed, so Ray must spill to make room.
* - **Periodic threshold**
  - [Every `free_objects_period_milliseconds`](https://github.com/ray-project/ray/blob/master/src/ray/raylet/node_manager.cc#L427), which defaults to 1000 ms.
  - Proactive. Primary object bytes divided by capacity is at least `object_spilling_threshold`, which defaults to 0.8.
* - **Object sealed**
  - Whenever a new object is sealed in Plasma, through [SealObjects](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/store.cc#L278) and then [HandleObjectLocal](https://github.com/ray-project/ray/blob/master/src/ray/raylet/node_manager.cc#L2451).
  - Proactive. The same threshold check as the preceding row, triggered immediately on the new object.
```

All three paths converge on `LocalObjectManager::SpillObjectUptoMaxThroughput()`.

### Reactive: OOM on object creation

When the Plasma store can't allocate space for a new object, the [CreateRequestQueue](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/create_request_queue.h#L34) manages the queued request and starts a recovery sequence. The key decision logic lives in [ProcessRequests](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/create_request_queue.cc#L85):

1. Try to allocate the object in shared memory.
1. If allocation fails with `OutOfMemory` and `FileSystemMonitor` reports that the disk is full, return `OutOfDisk` immediately. There's nowhere to spill to.
1. Trigger global garbage collection (GC) if configured. GC might free Python-side references so that Plasma objects can be unpinned.
1. Call `spill_objects_callback_()`. [main.cc](https://github.com/ray-project/ray/blob/master/src/ray/raylet/main.cc#L752) registers this callback, and it runs on the Plasma store thread. It does two things:

   ```cpp
   /*spill_objects_callback=*/
   [&]() {
     // 1) Post spill task to Raylet main thread (non-blocking, enqueue only)
     main_service.post(
         [&]() { local_object_manager->SpillObjectUptoMaxThroughput(); },
         "NodeManager.SpillObjects");
     // 2) Return whether spilling is active (std::atomic, safe to read cross-thread)
     return local_object_manager->IsSpillingInProgress();
   }
   ```

   Based on the return value, `CreateRequestQueue` decides what to do next:

   - `true` means spilling is in progress. The LocalObjectManager has identified eligible pinned primary copies, and spill workers are actively writing them to external storage. An eligible copy is an object with a reference count of 1, meaning only the owner holds a reference and no task is actively using it. See [PlasmaStore::IsObjectSpillable](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/store.cc#L560).
   - `false` means the LocalObjectManager has no ongoing spills. This result occurs in any of the following cases:

     - No eligible objects were found, for example because running tasks are using all pinned objects.
     - The total size of spillable objects is below `min_spilling_size`, which is too small to justify an immediate spill, so Ray waits to batch more objects.
     - Spilling is disabled in the configuration.

     In this case, the queue enters the *grace period*, which `oom_grace_period_s` sets. Retries continue during the grace period to account for global GC latency and for the delay between spilling completing and space being freed in the object store.

1. If the grace period expires without progress, try the fallback allocator as a last resort. The fallback allocator uses `mmap` to allocate the object directly on the local filesystem instead of shared memory. This path is slower but avoids blocking the caller indefinitely. If it also fails, for example because the disk is full, return `OutOfDisk`.

The Plasma store retries `ProcessCreateRequests()` periodically, at an interval that `delay_on_oom_ms` controls, as long as the queue is non-empty and the status isn't OK. See [PlasmaStore::ProcessCreateRequests](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/store.cc#L508).

### Proactive: Threshold-based spilling

The reactive OOM path fires only *after* the store is already full. To avoid hitting that cliff, Ray also spills objects proactively before the store is full. [NodeManager::SpillIfOverPrimaryObjectsThreshold](https://github.com/ray-project/ray/blob/master/src/ray/raylet/node_manager.cc#L2400) checks whether the fraction of primary object bytes in the store exceeds `object_spilling_threshold`, which defaults to 0.8. If it does, it calls `SpillObjectUptoMaxThroughput()`.

Two places invoke this check:

1. **Periodic timer**: A timer in [node_manager.cc](https://github.com/ray-project/ray/blob/master/src/ray/raylet/node_manager.cc#L427) runs the check every `free_objects_period_milliseconds`, which defaults to 1000 ms. This timer is the steady-state proactive spilling path. Even without any new object creation, the system periodically checks and spills if needed.

1. **Object sealed event**: Every time [SealObjects](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/plasma/store.cc#L278) seals a new object in Plasma, the `add_object_callback_` posts `HandleObjectLocal()` to the raylet main thread. [HandleObjectLocal](https://github.com/ray-project/ray/blob/master/src/ray/raylet/node_manager.cc#L2451) calls `SpillIfOverPrimaryObjectsThreshold()` at the end. As a result, spilling reacts immediately when a large object pushes memory usage over the threshold, rather than waiting up to 1 second for the next periodic check.


## Alternatives to spilling

Aside from spilling, Ray includes the following mechanisms to handle memory pressure:

1. **Eviction of secondary copies**: As described earlier, Ray creates replicas of objects on other nodes when tasks or `ray.get` need them. These secondary copies are evictable. When the object store is full, Ray deletes these copies in LRU order to free space before attempting to spill primary objects.

1. **Fallback allocation**: If spilling is too slow or the object store is fragmented, Ray might use fallback allocation. Fallback allocation happens when a create request fails with OOM even after a spill attempt. Ray allocates the object directly on the filesystem with `mmap` rather than in the shared memory pool. This approach avoids application deadlock but performs worse than shared memory.


## Object pinning

Before Ray can spill objects, the raylet must *pin* them. Pinning makes the raylet hold a reference to the object so that it isn't prematurely evicted from the object store.

When the CoreWorker creates an object in Plasma, it sends a `PinObjectIDs` RPC to the raylet. The raylet's [HandlePinObjectIDs](https://github.com/ray-project/ray/blob/master/src/ray/raylet/node_manager.cc#L2588) fetches the objects from Plasma and calls [PinObjectsAndWaitForFree](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L31), which does the following:

1. Stores object metadata in `local_objects_`. The metadata is the owner address, generator ID, and size.
1. Holds the `std::unique_ptr<RayObject>` in `pinned_objects_`, preventing Plasma eviction.
1. Subscribes to eviction notifications through pub/sub. When the object owner says the object can be freed, or the owner process dies, `ReleaseFreedObject()` is called.

Every object that `LocalObjectManager` tracks has an entry in the `local_objects_` metadata map and, at the same time, in exactly one of three sub-maps that corresponds to its current state:

```{list-table}
:widths: 20 30 50
:header-rows: 1

* - State
  - Sub-map
  - Meaning
* - **Pinned**
  - `pinned_objects_`
  - Object is held in shared memory, eligible for spilling.
* - **PendingSpill**
  - `objects_pending_spill_`
  - Object has been handed to an IO worker, spill in progress.
* - **Spilled**
  - `spilled_objects_url_`
  - Object has been written to external storage, in-memory copy released.
```

When the owner frees an object, `LocalObjectManager` deletes it by removing it from `local_objects_` and from its corresponding sub-map, and stops tracking it.

```{image} ../images/object_spilling_states.png
:alt: State diagram of a tracked object's transitions between the Pinned, PendingSpill, and Spilled states
```

%    Mermaid source (generate image from this):
%
%    stateDiagram-v2
%        [*] --> Pinned : PinObjectsAndWaitForFree()
%        Pinned --> PendingSpill : SpillObjectsInternal()
%        PendingSpill --> Spilled : OnObjectSpilled()
%        PendingSpill --> Pinned : Spill failed (rollback)
%
%        Pinned --> [*] : ReleaseFreedObject()<br/>(unpin, remove from local_objects_)
%        PendingSpill --> [*] : ReleaseFreedObject()<br/>(deferred to spill completion)
%        Spilled --> [*] : ProcessSpilledObjectsDeleteQueue()<br/>(decrement url_ref_count)


## Spill scheduling

The [LocalObjectManager](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.h#L46) orchestrates all spill operations.

### Strategy: Optimistic batching

The core tension in spill scheduling is between latency and efficiency:

- Spilling should start as soon as possible when memory is under pressure, because a delay risks blocking object creation.
- Fusing multiple objects into a single spill file amortizes I/O overhead through fewer syscalls and sequential writes, so larger batches are more efficient.

Ray resolves this tension with an *optimistic batching* strategy. It spills immediately with whatever objects are available, but defers tiny batches when other spills are already in flight. Ray uses this strategy for the following reasons:

- In-flight spills are about to free memory, which reduces the urgency.
- Waiting gives more objects time to accumulate, which improves the next batch's efficiency.
- When no spills are in progress, Ray dispatches even a small batch immediately, because there's nothing to wait for.

The entry point is [SpillObjectUptoMaxThroughput](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L169), which runs when memory pressure is detected. It aggressively tries to saturate all available IO workers by calling `TryToSpillObjects()` in a loop until either no more objects can be spilled or all workers are busy, which is when `num_active_workers_ >= max_active_workers_`.

### Batch construction

[TryToSpillObjects](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L186) constructs a single spill batch. It iterates through `pinned_objects_` and skips objects that aren't currently spillable. `is_plasma_object_spillable_` checks that a worker process isn't actively using the object. `TryToSpillObjects` accumulates candidates until it reaches one of the following limits:

- It has collected `max_fused_object_count_` objects.
- Adding the next object would exceed `max_spilling_file_size_bytes_`. This limit applies only when it's enabled, meaning greater than 0. The first object is always included, even if it alone exceeds the limit.
- It has checked all pinned objects.

### Deferral decision

After constructing the candidate batch, `TryToSpillObjects` decides whether to spill now or defer. It defers spilling and returns `false` when [all three of the following conditions](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L220) hold at the same time:

1. The scan visited all pinned objects without hitting `max_fused_object_count_`. The candidate batch is small and not limited by fusion constraints.
1. The total bytes to spill, `bytes_to_spill`, is below `min_spilling_size_`.
1. Some objects are already being spilled, which means `objects_pending_spill_` is non-empty.

In other words, the batch is small, it's below the minimum threshold, and other spills are already making progress, so waiting is safe. If any condition isn't met, spilling proceeds immediately. That happens when the batch hit a fusion limit, which indicates enough objects to justify a spill, when the batch is large enough, or when no other spills are in progress.

Once the decision is to spill, `SpillObjectsInternal()` is called with the selected batch.

### SpillObjectsInternal

[SpillObjectsInternal](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L282) performs the spill in the following steps:

1. Filters out objects that have already been freed or are already pending spill.
1. Moves objects from `pinned_objects_` to `objects_pending_spill_` and updates size counters.
1. Increments `num_active_workers_` and pops a spill worker from the IO worker pool.
1. Constructs a `SpillObjectsRequest` RPC with object refs and owner addresses, then sends it to the IO worker.
1. On the RPC response, moves failed objects back to `pinned_objects_` and calls `OnObjectSpilled()` for successful ones.

:::{note}
Spilling is ordered. If object N succeeds, all objects before N in the request are guaranteed to have succeeded as well. Failed objects, from the first failure onward, are moved back to pinned state.
:::


## Python IO workers and external storage

IO workers are specialized Python processes that perform the I/O operations. The [WorkerPool](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.h#L280) spawns and manages them.

The `WorkerPool` manages all worker processes on a node, including regular task workers, driver processes, and IO workers. It tracks regular workers and IO workers in separate data structures. Regular task workers go into the `State::idle` set, while IO workers have their own dedicated [`IOWorkerState`](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.h#L619). Each `IOWorkerState` maintains an `idle_io_workers` set, a `pending_io_tasks` queue, and a `started_io_workers` count. This separation ensures that IO workers are never assigned regular tasks, and vice versa.

### Worker types and pool management

IO workers handle three types of operations:

- **Spill**: Write objects from Plasma to external storage.
- **Restore**: Read a previously spilled object from external storage back into Plasma.
- **Delete**: Remove spill files from external storage when all objects within them have gone out of scope.

Each operation type maps to an `IOWorkerState`:

- `SPILL_WORKER`: A dedicated pool, [spill_io_worker_state](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.h#L658), that handles `SpillObjects` RPCs.
- `RESTORE_WORKER`: A dedicated pool, [restore_io_worker_state](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.h#L660), that handles `RestoreSpilledObjects` RPCs.
- Delete operations don't have a dedicated pool. [PopDeleteWorker](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.cc#L1063) compares the number of idle workers in the spill and restore pools and borrows a worker from whichever pool has more idle workers. After the delete completes, the worker is returned to its original pool.

When [PopSpillWorker](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.cc#L990) or `PopRestoreWorker` is called, one of two things happens:

- If an idle IO worker of the matching type is available in its `idle_io_workers` set, it's returned immediately through the callback.
- If no idle IO workers exist, the callback is queued in `pending_io_tasks`, and [TryStartIOWorkers](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.cc#L1750) spawns new Python IO worker processes, up to `max_io_workers` per type.

When the operation completes, the worker is returned to its pool through `PushSpillWorker` or `PushRestoreWorker`. If pending tasks are queued, the worker is immediately assigned to the next task instead of going idle.

### Object fusion format

Rather than writing each object to its own file, Ray *fuses* multiple objects from a single spill batch into a single file. Fusion reduces I/O overhead, because it needs fewer `open` and `close` syscalls, and it reduces filesystem fragmentation. The [_write_multiple_objects](https://github.com/ray-project/ray/blob/master/python/ray/_private/external_storage.py#L133) method writes objects sequentially, each prefixed with a 24-byte header:

```text
┌──────────────────────────────────────────────────────┐
│                      Spill File                      │
│                                                      │
│  Object 1:                                           │
│  ┌──────────┬──────────────┬──────────┐              │
│  │ addr_len │ metadata_len │ buf_len  │  (24 bytes)  │
│  │ (8 bytes)│ (8 bytes)    │ (8 bytes)│              │
│  ├──────────┴──────────────┴──────────┤              │
│  │ owner_address │ metadata │ buffer  │              │
│  └───────────────┴──────────┴─────────┘              │
│                                                      │
│  Object 2: [same format...]                          │
│  ...                                                 │
└──────────────────────────────────────────────────────┘
```

The header's three 8-byte fields, `addr_len`, `metadata_len`, and `buf_len`, encode the sizes of the three variable-length sections that follow. The first section is the serialized owner address, which `ReportObjectSpilled` needs. The second is the object metadata, and the third is the object data buffer. During restore, the IO worker reads this header first to determine how many bytes to read for each section.

Because multiple objects share a single file, each object needs to be independently addressable. For example, object 3 in a fused file might be restored while objects 1 and 2 are still alive. Ray solves this problem with a *spill URL* that encodes the object's position within the file:

```text
/tmp/ray/spill/ray_spilled_objects_<node_id>/<uuid>-multi-<count>?offset=<N>&size=<M>
```

The URL has two parts:

- **Base URL**: The path before `?`, which identifies the spill file. It's the same for all objects fused into the same file. It's also the key that `url_ref_count_` uses to track how many live objects reference the file. The file is deleted only when this count reaches zero.
- **Query parameters**: `offset` is the byte offset of this specific object within the file, and `size` is its total size, including the header and data. The restore IO worker seeks to the offset and reads exactly `size` bytes.

This URL is stored in `spilled_objects_url_` on the spilling node and reported to the object directory through `ReportObjectSpilled()`. Any node in the cluster that needs to restore the object can then discover it.

### Storage backends

The [ExternalStorage](https://github.com/ray-project/ray/blob/master/python/ray/_private/external_storage.py#L72) abstract class defines the interface for all backends. Ray provides two production implementations:

- [FileSystemStorage](https://github.com/ray-project/ray/blob/master/python/ray/_private/external_storage.py#L271), the default, writes to the local filesystem. It supports multiple directories with round-robin distribution for I/O parallelism across mount points. It names files `{directory}/ray_spilled_objects_{node_id}/{uuid}-multi-{count}`.

- [ExternalStorageSmartOpenImpl](https://github.com/ray-project/ray/blob/master/python/ray/_private/external_storage.py#L398) uses the `smart_open` library for cloud storage, such as S3 and GCS. It reuses boto3 sessions and uses deferred seek for performance.

[setup_external_storage](https://github.com/ray-project/ray/blob/master/python/ray/_private/external_storage.py#L577) selects the backend based on the `object_spilling_config` JSON configuration.


## Post-spill processing

After the IO worker successfully writes objects to external storage, [OnObjectSpilled](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L399) is called for each spilled object. It does the following:

1. Parses the returned URL to extract the `base_url`, which is the path without the offset and size query parameters.
1. Increments `url_ref_count_[base_url]`. Because multiple objects can be fused into one file, this ref count tracks how many live objects reference each file.
1. Records the `object_id → url_with_offset` mapping in `spilled_objects_url_`.
1. Removes the object from `objects_pending_spill_`, which releases the in-memory copy.
1. Updates spill metrics, such as `spilled_bytes_total_` and `spilled_objects_total_`.
1. If the object hasn't already been freed, reports the spilled URL to the object owner through `object_directory_->ReportObjectSpilled()` so that other nodes in the cluster can locate the spilled object.


## Object restore

When a spilled object is needed again, Ray must restore it into the Plasma store, or stream it directly over the network, before it can be used. A node triggers a restore whenever it determines that a required object isn't available in any node's in-memory Plasma store but has a known spilled URL.

### Triggering restore

At the user API level, the following operations can trigger a restore if the referenced object has been spilled:

- `ray.get(ref)`: A blocking get on a spilled object.
- `ray.wait(refs)`: Waiting for spilled objects to become available.
- **Task scheduling**: A task's input arguments are spilled, and the scheduler must restore them before the task can run.
- **Actor task arguments**: An actor receives a task whose arguments are spilled.
- `await ref`: An async Python get on a spilled object.

All of these operations go through the same internal path. The raylet's [LeaseDependencyManager](https://github.com/ray-project/ray/blob/master/src/ray/raylet/lease_dependency_manager.cc) issues an `ObjectManager::Pull` request for the missing object. The [PullManager](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/pull_manager.cc#L446) then consults the object directory for the object's location. If the object has been spilled, the directory returns the spilled URL that `OnObjectSpilled` reported earlier through `ReportObjectSpilled`. The `PullManager` then decides how to restore the object based on the storage backend.

A periodic retry timer, [ObjectManager::Tick](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/object_manager.cc#L828), also re-evaluates all active pull requests and retries restores that previously failed.

### Two restore paths

The restore path depends on the storage backend:

- **Filesystem storage**: The object is spilled to local disk, so the spill file exists only on the node that spilled it. If the requesting node is the same node, it restores the object locally through [AsyncRestoreSpilledObject](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L464). If the requesting node is a *different* node, it sends a pull request to the spilling node. The spilling node reads the object directly from disk and streams it over the network through [PushFromFilesystem](https://github.com/ray-project/ray/blob/master/src/ray/object_manager/object_manager.cc#L409), without restoring the object into its own Plasma store. This approach avoids unnecessary memory pressure on the spilling node.

- **Cloud storage**: The object is spilled to a service such as S3 or GCS, so the spill file is accessible from any node. The requesting node restores the object locally through `AsyncRestoreSpilledObject`, using the cloud URL directly. No cross-node RPC is needed.

### Restore mechanics

[AsyncRestoreSpilledObject](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L464) performs the local restore:

1. **Deduplication**: If the same object is already being restored, meaning `objects_pending_restore_` contains the ID, the call is a no-op to avoid duplicate restores.
1. Pops a restore worker from the IO worker pool.
1. Sends a `RestoreSpilledObjectsRequest` RPC with the spilled URL and object ID.
1. The Python IO worker reads the file at the specified offset, parses the 24-byte header, and puts the object back into the Plasma store through `core_worker.put_file_like_object()`.
1. On completion, updates restore metrics and invokes the callback.


## Object deletion and cleanup

Object deletion is a two-phase process. It handles objects that are freed while they're still being spilled, and multiple objects that share a single spill file.

### Phase 1: Marking objects as freed

When the object owner frees the object through a pub/sub eviction notification, or when the owner dies, [ReleaseFreedObject](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L111) is called. It does the following:

1. Marks `local_objects_[id].is_freed_ = true`.
1. If the object is pinned, removes it from `pinned_objects_` and erases the `local_objects_` entry immediately.
1. If the object is being spilled or already spilled, pushes it onto `spilled_object_pending_delete_` for deferred cleanup. Deletion can't happen while spilling is in progress.
1. Adds the object ID to `objects_pending_deletion_` for batch eviction from Plasma across the cluster.

### Phase 2: Batch cleanup of spilled files

[ProcessSpilledObjectsDeleteQueue](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L523) drains the `spilled_object_pending_delete_` queue up to a batch size limit. For each object, it does the following:

1. If the object is still being spilled, meaning `objects_pending_spill_` contains the ID, break out of the loop entirely. The queue is first in, first out (FIFO), so no subsequent entries are processed either. The object isn't removed from the queue. It remains at the front and is retried the next time the function is called. This behavior is a deliberate simplification. Deletion is low priority compared to spilling, and the spill eventually completes, at which point the next call makes progress. No data structures are modified for this object.

1. If the object has a spilled URL in `spilled_objects_url_`, parse the URL to extract the `base_url` and decrement `url_ref_count_[base_url]`. The `base_url` is the path without the offset and size query parameters. If the ref count reaches zero, add the URL to the list of files to delete and remove the ref count entry. Remove the object from `spilled_objects_url_` and `local_objects_`.

1. If the object doesn't have a spilled URL, remove it from `pinned_objects_`, if present, and from `local_objects_` to prevent a memory leak. This case happens when the object was freed while still being spilled, and the spill has since either completed without recording a URL for it or been rolled back.

:::{note}
Ray doesn't maintain a dedicated pool of workers for deleting spilled objects. Instead, deletion tasks borrow an idle worker from either the spill or the restore worker pool. To minimize impact on critical-path operations, the `WorkerPool` dynamically selects a worker from the pool with more idle capacity. See [WorkerPool::PopDeleteWorker](https://github.com/ray-project/ray/blob/master/src/ray/raylet/worker_pool.cc#L1071). Once the delete operation completes, the worker is returned to its original pool.
:::

After processing the queue, if any files have had their ref counts drop to zero, [DeleteSpilledObjects](https://github.com/ray-project/ray/blob/master/src/ray/raylet/local_object_manager.cc#L579) is called. This function pops a delete worker from the IO worker pool and sends a `DeleteSpilledObjectsRequest` RPC containing the list of URLs to delete. The delete worker receives the full list of file URLs and deletes each one. For filesystem storage, each deletion is an [`os.remove(path)` call per file](https://github.com/ray-project/ray/blob/master/python/ray/_private/external_storage.py#L364). The C++ side has already decided *which* files to delete through ref counting, and the Python IO worker unconditionally deletes every URL it receives. If the RPC fails, for example because the worker crashes, the entire batch is retried up to 3 times.

:::{note}
The URL ref counting mechanism is critical for correctness. Because multiple objects can be fused into a single file, the file must not be deleted until all objects within it have gone out of scope.
:::


## Complete lifecycle

The following sequence diagrams show the end-to-end interactions between components during spill, restore, and delete.

The following diagram shows the spill path, which memory pressure triggers:

```{image} ../images/object_spilling_spill_sequence.png
:alt: Sequence diagram of the spill path
```

%    Mermaid source (generate image from this):
%
%    sequenceDiagram
%        participant App as User Application
%        participant CW as CoreWorker
%        participant PS as PlasmaStore<br/>(store thread)
%        participant CRQ as CreateRequestQueue
%        participant LOM as LocalObjectManager<br/>(main thread)
%        participant WP as WorkerPool
%        participant IO as Python IO Worker
%        participant FS as External Storage
%
%        App->>CW: ray.put(obj)
%        CW->>PS: Create(object_id, size)
%        PS->>CRQ: ProcessRequests()
%
%        alt Space available
%            CRQ-->>PS: OK
%            PS-->>CW: PlasmaObject
%        else OutOfMemory
%            CRQ->>CRQ: trigger_global_gc_()
%            CRQ->>LOM: spill_objects_callback_()<br/>[post to main_service]
%            LOM->>LOM: SpillObjectUptoMaxThroughput()
%            LOM->>LOM: TryToSpillObjects()<br/>[batch by size/count]
%            LOM->>LOM: SpillObjectsInternal()<br/>[pinned → pending_spill]
%            LOM->>WP: PopSpillWorker()
%            WP-->>LOM: io_worker
%            LOM->>IO: SpillObjects RPC<br/>[object_refs + owner_addrs]
%            IO->>FS: Write fused objects to file
%            FS-->>IO: file path
%            IO-->>LOM: spilled_objects_urls
%            LOM->>LOM: OnObjectSpilled()<br/>[pending_spill → spilled_url]<br/>[update url_ref_count]
%            LOM->>CW: ReportObjectSpilled()<br/>[notify owner]
%            LOM-->>CRQ: IsSpillingInProgress() = true
%            Note over CRQ: Retry ProcessRequests()<br/>after delay_on_oom_ms
%        end

The following diagram shows the restore path, which runs when a spilled object is needed again:

```{image} ../images/object_spilling_restore_sequence.png
:alt: Sequence diagram of the restore path
```

%    Mermaid source (generate image from this):
%
%    sequenceDiagram
%        participant Task as Task / ray.get()
%        participant OM as ObjectManager
%        participant OD as ObjectDirectory
%        participant LOM as LocalObjectManager<br/>(main thread)
%        participant WP as WorkerPool
%        participant IO as Python IO Worker
%        participant FS as External Storage
%
%        Task->>OM: Request object
%        OM->>OD: Lookup object location
%        OD-->>OM: spilled_url
%        OM->>LOM: AsyncRestoreSpilledObject()<br/>[object_id, url]
%        LOM->>LOM: Dedup check<br/>[objects_pending_restore_]
%        LOM->>WP: PopRestoreWorker()
%        WP-->>LOM: io_worker
%        LOM->>IO: RestoreSpilledObjects RPC
%        IO->>FS: Read file at offset
%        IO->>IO: Parse header (24 bytes)<br/>[addr_len, metadata_len, buf_len]
%        IO->>IO: put_file_like_object()<br/>[back into Plasma]
%        IO-->>LOM: bytes_restored_total
%        LOM-->>Task: Object available in Plasma

The following diagram shows the delete path, which runs when an object goes out of scope:

```{image} ../images/object_spilling_delete_sequence.png
:alt: Sequence diagram of the delete path
```

%    Mermaid source (generate image from this):
%
%    sequenceDiagram
%        participant Owner as Object Owner
%        participant LOM as LocalObjectManager<br/>(main thread)
%        participant WP as WorkerPool
%        participant IO as Python IO Worker
%        participant FS as External Storage
%
%        Owner->>LOM: PubSub: object eviction<br/>(or owner death)
%        LOM->>LOM: ReleaseFreedObject()<br/>[is_freed_ = true]
%
%        alt Object is PINNED
%            LOM->>LOM: Unpin immediately<br/>[remove from pinned_objects_]
%        else Object is SPILLED / PENDING_SPILL
%            LOM->>LOM: Push to<br/>spilled_object_pending_delete_
%        end
%
%        LOM->>LOM: Batch: objects_pending_deletion_
%        LOM->>LOM: FlushFreeObjects()
%
%        Note over LOM: ProcessSpilledObjectsDeleteQueue()
%        LOM->>LOM: url_ref_count_[base_url] -= 1
%        alt ref_count == 0
%            LOM->>WP: PopDeleteWorker()
%            WP-->>LOM: io_worker
%            LOM->>IO: DeleteSpilledObjects RPC
%            IO->>FS: os.remove(file)<br/>[retry up to 3x on failure]
%        else ref_count > 0
%            Note over LOM: File still has live objects,<br/>skip deletion
%        end


## Configuration

The following configuration parameters control object spilling:

- `object_spilling_config`: JSON string specifying the storage backend. Empty string disables spilling.
- `object_spilling_threshold`: Fraction of available object store memory, from 0.0 to 1.0, at which spilling begins. Default: `0.8`.
- `min_spilling_size`: Minimum bytes to accumulate before triggering a spill batch.
- `max_spilling_file_size_bytes`: Maximum bytes allowed in a single fused spill file. The limit is enabled when the value is greater than 0. When enabled, `TryToSpillObjects` stops fusing objects once adding the next object would exceed this limit, though the first object is always included. When enabled, the value must be at least `min_spilling_size`. The default, `-1`, disables the limit.
- `max_fused_object_count`: Maximum number of objects fused into a single spill file. Default: `2000`.
- `max_io_workers`: Maximum number of concurrent spill or restore IO worker processes.
- `oom_grace_period_s`: Seconds to wait after OOM before using the fallback allocator.
- `free_objects_batch_size`: Number of freed objects to batch before flushing.
- `free_objects_period_milliseconds`: Interval for flushing freed objects.
- `verbose_spill_logs`: Byte threshold, with exponential backoff, for error-level spill log messages.


The following example sets a spilling configuration:

```python
import json
import ray

ray.init(
    _system_config={
        "object_spilling_config": json.dumps({
            "type": "filesystem",
            "params": {
                "directory_path": ["/mnt/ssd1/spill", "/mnt/ssd2/spill"],
                "buffer_size": 1048576,
            }
        }),
        "min_spilling_size": 100 * 1024 * 1024,  # 100 MB
        "max_spilling_file_size_bytes": 1024 * 1024 * 1024,  # 1 GB cap per file
        "max_io_workers": 4,
    }
)
```

## Key source files

```{list-table}
:widths: 40 60
:header-rows: 1

* - File
  - Role
* - `src/ray/object_manager/plasma/create_request_queue.cc`
  - Decides when to trigger spilling on OOM.
* - `src/ray/object_manager/plasma/store.cc`
  - Plasma store, which retries create requests periodically.
* - `src/ray/raylet/main.cc`
  - Wires up the spill callback between Plasma and LocalObjectManager.
* - `src/ray/raylet/node_manager.cc`
  - Handles `PinObjectIDs` RPC and integrates LocalObjectManager.
* - `src/ray/raylet/local_object_manager.h`
  - Class definition, state tracking, and member variables.
* - `src/ray/raylet/local_object_manager.cc`
  - Orchestration logic for spill, restore, and delete.
* - `src/ray/raylet/worker_pool.cc`
  - IO worker pool management, including popping, pushing, and starting workers.
* - `python/ray/_private/external_storage.py`
  - The FileSystemStorage and SmartOpenImpl storage backends.
```
