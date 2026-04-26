# 2026-04-17 - Blocker 6 Listener Integration Review

## Conclusion

Blocker 6 is not fully fixed.

The Rust GCS server now wires several important node-lifecycle integrations that were previously missing:

- node -> resource manager
- node -> autoscaler manager
- node -> actor manager
- node -> placement group manager
- publisher -> long-poll pubsub bridge

Those are real improvements and close part of the parity gap.

However, the specific task-manager lifecycle integrations that exist in the C++ GCS are still missing in Rust. Because of that, Rust is still not behaviorally identical to the C++ GCS in this area and cannot yet be treated as a drop-in replacement.

## Main Finding

### Task-manager lifecycle callbacks are still missing

In C++, `GcsServer::InstallEventListeners()` wires worker-death and job-finished events into the task manager:

- `gcs_task_manager_->OnWorkerDead(worker_id, worker_failure_data)`
- `gcs_task_manager_->OnJobFinished(job_id, job_data.end_time())`

Source:

- [gcs_server.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_server.cc:879)
- [gcs_server.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_server.cc:902)

These callbacks are important because they drive task-state transitions after:

- worker failure
- job completion

That behavior is part of the externally visible semantics of the C++ task-event subsystem.

## What Rust Has Now

The Rust server has added several node-based listeners:

- [lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:342)
- [lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:396)
- [lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:433)
- [lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:459)
- [lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:483)

Specifically, Rust now wires:

- node add/remove/draining -> resource manager
- node add/remove/draining -> autoscaler manager
- node add/remove -> actor manager
- node add/remove -> placement-group manager
- publisher messages -> long-poll pubsub manager

That is good parity progress.

## What Is Still Missing

### 1. Rust server does not install worker-death -> task-manager listener

Rust `GcsWorkerManager` exposes listener support:

- [worker_manager.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:48)

But the Rust server does not attach a task-manager callback to it. I do not see a Rust equivalent of the C++ wiring:

```cpp
gcs_worker_manager_->AddWorkerDeadListener(... {
  ...
  gcs_task_manager_->OnWorkerDead(worker_id, worker_failure_data);
});
```

Source:

- [gcs_server.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_server.cc:880)

### 2. Rust server does not install job-finished -> task-manager listener

Rust `GcsJobManager` also exposes listener support:

- [job_manager.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs:43)

But the Rust server does not register a callback that forwards job-finished events into the task manager. I do not see a Rust equivalent of:

```cpp
gcs_job_manager_->AddJobFinishedListener([this](const rpc::JobTableData &job_data) {
  ...
  gcs_task_manager_->OnJobFinished(job_id, job_data.end_time());
});
```

Source:

- [gcs_server.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_server.cc:903)

### 3. The Rust task manager does not implement the corresponding handlers

This is the clearest remaining blocker in this area.

The Rust task manager file does not define:

- `on_worker_dead`
- `on_job_finished`

I also do not see Rust equivalents of the C++ storage-side failure marking hooks tied to these callbacks:

- `MarkTasksFailedOnWorkerDead`
- `MarkTaskAttemptFailedIfNeeded`

Relevant files:

- Rust task manager: [task_manager.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:1)
- C++ task manager declarations: [gcs_task_manager.h](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_task_manager.h:138)
- C++ task manager implementation: [gcs_task_manager.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_task_manager.cc:729)

Without these handlers, even if the Rust server added the listener wiring, the actual C++ behavior still would not exist.

### 4. Rust also appears to lack the C++ job-manager node-death callback

C++ installs:

```cpp
gcs_job_manager_->OnNodeDead(node_id);
```

on node removal.

Source:

- [gcs_server.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_server.cc:856)

I do not see a Rust `on_node_dead` implementation in the job manager or corresponding server wiring for it.

This is adjacent to the same lifecycle-integration gap and remains another mismatch versus C++.

## Why This Matters

This is not just missing internal cleanup.

In C++, these lifecycle callbacks affect task-event correctness after worker exits and job completion. That can change:

- which task attempts are marked failed
- which failure metadata is attached
- how task history looks after a worker crash
- what clients observe through task-event queries

So this is still a behavioral parity issue, not just a code-organization issue.

## What Looks Fixed or Improved

The changes in this pass are still valuable:

- node lifecycle listeners are installed before initialization, which is the right recovery-sensitive placement
- recovered alive nodes can now populate resource/autoscaler state through the listener path
- actor and placement-group managers now participate in node add/remove lifecycle handling
- publisher traffic is now bridged into the long-poll pubsub manager
- event-export wiring now points at the task manager instead of a do-nothing service stub

These improvements reduce the remaining gap, but they do not close the task-manager listener blocker.

## Recommended Fix

To close Blocker 6, Rust needs all of the following:

1. Add task-manager lifecycle methods that mirror the C++ semantics:

- `on_worker_dead(...)`
- `on_job_finished(...)`

2. Add the corresponding storage logic needed to preserve the C++ behavior on task attempts affected by:

- worker failure
- job completion

3. Register those handlers from the server using the existing Rust listener primitives:

- `worker_manager.add_worker_dead_listener(...)`
- `job_manager.add_job_finished_listener(...)`

4. Add integration tests that prove parity for the triggered behavior, not just that listeners exist:

- worker failure updates task state as expected
- job finish updates task state as expected
- task-event queries reflect those transitions

5. If parity with C++ node-removal job handling is required in the same blocker, add:

- `job_manager.on_node_dead(...)`
- corresponding server wiring on node removal

## Final Status

Blocker 6 should remain open.

The Rust GCS listener surface is broader than before, but the specific C++ task-manager lifecycle integrations that matter for task-event parity are still missing.
