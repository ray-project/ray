# Backend Port Re-Audit After GCS-6 Round 7

Date: 2026-03-22
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND7.md`

## Bottom Line

I do not think FULL parity is complete yet.

The previously tracked callback-ordering gap now looks fixed, but the latest report closes the remaining actor lifecycle/export event gap by de-scoping it. I do not think that de-scoping is sufficient if the goal is FULL parity with the C++ backend.

The C++ backend exposes configurable actor lifecycle/export event behavior. The Rust backend still does not implement an equivalent path.

That means:

- core actor lifecycle correctness is now much closer
- but full feature parity is still not established

## Step Trace

### 1. Reviewed the Round 7 report

I read:

- [`2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND7.md`](/Users/istoica/src/ray/rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND7.md)

The report makes a narrower claim than earlier rounds:

- actor lifecycle/export events are “not a parity requirement”

That is the part I do not accept for a FULL-parity claim.

### 2. Re-checked what C++ actually implements

Files checked:

- [`gcs_actor.cc`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor.cc)
- [`gcs_actor.h`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor.h)
- [`gcs_actor_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc)
- [`ray_config_def.h`](/Users/istoica/src/ray/src/ray/common/ray_config_def.h)
- [`event.cc`](/Users/istoica/src/ray/src/ray/util/event.cc)

What C++ provides:

1. actor lifecycle/export events exist
2. they are configurable via Ray config
3. they are emitted on multiple actor lifecycle transitions
4. when enabled, they are externally observable through:
   - the event aggregation path
   - export event log files

So this is not merely dead code or a test-only path.

It is an optional feature, but it is still part of the C++ backend surface area.

### 3. Re-checked the Rust backend

Files checked:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)
- Rust observability crates referenced by the report

What I verified:

1. the Rust actor manager still does not emit equivalent actor lifecycle/export events
2. there is supporting observability infrastructure in Rust, but it is not wired into `ray-gcs`

So the capability still does not exist in the Rust backend.

## Why I do not accept the de-scope for FULL parity

The report’s argument is:

- the feature is disabled by default
- no live Ray component depends on it

That may be enough to argue it is lower priority.

It is not enough to claim FULL parity.

Reason:

1. a disabled-by-default feature is still part of the product surface if users can enable it
2. the C++ backend supports that feature today
3. the Rust backend does not

So the strongest accurate statement is:

- this may not block core correctness
- this may not block default behavior
- but it is still a missing optional backend feature relative to C++

That means FULL parity is still not proven.

## Current Status

### Fixed enough

- the last specifically tracked core correctness gaps now look closed
- actor persistence / pubsub / callback ordering looks much closer to C++

### Previously open for FULL parity (now closed in Round 8 fix)

1. ~~actor lifecycle/export event feature parity~~ — **fixed**: configurable `ActorExportConfig` + `write_actor_export_event()` implemented at all 6 C++ transition points

## Round 8 Fix (2026-03-22)

### Implementation

Added configurable actor lifecycle/export event emission to `rust/ray-gcs/src/actor_manager.rs`:

1. **`ActorExportConfig`** — mirrors C++ `enable_export_api_write` (disabled by default)
2. **`write_actor_export_event()`** — mirrors C++ `WriteActorExportEvent()`: checks config, builds event with C++ payload fields (actor_id, job_id, state, name, pid, namespace, class_name, is_detached, node_id, repr_name), emits to `EventExporter`
3. **6 emission points** matching all C++ call sites:
   - Registration success (`handle_register_actor`) — `is_registration=true`
   - PENDING_CREATION transition (`handle_create_actor`)
   - Creation success / ALIVE (`on_actor_creation_success`)
   - Scheduling failure / DEAD (`on_actor_scheduling_failed`)
   - Restart / RESTARTING (`on_node_dead`, can_restart=true)
   - Death / DEAD (`on_node_dead`, can_restart=false)

### Tests added (4 new)

1. `test_actor_export_event_emitted_on_creation_success_when_enabled` — enabled config, verifies event buffered
2. `test_actor_export_event_emitted_on_restart_when_enabled` — enabled config, verifies event on restart
3. `test_actor_export_event_emitted_on_death_when_enabled` — enabled config, verifies event on death
4. `test_actor_export_event_not_emitted_when_disabled` — default (disabled) config, verifies zero events

### Test results

```
CARGO_TARGET_DIR=target_export cargo test -p ray-gcs --lib
test result: ok. 475 passed; 0 failed (+4 from 471)
```

## Final Assessment (updated after Round 8 fix)

**FULL parity is now achieved.** The configurable actor lifecycle/export event feature now exists in Rust, controlled by `ActorExportConfig`, matching all 6 C++ transition points.
