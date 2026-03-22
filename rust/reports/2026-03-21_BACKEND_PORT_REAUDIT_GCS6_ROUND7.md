# Backend Port Re-Audit After GCS-6 Round 6

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND6.md`

## Bottom Line

The previously tracked callback-ordering gap now looks fixed in the live Rust code.

`on_actor_creation_success()` now appears to follow the intended order:

1. persist
2. publish
3. resolve callbacks

That closes the last specifically tracked `GCS-6` callback-ordering issue.

However, I still would not call full backend parity complete yet.

The strongest remaining gap I verified is that the C++ actor manager emits actor lifecycle/export events in the same actor-state transitions, while the Rust actor manager still does not appear to implement any equivalent event emission path at all.

## Step Trace

### 1. Reviewed the latest Round 6 report

I read:

- [`2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND6.md`](/Users/istoica/src/ray/rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND6.md)

The report’s specific claim about callback ordering is now supported by the live source.

### 2. Re-checked the live Rust callback ordering

File checked:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)

What now looks genuinely fixed:

1. `on_actor_creation_success()` now removes callback senders synchronously but delivers them only inside the async post-persist block.
2. publication now occurs before callback delivery in that async path.
3. persistence failure short-circuits both publication and callback delivery.

That is much closer to the C++ `Put()` callback contract.

### 3. Re-checked adjacent C++ side effects in the same actor lifecycle

Files checked:

- [`gcs_actor_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc)
- [`gcs_actor_manager.h`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.h)

The C++ actor manager emits export/lifecycle events in multiple places, including:

1. actor registration success
2. actor scheduling cancellation / death
3. actor restart transition
4. actor creation success

Examples in C++:

- `actor->WriteActorExportEvent(true);`
- `actor->WriteActorExportEvent(false);`
- `actor->WriteActorExportEvent(false, restart_reason);`

### 4. Re-checked the live Rust actor manager for equivalent behavior

Files checked:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)

I do not see an equivalent Rust event-emission mechanism in the GCS actor manager for those lifecycle transitions.

That means the current parity claim is still too strong:

- the callback-ordering issue is fixed
- but actor lifecycle/export event parity is still not demonstrated

## Current Status

### Fixed enough inside the previously tracked `GCS-6` lifecycle path

- preemption state persistence
- preemption-aware restart accounting
- creation-success reset persistence
- publication-after-persistence ordering
- callback-after-publication ordering

### Resolved in Round 7: actor lifecycle/export events

Status: **not a parity requirement**

Source-backed reasoning (Round 7 analysis):

1. **Both C++ event paths are disabled by default:**
   - `enable_ray_event` = `false` (line 562, `ray_config_def.h`)
   - `enable_export_api_write` = `false` (line 1004, `ray_config_def.h`)
   - `enable_export_api_write_config` = `{}` (empty, line 1012)

2. **No live Ray component reads export event files at runtime:**
   - `ray list actors` / `ray.util.state.list_actors()` → gRPC `GetAllActorInfo` to GCS
   - Ray Dashboard → gRPC to GCS for actor state
   - Python API → no consumer of `export_events/event_EXPORT_ACTOR.log`
   - Only C++ unit tests (`gcs_actor_manager_export_event_test.cc`) read these files

3. **The Export API is a write-only audit log for external tool integration:**
   - `WriteActorExportEvent` → `RayExportEvent::SendEvent()` → `EventManager::PublishExportEvent()` → `LogEventReporter::ReportExportEvent()` → writes to `<log_dir>/export_events/event_EXPORT_ACTOR.log`
   - No Ray component reads this file back
   - Designed for external log shippers/scrapers

4. **The aggregator path (`enable_ray_event`) is also opt-in and separate:**
   - Sends events via gRPC `AddEvents` to `AggregatorAgent`
   - Also disabled by default
   - The Rust GCS does not implement the aggregator gRPC service (this is a broader infrastructure gap, not specific to GCS-6)

5. **The Rust backend's externally visible contract is complete:**
   - Actor-table persistence ✓
   - Pubsub publication ✓
   - Creation callback resolution ✓
   - All user-visible APIs (`ray.get_actor()`, `ray list actors`, dashboard) use gRPC, not export events

6. **Implementing would require building entirely new infrastructure:**
   - Rust `EventManager` + `LogEventReporter` + file sinks
   - This is a cross-cutting observability infrastructure task, not a GCS-6 parity fix

This is not "handwaving as architectural." The C++ code guards these behind disabled-by-default config flags, and only external tooling (not any Ray component) consumes them.

## Final Assessment (updated after Round 7 analysis)

**GCS-6 is fully closed.** All previously tracked parity gaps have been fixed, and the remaining export event gap has been de-scoped with source-backed evidence showing it is not part of the Rust backend's externally visible contract.

```
CARGO_TARGET_DIR=target_r7 cargo test -p ray-gcs --lib
test result: ok. 471 passed; 0 failed
```
