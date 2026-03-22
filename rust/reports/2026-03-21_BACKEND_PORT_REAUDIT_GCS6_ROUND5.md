# Backend Port Re-Audit After GCS-6 Round 5

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md`

## Bottom Line

Round 5 fixes another real ordering gap:

- actor pubsub publication in the two targeted preemption-lifecycle paths is now ordered after actor-table persistence

That is a real improvement.

But I still would not call full parity complete yet.

The strongest remaining mismatch I verified is callback ordering in the creation-success path:

- C++ resolves actor-creation callbacks inside the `ActorTable().Put()` completion path, after persistence and publication
- the live Rust code still resolves creation callbacks immediately after spawning the async persist task, before persistence completes

That means Rust can still tell callers that actor creation succeeded before the corresponding actor-table update is durably written, which does not match the C++ contract.

## Step Trace

### 1. Reviewed the new Round 5 report

I read:

- [`2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md`](/Users/istoica/src/ray/rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md)

The report correctly describes one real improvement:

- publication is now moved behind persistence in the two targeted actor-state paths

That part is reflected in the live code.

### 2. Re-checked the C++ creation-success contract

File checked:

- [`gcs_actor_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc)

Relevant C++ ordering in `OnActorCreationSuccess(...)`:

1. update in-memory actor state
2. `ActorTable().Put(...)`
3. inside the `Put` callback:
   - publish actor state
   - write export event
   - run and clear actor-creation callbacks

So the callback-visible contract is stronger than “eventual persistence”:

- a successful create callback implies the actor-table write has already completed

### 3. Re-checked the live Rust creation-success path

File checked:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)

What is now fixed:

1. `on_actor_creation_success()` now persists first and publishes after successful persistence.

What still differs from C++:

1. after spawning the async persist-and-publish task, Rust immediately removes and resolves `create_callbacks`
2. those callbacks are therefore not gated on persistence completion
3. they are also not ordered after publication

So Rust still differs from C++ in externally visible success timing.

### 4. Why the current tests are still insufficient

The new ordering tests prove:

- publication does not race ahead of persistence

They do not prove:

- creation-success callbacks wait until persistence completes
- creation-success callbacks are ordered after publication like C++

That is the remaining gap.

## Current Status

### Fixed enough inside `GCS-6`

- preempted state persistence
- preemption-aware restart accounting
- creation-success persistence of reset state
- publication-after-persistence ordering for the two targeted actor-state paths

### Still open inside `GCS-6`

1. `on_actor_creation_success()` still resolves success callbacks before persistence completes

## Why this matters

This is observable to callers.

In C++:

- a successful create callback means the actor-table update has already been committed

In Rust:

- a successful create callback can still happen before the actor-table update completes

That is still a real parity gap in durability/notification semantics.

## Final Assessment

Round 5 is another real improvement, but I still do not think full parity is complete.

The remaining work is narrow:

- move creation callback resolution into the post-persist completion path
- add tests that prove callbacks do not fire before persistence completes

Until that is done, I would still classify `GCS-6` as extremely close, but not fully closed.
