# Re-Audit Executive Summary: Current Backend Port Status

Date: 2026-03-18

Reference:

- [`2026-03-18_BACKEND_PORT_REAUDIT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_REAUDIT.md)

## Bottom Line

The Rust backend is improved relative to the March 17 audit, but it still does
not look functionally equivalent to the original C++ backend.

The current branch should still be treated as experimental rather than as a
drop-in replacement.

## Fixed Or Materially Improved

These areas now look substantially better than in the earlier audit:

- `GCS-2`: `ReportJobError`
- `GCS-3`: job cleanup on node death
- `GCS-5`: unregister-node death info preservation
- `GCS-7`: node-address/liveness filtering and limit support
- `GCS-9`: actor lineage-reconstruction restart RPC
- `GCS-10`: `ReportActorOutOfScope`
- `GCS-11`: worker lookup/update RPCs
- `GCS-13`: placement-group removal persists `REMOVED`
- `GCS-14`: async wait for PG readiness
- `GCS-18`: autoscaler drain-node validation
- `RAYLET-1`: `ReturnWorkerLease`
- `RAYLET-7`: `DrainRaylet` rejection path
- `RAYLET-9`: cleanup RPCs
- `RAYLET-11`: wait timeout/validation
- `OBJECT-2`: duplicate pull-bundle ID handling
- `OBJECT-3`: deferred push support
- `OBJECT-4`: chunk validation path
- `OBJECT-5`: spill-path integration
- part of `CORE-8`: future-resolution timeout behavior
- part of `CORE-9`: lineage recovery integration
- part of `CORE-10`: owner-side location-update handling
- Python/native compatibility remains much stronger than before
- Tier 1 cluster scaffold remains green

## Partial Only

These areas improved, but should not yet be considered closed:

- `CORE-10`: ownership is still only partially matched; Rust still looks much
  more local than the C++ owner-driven distributed protocol
- Object Manager parity generally looks better, but still needs stronger
  end-to-end verification
- several formerly stubbed GCS and Raylet paths moved from “obviously broken”
  to “needs behavioral verification”

## Still Open

These still appear open from the current source review:

- `GCS-1`: internal KV still appears in-memory even when GCS storage is Redis
- `GCS-4`: `GetAllJobInfo` still appears much weaker than C++
- `GCS-6`: GCS drain-node side effects still appear too weak
- `GCS-8`: pubsub unsubscribe semantics still differ
- `GCS-12`: placement-group creation still starts in immediate `Created`
- `GCS-16`: autoscaler version-handling still appears weak
- `GCS-17`: cluster-status payload still appears thinner than C++
- `RAYLET-3`: worker backlog reporting still appears acknowledged-only
- `RAYLET-4`: object pinning still appears blanket-success
- `RAYLET-5`: local resource resize still appears stubbed
- `RAYLET-6`: node stats still appear drastically weaker than C++
- `CORE-10`: distributed ownership semantics are still not fully matched end to end

## Highest-Value Next Tests

The next tests most likely to expose remaining real differences are:

- Redis-backed internal-KV restart persistence
- `GetAllJobInfo` conformance vs C++ metadata/filter behavior
- GCS drain-node end-to-end side-effect test
- channel/key-scoped pubsub unsubscribe semantics
- placement-group create lifecycle starting in `PENDING`
- stale autoscaler version rejection
- cluster-status payload conformance
- backlog affecting scheduler/autoscaler-visible state
- object pinning failure/validation semantics
- resize-local-resources validation/clamping behavior
- rich node-stats payload conformance
- owner-death and borrower-cleanup distributed ownership tests
- full add/remove/spill object-location batch conformance

## Recommendation

The next work should focus on the remaining GCS control-plane gaps, the still
stubbed Raylet operational behaviors, and the incomplete distributed ownership
semantics in Core Worker before making stronger equivalence claims.
