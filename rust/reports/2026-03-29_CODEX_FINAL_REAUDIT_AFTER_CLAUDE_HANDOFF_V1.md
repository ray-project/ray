# Codex Final Re-Audit After Claude Handoff

**Date:** 2026-03-29
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-29_CLAUDE_CODEX_HANDOFF_FINAL_REAUDIT_FIXES.md`
**Scope:** Source-level parity audit of Rust raylet against C++ raylet for the areas previously identified as open.

## Executive Verdict

Claude's latest round materially improved parity. Several previously high-severity gaps are now genuinely fixed.

However, parity is **not fully closed**. The latest Claude report overstates the result in one important area and understates the operational significance of two others.

### Final status

| Area | Verdict | Notes |
|---|---|---|
| Runtime-env auth token loading | **Closed** | Rust now loads token from env/file and passes it into production config. |
| Runtime-env eager-install gating | **Closed** | Rust now gates eager install on `runtime_env_config.eager_install()`. |
| RuntimeEnvConfig propagation | **Closed** | No remaining gap found in the previously flagged paths. |
| Worker command parsing | **Improved, not full parity** | Base command parsing is real, but full worker argv construction still diverges. |
| Runtime-env timeout fatal path | **Not closed** | Rust still does not execute the C++ graceful shutdown path before forced exit. |
| CLI defaults | **Documented divergence, not parity** | Defaults still differ; this is not "closed", only explained. |

## Findings

### 1. Runtime-env timeout shutdown parity is still open

**Severity:** HIGH if the claim is "C++ parity closed"

Claude marked this as fixed. That is not correct.

#### What C++ does

C++ constructs `NodeDeathInfo`, calls the raylet graceful shutdown callback, then schedules a forced process exit after 10 seconds:

- [runtime_env_agent_client.cc](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L300)
- [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L459)

The relevant C++ sequence is:

1. Build `NodeDeathInfo`
2. Call `shutdown_raylet_gracefully_(node_death_info)`
3. Schedule `QuickExit()` after 10 seconds

That graceful shutdown callback is a real shutdown path, not just a log line.

#### What Rust does now

Rust passes a zero-argument closure into the runtime-env client:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1155)

That closure:

1. Logs an error
2. Spawns a background thread
3. Sleeps 10 seconds
4. Calls `std::process::exit(1)`

It does **not** initiate raylet graceful shutdown before arming the forced exit timer.

The runtime-env client invokes that closure on deadline expiry:

- [runtime_env_agent_client.rs](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L303)

#### Why this still matters

This is not a cosmetic mismatch. It changes failure semantics:

- C++ gives the raylet a chance to shut down through its normal path.
- Rust currently skips that step and only schedules forced termination.
- Claude's report described a graceful-then-force pattern, but the Rust code does not actually implement the graceful half.

#### Firm recommendation

Do **not** mark runtime-env retry/deadline/fatal handling as closed until Rust has a real graceful shutdown callback with equivalent semantics.

### 2. Worker argv parity is still partial

**Severity:** MEDIUM

Claude downgraded this area honestly, which is correct. It remains open.

#### What Rust now does

Rust parses `python_worker_command` into argv and appends:

- `--node-ip-address=...`
- `--node-manager-port=...`

See:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L113)

#### What C++ still appends beyond that

C++ appends a materially larger worker-specific argv set, including:

- `--worker-id=...`
- `--worker-launch-time-ms=...`
- `--node-id=...`
- `--runtime-env-hash=...`
- `--language=...`
- `--serialized-runtime-env-context=...`
- `--worker-type=...` for IO workers
- object spilling config for IO workers

See:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L323)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L346)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L363)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L376)

#### Why this is still a real gap

Rust currently relies on environment variables for part of this state. That may be enough for basic startup, but it is not argv parity, and it leaves advanced behavior dependent on worker-side env handling rather than the C++ contract.

#### Firm recommendation

Treat full worker argv construction as an open parity item until Rust emits the C++ worker-specific arguments directly.

### 3. CLI defaults are still divergent

**Severity:** MEDIUM

Claude's report correctly describes this as intentionally different, but that means the gap is documented, not eliminated.

#### Current defaults

Rust:

- `metrics_agent_port = 0` in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L119)
- `object_store_memory = 0` in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L147)
- `object_manager_port = 0` in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L188)

C++:

- `metrics_agent_port = -1` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L72)
- `object_store_memory = -1` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L114)
- `object_manager_port = -1` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L70)

#### Assessment

If the standard is strict parity, this is still open. If the standard is operational equivalence under `ray start`, the divergence is probably acceptable.

#### Firm recommendation

Do not describe this area as fixed. Describe it as an intentional, bounded divergence unless and until the Rust CLI adopts the same sentinel model.

## Confirmed closures from this round

These previous findings now appear genuinely fixed in the current source:

### Runtime-env auth token loading

Rust now loads auth token from:

1. `RAY_AUTH_TOKEN`
2. `RAY_AUTH_TOKEN_PATH`
3. `~/.ray/auth_token`

and passes it into production `RayletConfig`:

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L342)
- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L363)

This closes the previous "auth is implemented in the client but not wired in production" gap.

### Eager-install semantics

Rust now only eagerly installs runtime envs when:

1. runtime env is non-empty
2. `eager_install` is true

See:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L480)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

This closes the previous semantic mismatch.

## Prescriptive plan

### Priority 1: close runtime-env fatal shutdown semantics

**Recommendation:** Treat this as the next required parity fix.

Implement all of the following:

1. Change the Rust shutdown callback contract from `Fn()` to a callback that carries shutdown context equivalent to C++ `NodeDeathInfo`, or build that context inside the callback owner.
2. Route the timeout path through the real raylet shutdown mechanism rather than only logging.
3. Preserve the 10-second forced-exit backstop after graceful shutdown is initiated.
4. Add a focused test that proves the timeout path invokes graceful shutdown before the forced exit timer is armed.

Acceptance criteria:

- Rust timeout path first requests raylet shutdown through the normal shutdown path.
- Forced exit remains a fallback, not the primary action.
- Code comments no longer overclaim parity.

### Priority 2: close worker argv construction parity

**Recommendation:** Implement the missing argv fields rather than relying on env vars as substitutes.

Add the missing worker-specific flags where applicable:

1. `--worker-id`
2. `--worker-launch-time-ms`
3. `--node-id`
4. `--runtime-env-hash`
5. `--language`
6. `--serialized-runtime-env-context`
7. IO-worker-only flags such as `--worker-type` and object spilling config

Acceptance criteria:

- Rust worker spawn path can be compared line-by-line against the C++ worker argv contract.
- Python and IO-worker startup no longer depend on env-only fallbacks for C++ argv fields.

### Priority 3: decide whether CLI default divergence is acceptable

**Recommendation:** Make an explicit project-level decision and encode it in the report, tests, and comments.

Choose one of two positions:

1. Match C++ sentinels exactly.
2. Keep Rust defaults divergent, but mark them as intentional non-parity and exclude them from parity claims.

Do not leave this in an ambiguous middle state.

Acceptance criteria:

- The parity report uses one consistent standard.
- The code comments, tests, and status table all reflect that same standard.

## Merge recommendation

Do **not** claim "C++ parity closed" yet.

The correct statement is narrower:

- Auth wiring is now closed.
- Eager-install semantics are now closed.
- Runtime-env fatal shutdown semantics are still open.
- Worker argv parity is still partial.
- CLI defaults are still a documented divergence, not parity.

That is the technically defensible position from the current source.
