# 2026-04-17 — Blocker 5 fix: runtime env pin with `expiration_s == 0`

## Reviewer's finding (valid)

C++ `HandlePinRuntimeEnvURI` always schedules the delayed removal — even when `expiration_s == 0`, in which case the timer fires immediately, releasing the temporary pin right after the RPC completes. The Rust implementation guarded the spawn with `if expiration_s > 0`, so a zero-expiration request leaked a permanent pin.

C++ test that pins this contract: `runtime_env_handler_test.cc:82-98` (`TestPinRuntimeEnvURI_ZeroExpiration`) — asserts the delay executor is still called, with `delay_ms == 0`.

## Fix

`ray/src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:189-247`

Before:

```rust
if expiration_s > 0 {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(expiration_s as u64)).await;
        // ...decrement, KV cleanup...
    });
}
```

After:

```rust
tokio::spawn(async move {
    if expiration_s > 0 {
        tokio::time::sleep(Duration::from_secs(expiration_s as u64)).await;
    } else {
        // Match C++ delay_executor_(..., 0): yield so the reply is sent
        // first, then run the removal on the next executor tick.
        tokio::task::yield_now().await;
    }
    // ...decrement, KV cleanup...
});
```

The spawn is now unconditional. `expiration_s == 0` triggers a single `yield_now().await`, which is the Tokio analog of an ASIO deadline timer firing with `delay_ms = 0` — the cooperative yield ensures the gRPC reply is sent before the removal runs, matching the C++ comment "this must be called after the delay executor is set up" (runtime_env_handler.cc:40-41).

## New test coverage

Added two tests mirroring the C++ contract:

1. `test_pin_zero_expiration_releases_immediately` — pins `s3://bucket/my_env.zip` with `expiration_s = 0`, yields, asserts refcount returned to 0 and `id_to_uris` is empty.
2. `test_pin_zero_expiration_deletes_gcs_uri_from_kv` — same but for a `gcs://` URI seeded into the KV; asserts the entry is deleted from the KV after the zero-expiration pin completes (verifies the KV deleter still cascades on zero-delay expiry).

## Verification

```
cargo test -p gcs-managers runtime_env
running 9 tests
... test_pin_zero_expiration_releases_immediately ... ok
... test_pin_zero_expiration_deletes_gcs_uri_from_kv ... ok
... (7 prior tests) ... ok
test result: ok. 9 passed; 0 failed
```

Full workspace: 252 tests pass, 0 failures.

## Parity status

- Behavioral parity with C++ `HandlePinRuntimeEnvURI` — including the zero-expiration edge case — is now in place.
- The corresponding C++ test (`TestPinRuntimeEnvURI_ZeroExpiration`) has a direct Rust analog with the same assertion intent.

Blocker 5 is closed.
