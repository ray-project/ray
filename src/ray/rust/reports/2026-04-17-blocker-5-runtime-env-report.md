# 2026-04-17 - Blocker 5 Runtime Env Review

## Conclusion

Blocker 5 is not fully fixed yet.

The Rust runtime-env implementation now covers the main pieces that were previously missing:

- the runtime-env gRPC service is wired into the Rust GCS server
- URI pins are reference-counted
- expiry removes temporary references
- `gcs://` URIs are deleted from the internal KV store when the last reference is released

However, there is still one functional mismatch versus the C++ implementation:

- `expiration_s == 0` behaves differently in Rust than in C++

That difference is observable and matters for drop-in replacement parity.

## Main Finding

### Zero-expiration pinning is still incorrect in Rust

C++ always schedules the delayed removal callback, even when `expiration_s` is zero.
That means a zero-expiration pin is added and then immediately released.

Evidence:

- C++ handler: [runtime_env_handler.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/runtime_env_handler.cc:22)
- C++ zero-expiration test: [runtime_env_handler_test.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/tests/runtime_env_handler_test.cc:82)
- Rust implementation: [runtime_env_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:189)

Relevant C++ behavior:

```cpp
runtime_env_manager_.AddURIReference(hex_id, request.uri());

delay_executor_(
    [this, hex_id, request] {
      runtime_env_manager_.RemoveURIReference(hex_id);
    },
    /* expiration_ms= */ request.expiration_s() * 1000);
```

The C++ unit test explicitly checks the zero-expiration path:

```cpp
request.set_expiration_s(0);
...
ASSERT_EQ(delayed_calls_.size(), 1);
ASSERT_EQ(delayed_calls_[0].second, 0);
```

Rust currently gates the removal task behind `if expiration_s > 0`:

```rust
if expiration_s > 0 {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(expiration_s as u64)).await;
        ...
    });
}
```

As a result:

- C++: `expiration_s == 0` schedules immediate unpin
- Rust: `expiration_s == 0` does not schedule unpin at all

In Rust, that leaves the URI pinned indefinitely unless something else removes it later. That is not equivalent to C++.

## What Is Correct Now

The rest of the runtime-env path looks materially improved and mostly aligned.

### 1. Runtime-env service wiring is present

Rust server construction now uses a KV-backed runtime-env service:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:248)

This closes the earlier gap where runtime-env support was only nominal.

### 2. Reference counting is implemented

Rust now tracks:

- URI -> refcount
- temporary ref ID -> pinned URIs

Source:

- [runtime_env_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:41)

This matches the basic model used by C++ `RuntimeEnvManager`.

### 3. Last-release `gcs://` cleanup is implemented

When a URI's refcount reaches zero, Rust now deletes `gcs://` URIs from the KV store.

Sources:

- Rust cleanup path: [runtime_env_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:226)
- C++ deleter wiring: [gcs_server.cc](/Users/istoica/src/cc-to-rust/ray/src/ray/gcs/gcs_server.cc:696)

That is the right parity direction and appears consistent with the C++ server-side deleter contract.

## Validation Performed

I rechecked the implementation directly against the C++ sources and ran the targeted Rust tests.

Command run:

```bash
cargo test -p gcs-managers runtime_env -- --nocapture
```

Result:

- 7 tests passed
- 0 failed

The passing tests show that the refcounting and ordinary expiry paths work, but they do not prove parity for the zero-expiration case. In fact, the Rust test suite currently lacks the equivalent of the C++ `TestPinRuntimeEnvURI_ZeroExpiration`.

## Risk Assessment

This is a narrow bug, not a broad reversion.

The implementation is close to parity now, but the remaining mismatch is still a real correctness issue because:

- the C++ behavior is explicit and tested
- Rust behavior is observably different for a valid input
- zero-expiration requests can leave stale runtime-env pins in Rust
- stale pins can delay or prevent cleanup of runtime-env artifacts

For a drop-in replacement claim, this should still be treated as open.

## Recommended Fix

Update the Rust implementation so that it always schedules the removal task for non-empty URIs, including when `expiration_s == 0`.

Concretely:

- remove the `if expiration_s > 0` guard
- allow a zero-duration sleep or immediate spawned cleanup path
- add a Rust test that mirrors the C++ zero-expiration case

Suggested test coverage:

- pin with `expiration_s = 0`
- confirm the temporary ref is released immediately after the scheduled task runs
- if using KV-backed test scaffolding, confirm `gcs://` cleanup is triggered when that release drops the count to zero

## Final Status

Blocker 5 should remain open.

The runtime-env path is much closer to C++ parity than before, but it is not yet a drop-in replacement because the `expiration_s == 0` behavior still differs from the C++ GCS implementation.
