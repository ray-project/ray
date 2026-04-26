//! `GcsFunctionManager` — job-scoped KV cleanup refcounter.
//!
//! Port of C++ `ray::gcs::GCSFunctionManager`
//! (`src/ray/gcs/gcs_function_manager.h`). Single-file parity:
//! everything non-trivial in C++ is in the header. This module keeps
//! the shape 1:1.
//!
//! What it does
//! ------------
//! Tracks a `job_id → count` refcounter. Both the job manager and the
//! actor manager hold a reference and bump/decrement as they observe
//! lifecycle events:
//!
//!  * Job manager: `add` on register / on recovery; `remove` on
//!    `MarkJobFinished`.
//!  * Actor manager: `add` on register / on recovery; `remove` on
//!    destroy-and-not-restartable.
//!
//! When a job's count drops to zero, three KV entries (with the
//! job id hex suffix) are prefix-deleted from namespace `fun`:
//!
//!  * `RemoteFunction:<hex>:`
//!  * `ActorClass:<hex>:`
//!  * `FunctionsToRun:<hex>:` (`kWorkerSetupHookKeyName`)
//!
//! Without this, KV retains per-job function/class/hook metadata
//! indefinitely — a long-lived cluster leaks entries that the C++ GCS
//! cleans up on the same conditions.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::debug;

use gcs_kv::InternalKVInterface;

/// KV namespace for function/actor/class metadata. Matches C++ call
/// sites at `gcs_function_manager.h:64-69`.
const FUN_NAMESPACE: &str = "fun";

/// Prefix for RemoteFunction entries. C++ line 64.
const REMOTE_FUNCTION_PREFIX: &str = "RemoteFunction:";

/// Prefix for ActorClass entries. C++ line 65.
const ACTOR_CLASS_PREFIX: &str = "ActorClass:";

/// Prefix for worker-setup-hook entries. Matches C++
/// `kWorkerSetupHookKeyName` in `common/constants.h:25`.
const WORKER_SETUP_HOOK_PREFIX: &str = "FunctionsToRun:";

/// Job-scoped KV-cleanup refcounter.
pub struct GcsFunctionManager {
    kv: Arc<dyn InternalKVInterface>,
    counter: Mutex<HashMap<Vec<u8>, usize>>,
}

impl GcsFunctionManager {
    pub fn new(kv: Arc<dyn InternalKVInterface>) -> Self {
        Self {
            kv,
            counter: Mutex::new(HashMap::new()),
        }
    }

    /// Increment the reference count for `job_id`. Idempotent under
    /// concurrent callers — both the job and actor managers may bump
    /// the same job id. Mirrors C++
    /// `AddJobReference` (`gcs_function_manager.h:44`).
    pub fn add_job_reference(&self, job_id: &[u8]) {
        let mut c = self.counter.lock();
        *c.entry(job_id.to_vec()).or_insert(0) += 1;
    }

    /// Decrement the reference count. When it hits zero, removes
    /// the per-job entries from KV. A call for a job that is not in
    /// the map is a no-op — matches C++'s early return for network
    /// retry duplicates at `gcs_function_manager.h:48-50`.
    ///
    /// Async because the KV deletes are awaited. C++ fires them as
    /// io_context callbacks; we keep the side effects ordered with
    /// the caller for easier testing and equivalent observable
    /// semantics.
    pub async fn remove_job_reference(&self, job_id: &[u8]) {
        // Decrement under the lock, then drop it before any `await`.
        let should_cleanup = {
            let mut c = self.counter.lock();
            let Some(n) = c.get_mut(job_id) else {
                return; // Already removed — duplicate call, no-op.
            };
            *n -= 1;
            if *n == 0 {
                c.remove(job_id);
                true
            } else {
                false
            }
        };
        if should_cleanup {
            self.remove_exported_functions(job_id).await;
        }
    }

    /// Prefix-delete the three job-scoped KV entries. Mirrors C++
    /// `RemoveExportedFunctions` (`gcs_function_manager.h:61-70`).
    async fn remove_exported_functions(&self, job_id: &[u8]) {
        let hex = hex_encode(job_id);
        let keys = [
            format!("{REMOTE_FUNCTION_PREFIX}{hex}:"),
            format!("{ACTOR_CLASS_PREFIX}{hex}:"),
            format!("{WORKER_SETUP_HOOK_PREFIX}{hex}:"),
        ];
        for k in &keys {
            // `del_by_prefix = true` matches C++'s third argument; the
            // specific keys under each prefix (per-function / per-class /
            // per-hook names) are created by the Python side and the
            // GCS just wholesale-removes them on job retirement.
            let _ = self.kv.del(FUN_NAMESPACE, k, true).await;
        }
        debug!(
            job_hex = hex.as_str(),
            "GcsFunctionManager: removed exported functions for finished job"
        );
    }

    /// Peek at the current reference count for `job_id`. Test
    /// helper — production code doesn't need to introspect the
    /// refcounter, but cross-crate tests (in `gcs-server`) do, and
    /// `#[cfg(test)]` would hide the symbol from them.
    pub fn count(&self, job_id: &[u8]) -> usize {
        self.counter
            .lock()
            .get(job_id)
            .copied()
            .unwrap_or(0)
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_kv::StoreClientInternalKV;
    use gcs_store::InMemoryStoreClient;

    fn make_kv() -> Arc<dyn InternalKVInterface> {
        Arc::new(StoreClientInternalKV::new(Arc::new(InMemoryStoreClient::new())))
    }

    /// Parity guard for the refcount arithmetic: `add` then `remove`
    /// brings the count back to zero, and a second `remove` is a
    /// no-op (covers the network-retry duplicate case at C++ line
    /// 48-50).
    #[tokio::test]
    async fn add_and_remove_balance_plus_idempotent_remove() {
        let fm = GcsFunctionManager::new(make_kv());
        fm.add_job_reference(b"job-1");
        fm.add_job_reference(b"job-1");
        assert_eq!(fm.count(b"job-1"), 2);

        fm.remove_job_reference(b"job-1").await;
        assert_eq!(fm.count(b"job-1"), 1);
        fm.remove_job_reference(b"job-1").await;
        assert_eq!(fm.count(b"job-1"), 0);

        // Third remove must be a no-op, not a panic or negative count.
        fm.remove_job_reference(b"job-1").await;
        assert_eq!(fm.count(b"job-1"), 0);
    }

    /// Parity guard for KV cleanup shape. When refcount drops to
    /// zero, the three prefix-deletes happen; earlier entries under
    /// other job ids must be untouched.
    #[tokio::test]
    async fn removes_fun_namespace_entries_when_refcount_hits_zero() {
        let kv = make_kv();
        // Seed KV with two remote-function keys under one job and one
        // under another. All three go in the `fun` namespace.
        kv.put("fun", "RemoteFunction:aa:f1", "payload1".to_string(), true)
            .await;
        kv.put("fun", "RemoteFunction:aa:f2", "payload2".to_string(), true)
            .await;
        kv.put("fun", "ActorClass:aa:A", "payloadA".to_string(), true).await;
        kv.put("fun", "FunctionsToRun:aa:H", "payloadH".to_string(), true)
            .await;
        kv.put("fun", "RemoteFunction:bb:other", "keep".to_string(), true)
            .await;

        let fm = GcsFunctionManager::new(kv.clone());
        fm.add_job_reference(&[0xaa]);
        fm.remove_job_reference(&[0xaa]).await;

        // All three prefix-delete paths for job 0xaa should be gone.
        assert_eq!(kv.get("fun", "RemoteFunction:aa:f1").await, None);
        assert_eq!(kv.get("fun", "RemoteFunction:aa:f2").await, None);
        assert_eq!(kv.get("fun", "ActorClass:aa:A").await, None);
        assert_eq!(kv.get("fun", "FunctionsToRun:aa:H").await, None);
        // And the other job's key must be untouched.
        assert_eq!(
            kv.get("fun", "RemoteFunction:bb:other").await.as_deref(),
            Some("keep"),
            "prefix delete for job aa must not touch job bb"
        );
    }

    /// Positive parity guard: remove only fires the KV cleanup on the
    /// transition to zero, not on every decrement. Two references
    /// means one `remove` does nothing to KV.
    #[tokio::test]
    async fn partial_remove_keeps_kv_entries() {
        let kv = make_kv();
        kv.put("fun", "RemoteFunction:aa:x", "keep".to_string(), true)
            .await;
        let fm = GcsFunctionManager::new(kv.clone());

        fm.add_job_reference(&[0xaa]);
        fm.add_job_reference(&[0xaa]);
        fm.remove_job_reference(&[0xaa]).await; // refcount 1, no cleanup
        assert_eq!(
            kv.get("fun", "RemoteFunction:aa:x").await.as_deref(),
            Some("keep"),
            "KV cleanup must wait for the refcount → 0 transition"
        );

        fm.remove_job_reference(&[0xaa]).await; // refcount 0, cleanup
        assert_eq!(kv.get("fun", "RemoteFunction:aa:x").await, None);
    }
}
