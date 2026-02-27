// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Dependency resolution for task arguments.
//!
//! Tracks pending object dependencies and notifies when they become available.

use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use ray_common::id::ObjectID;

use crate::error::{CoreWorkerError, CoreWorkerResult};

/// Tracks pending object dependencies and provides async wait for resolution.
pub struct DependencyResolver {
    /// Map from object ID to the list of oneshot senders waiting for it.
    pending: Mutex<HashMap<ObjectID, Vec<oneshot::Sender<()>>>>,
}

impl DependencyResolver {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Wait for all dependencies to become available.
    ///
    /// Returns immediately if `dependencies` is empty. Otherwise registers
    /// waiters for each missing object and awaits them all.
    pub async fn resolve_dependencies(
        &self,
        dependencies: &[ObjectID],
    ) -> CoreWorkerResult<()> {
        if dependencies.is_empty() {
            return Ok(());
        }

        let mut receivers = Vec::new();
        {
            let mut pending = self.pending.lock();
            for oid in dependencies {
                let (tx, rx) = oneshot::channel();
                pending.entry(*oid).or_default().push(tx);
                receivers.push(rx);
            }
        }

        for rx in receivers {
            rx.await.map_err(|_| {
                CoreWorkerError::Internal("dependency resolver channel closed".into())
            })?;
        }

        Ok(())
    }

    /// Signal that an object is now available, waking all waiters.
    pub fn object_available(&self, object_id: &ObjectID) {
        let mut pending = self.pending.lock();
        if let Some(waiters) = pending.remove(object_id) {
            for tx in waiters {
                let _ = tx.send(());
            }
        }
    }

    /// Number of objects with pending waiters.
    pub fn num_pending(&self) -> usize {
        self.pending.lock().len()
    }
}

impl Default for DependencyResolver {
    fn default() -> Self {
        Self::new()
    }
}
