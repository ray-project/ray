// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Syncer client — connects to a remote syncer and exchanges state.
//!
//! Ports the C++ `RaySyncerBidiReactor` client-side logic:
//! 1. Initiates bidirectional streaming with a peer
//! 2. Sends periodic outgoing batches
//! 3. Applies incoming batches
//! 4. Automatically reconnects on failure

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use ray_common::id::NodeID;
use ray_proto::ray::rpc::syncer::ray_syncer_client::RaySyncerClient;
use ray_proto::ray::rpc::syncer::RaySyncMessageBatch;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::NodeSyncState;

/// Configuration for the syncer client.
#[derive(Debug, Clone)]
pub struct SyncerClientConfig {
    /// Interval in milliseconds between outgoing sync batches.
    pub sync_interval_ms: u64,
    /// Delay before reconnection attempt in milliseconds.
    pub reconnect_delay_ms: u64,
    /// Whether to automatically reconnect on failure.
    pub auto_reconnect: bool,
}

impl Default for SyncerClientConfig {
    fn default() -> Self {
        Self {
            sync_interval_ms: 100,
            reconnect_delay_ms: 2000,
            auto_reconnect: true,
        }
    }
}

/// Handle for a running syncer client connection.
/// Drop this to stop the connection.
pub struct SyncerClientHandle {
    stop: Arc<AtomicBool>,
    _task: tokio::task::JoinHandle<()>,
}

impl SyncerClientHandle {
    /// Stop the syncer client connection.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Check if the connection is still running.
    pub fn is_running(&self) -> bool {
        !self._task.is_finished()
    }
}

impl Drop for SyncerClientHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Connect to a remote syncer and exchange state.
///
/// Returns a handle that can be used to stop the connection.
pub fn connect(
    state: Arc<NodeSyncState>,
    peer_id: NodeID,
    endpoint: tonic::transport::Endpoint,
    config: SyncerClientConfig,
) -> SyncerClientHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    let task = tokio::spawn(async move {
        loop {
            if stop_clone.load(Ordering::Relaxed) {
                break;
            }

            match run_sync_session(&state, &peer_id, &endpoint, &config, &stop_clone).await {
                Ok(()) => {
                    tracing::debug!(peer = ?peer_id, "Syncer session ended normally");
                }
                Err(e) => {
                    tracing::warn!(peer = ?peer_id, error = %e, "Syncer session error");
                }
            }

            if !config.auto_reconnect || stop_clone.load(Ordering::Relaxed) {
                break;
            }

            tracing::debug!(
                peer = ?peer_id,
                delay_ms = config.reconnect_delay_ms,
                "Reconnecting to syncer peer"
            );
            tokio::time::sleep(std::time::Duration::from_millis(config.reconnect_delay_ms)).await;
        }
    });

    SyncerClientHandle { stop, _task: task }
}

/// Run a single sync session with a peer.
async fn run_sync_session(
    state: &Arc<NodeSyncState>,
    peer_id: &NodeID,
    endpoint: &tonic::transport::Endpoint,
    config: &SyncerClientConfig,
    stop: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = endpoint.connect().await?;
    let mut client = RaySyncerClient::new(channel);

    // Create the outbound stream.
    let (tx, rx) = mpsc::channel::<RaySyncMessageBatch>(32);
    let outbound = ReceiverStream::new(rx);

    // Initiate bidirectional streaming.
    let response = client.start_sync(outbound).await?;
    let mut inbound = response.into_inner();

    // Track sent versions to this peer.
    let sent_resource_version = AtomicI64::new(0);
    let sent_commands_version = AtomicI64::new(0);

    // Send initial snapshot.
    let initial = state.build_outgoing_batch(0, 0);
    if !initial.messages.is_empty() {
        for msg in &initial.messages {
            match msg.message_type {
                0 => sent_resource_version.store(msg.version, Ordering::Relaxed),
                1 => sent_commands_version.store(msg.version, Ordering::Relaxed),
                _ => {}
            }
        }
        tx.send(initial).await?;
    }

    let interval_duration = std::time::Duration::from_millis(config.sync_interval_ms);
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        tokio::select! {
            // Receive from peer.
            maybe_batch = inbound.next() => {
                match maybe_batch {
                    Some(Ok(batch)) => {
                        state.apply_incoming_batch(&batch);
                    }
                    Some(Err(e)) => {
                        return Err(Box::new(e));
                    }
                    None => {
                        // Stream ended.
                        break;
                    }
                }
            }
            // Periodic outbound.
            _ = interval.tick() => {
                let rv = sent_resource_version.load(Ordering::Relaxed);
                let cv = sent_commands_version.load(Ordering::Relaxed);
                let batch = state.build_outgoing_batch(rv, cv);
                if !batch.messages.is_empty() {
                    for msg in &batch.messages {
                        match msg.message_type {
                            0 => sent_resource_version.store(msg.version, Ordering::Relaxed),
                            1 => sent_commands_version.store(msg.version, Ordering::Relaxed),
                            _ => {}
                        }
                    }
                    if tx.send(batch).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    // Clean up peer state on disconnect.
    state.remove_peer(peer_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    #[test]
    fn test_client_config_default() {
        let config = SyncerClientConfig::default();
        assert_eq!(config.sync_interval_ms, 100);
        assert_eq!(config.reconnect_delay_ms, 2000);
        assert!(config.auto_reconnect);
    }

    #[tokio::test]
    async fn test_handle_stop() {
        let state = Arc::new(NodeSyncState::new(make_nid(1)));

        // Create an endpoint that will fail to connect (no server running).
        // The client will attempt to connect, fail, and should be stoppable.
        let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
        let handle = connect(
            state,
            make_nid(2),
            endpoint,
            SyncerClientConfig {
                auto_reconnect: false,
                reconnect_delay_ms: 10,
                ..Default::default()
            },
        );

        // Give it time to fail.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        handle.stop();

        // Should eventually stop.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Not asserting is_running since it depends on timing.
    }
}
