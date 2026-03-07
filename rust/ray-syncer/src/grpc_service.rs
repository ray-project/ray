// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC service implementation for the Ray Syncer.
//!
//! Implements the `StartSync` bidirectional streaming RPC.
//! When a peer connects, this service:
//! 1. Sends the local node's current state as an initial snapshot
//! 2. Forwards incoming messages to `NodeSyncState::apply_incoming_batch()`
//! 3. Periodically sends outgoing updates via `NodeSyncState::build_outgoing_batch()`

use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use ray_proto::ray::rpc::syncer::ray_syncer_server::RaySyncer as RaySyncerTrait;
use ray_proto::ray::rpc::syncer::RaySyncMessageBatch;

use crate::NodeSyncState;

/// Configuration for the syncer gRPC service.
#[derive(Debug, Clone)]
pub struct SyncerServiceConfig {
    /// Interval in milliseconds between outgoing sync batches.
    pub sync_interval_ms: u64,
    /// Maximum messages per outgoing batch.
    pub max_batch_size: usize,
}

impl Default for SyncerServiceConfig {
    fn default() -> Self {
        Self {
            sync_interval_ms: 100,
            max_batch_size: 100,
        }
    }
}

/// The gRPC service implementation for bidirectional syncer streaming.
pub struct RaySyncerService {
    state: Arc<NodeSyncState>,
    config: SyncerServiceConfig,
}

impl RaySyncerService {
    pub fn new(state: Arc<NodeSyncState>, config: SyncerServiceConfig) -> Self {
        Self { state, config }
    }

    pub fn with_defaults(state: Arc<NodeSyncState>) -> Self {
        Self::new(state, SyncerServiceConfig::default())
    }
}

#[tonic::async_trait]
impl RaySyncerTrait for RaySyncerService {
    type StartSyncStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<RaySyncMessageBatch, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn start_sync(
        &self,
        request: tonic::Request<tonic::Streaming<RaySyncMessageBatch>>,
    ) -> Result<tonic::Response<Self::StartSyncStream>, tonic::Status> {
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let mut inbound = request.into_inner();

        let (tx, rx) = mpsc::channel::<Result<RaySyncMessageBatch, tonic::Status>>(32);

        // Track what versions we've already sent to this peer.
        let sent_resource_version = Arc::new(AtomicI64::new(0));
        let sent_commands_version = Arc::new(AtomicI64::new(0));

        // Send initial snapshot.
        let initial_batch = state.build_outgoing_batch(0, 0);
        if !initial_batch.messages.is_empty() {
            // Update sent versions.
            for msg in &initial_batch.messages {
                match msg.message_type {
                    0 => sent_resource_version.store(msg.version, Ordering::Relaxed),
                    1 => sent_commands_version.store(msg.version, Ordering::Relaxed),
                    _ => {}
                }
            }
            let _ = tx.send(Ok(initial_batch)).await;
        }

        // Spawn task to handle inbound messages and periodic outbound sync.
        let tx_clone = tx.clone();
        let state_clone = Arc::clone(&state);
        let srv = sent_resource_version.clone();
        let scv = sent_commands_version.clone();

        tokio::spawn(async move {
            let interval_duration = std::time::Duration::from_millis(config.sync_interval_ms);
            let mut interval = tokio::time::interval(interval_duration);

            loop {
                tokio::select! {
                    // Handle incoming messages from peer.
                    maybe_batch = inbound.next() => {
                        match maybe_batch {
                            Some(Ok(batch)) => {
                                state_clone.apply_incoming_batch(&batch);
                            }
                            Some(Err(e)) => {
                                tracing::warn!(error = %e, "Syncer inbound stream error");
                                break;
                            }
                            None => {
                                tracing::debug!("Syncer peer disconnected");
                                break;
                            }
                        }
                    }
                    // Periodic outbound sync.
                    _ = interval.tick() => {
                        let rv = srv.load(Ordering::Relaxed);
                        let cv = scv.load(Ordering::Relaxed);
                        let batch = state_clone.build_outgoing_batch(rv, cv);
                        if !batch.messages.is_empty() {
                            for msg in &batch.messages {
                                match msg.message_type {
                                    0 => srv.store(msg.version, Ordering::Relaxed),
                                    1 => scv.store(msg.version, Ordering::Relaxed),
                                    _ => {}
                                }
                            }
                            if tx_clone.send(Ok(batch)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeSyncState;
    use ray_common::id::NodeID;
    use ray_proto::ray::rpc::syncer::{MessageType, ResourceViewSyncMessage};
    use std::collections::HashMap;

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    fn make_resource_view(cpu: f64) -> ResourceViewSyncMessage {
        let mut available = HashMap::new();
        available.insert("CPU".to_string(), cpu);
        ResourceViewSyncMessage {
            resources_available: available.clone(),
            resources_total: available,
            ..Default::default()
        }
    }

    #[test]
    fn test_service_config_default() {
        let config = SyncerServiceConfig::default();
        assert_eq!(config.sync_interval_ms, 100);
        assert_eq!(config.max_batch_size, 100);
    }

    #[test]
    fn test_service_creation() {
        let state = Arc::new(NodeSyncState::new(make_nid(1)));
        let _service = RaySyncerService::with_defaults(state);
    }

    #[test]
    fn test_initial_snapshot_includes_current_state() {
        let state = Arc::new(NodeSyncState::new(make_nid(1)));
        state.publish_resource_view(make_resource_view(4.0));

        let batch = state.build_outgoing_batch(0, 0);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(
            batch.messages[0].message_type,
            MessageType::ResourceView as i32
        );
    }
}
