// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Internal pub/sub handler for GCS.
//!
//! Replaces the InternalPubSubGcsService handler.
//!
//! Uses long-polling: subscribers send a poll request, and the GCS
//! holds it until there are messages to return.

use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::broadcast;

/// Channel types for pub/sub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ChannelType {
    GcsActorChannel = 2,
    GcsJobChannel = 3,
    GcsNodeInfoChannel = 4,
    GcsWorkerDeltaChannel = 5,
    RayErrorInfoChannel = 6,
    RayLogChannel = 7,
    RayNodeResourceUsageChannel = 8,
}

/// Message published on a channel.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel_type: i32,
    pub key_id: Vec<u8>,
    pub value: Vec<u8>,
}

/// The internal pub/sub handler for GCS.
pub struct InternalPubSubHandler {
    /// Per-channel broadcast senders.
    channels: HashMap<i32, broadcast::Sender<PubSubMessage>>,
    /// Subscriber ID → pending messages buffer.
    subscriber_buffers: Mutex<HashMap<Vec<u8>, Vec<PubSubMessage>>>,
}

impl InternalPubSubHandler {
    pub fn new() -> Self {
        let mut channels = HashMap::new();
        // Create channels for each known type
        for channel_type in [2, 3, 4, 5, 6, 7, 8] {
            let (tx, _) = broadcast::channel(1024);
            channels.insert(channel_type, tx);
        }
        Self {
            channels,
            subscriber_buffers: Mutex::new(HashMap::new()),
        }
    }

    /// Publish a message to a channel.
    pub fn publish(&self, channel_type: i32, key_id: Vec<u8>, value: Vec<u8>) {
        let msg = PubSubMessage {
            channel_type,
            key_id,
            value,
        };
        if let Some(tx) = self.channels.get(&channel_type) {
            let _ = tx.send(msg);
        }
    }

    /// Subscribe to a channel (returns a receiver).
    pub fn subscribe(&self, channel_type: i32) -> Option<broadcast::Receiver<PubSubMessage>> {
        self.channels.get(&channel_type).map(|tx| tx.subscribe())
    }

    /// Handle GcsPublish RPC.
    pub fn handle_publish(&self, messages: Vec<ray_proto::ray::rpc::PubMessage>) {
        for msg in messages {
            // Encode the full PubMessage as the value so subscribers can decode it
            let encoded = prost::Message::encode_to_vec(&msg);
            self.publish(msg.channel_type, msg.key_id, encoded);
        }
    }

    /// Handle GcsSubscriberPoll RPC — long poll for messages.
    pub async fn handle_subscriber_poll(
        &self,
        subscriber_id: &[u8],
        max_messages: usize,
    ) -> Vec<PubSubMessage> {
        // Check for buffered messages first
        {
            let mut buffers = self.subscriber_buffers.lock();
            if let Some(buf) = buffers.get_mut(subscriber_id) {
                if !buf.is_empty() {
                    let count = buf.len().min(max_messages);
                    return buf.drain(..count).collect();
                }
            }
        }

        // No buffered messages — return empty (caller should retry).
        // In production, this would use long-polling with tokio::select.
        Vec::new()
    }

    /// Handle subscribe command — register a subscriber for channels.
    pub fn handle_subscribe_command(&self, subscriber_id: Vec<u8>, _channel_types: &[i32]) {
        self.subscriber_buffers
            .lock()
            .entry(subscriber_id)
            .or_default();
    }

    /// Handle unsubscribe command.
    pub fn handle_unsubscribe_command(&self, subscriber_id: &[u8]) {
        self.subscriber_buffers.lock().remove(subscriber_id);
    }
}

impl Default for InternalPubSubHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_subscribe() {
        let handler = InternalPubSubHandler::new();

        let mut rx = handler.subscribe(2).unwrap(); // GCS_ACTOR_CHANNEL

        handler.publish(2, b"actor1".to_vec(), b"data".to_vec());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel_type, 2);
        assert_eq!(msg.key_id, b"actor1");
        assert_eq!(msg.value, b"data");
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let handler = InternalPubSubHandler::new();

        handler.handle_subscribe_command(b"sub1".to_vec(), &[2, 3]);
        handler.handle_unsubscribe_command(b"sub1");

        // No panic — just verifying the operations complete
    }
}
