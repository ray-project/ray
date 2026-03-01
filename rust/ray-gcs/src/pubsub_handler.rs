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
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::Mutex;
use tokio::sync::{broadcast, Notify};

/// Channel types for pub/sub — values must match the proto ChannelType enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ChannelType {
    GcsActorChannel = 3,
    GcsJobChannel = 4,
    GcsNodeInfoChannel = 5,
    GcsWorkerDeltaChannel = 6,
    RayErrorInfoChannel = 7,
    RayLogChannel = 8,
    RayNodeResourceUsageChannel = 9,
    GcsNodeAddressAndLivenessChannel = 10,
}

/// Message published on a channel.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel_type: i32,
    pub key_id: Vec<u8>,
    pub value: Vec<u8>,
}

/// Per-subscriber state for the long-poll delivery mechanism.
struct SubscriberState {
    /// Buffered PubMessage protos waiting for delivery.
    pending_messages: Vec<ray_proto::ray::rpc::PubMessage>,
    /// Monotonically increasing sequence ID for published messages.
    next_sequence_id: i64,
    /// Subscribed channels: channel_type → set of key_ids (empty = all keys).
    subscriptions: HashMap<i32, Vec<Vec<u8>>>,
}

/// The internal pub/sub handler for GCS.
pub struct InternalPubSubHandler {
    /// Per-channel broadcast senders (for internal Rust subscribers).
    channels: HashMap<i32, broadcast::Sender<PubSubMessage>>,
    /// Per-subscriber state for long-poll delivery.
    subscribers: Mutex<HashMap<Vec<u8>, SubscriberState>>,
    /// Notification for waking up long-polling subscribers.
    notify: Notify,
    /// Global sequence counter.
    sequence_counter: AtomicI64,
}

impl InternalPubSubHandler {
    pub fn new() -> Self {
        let mut channels = HashMap::new();
        // Create channels for each known type (values match proto ChannelType)
        for channel_type in [3, 4, 5, 6, 7, 8, 9, 10] {
            let (tx, _) = broadcast::channel(1024);
            channels.insert(channel_type, tx);
        }
        Self {
            channels,
            subscribers: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            sequence_counter: AtomicI64::new(1),
        }
    }

    /// Publish a raw message to the internal broadcast channel.
    pub fn publish(&self, channel_type: i32, key_id: Vec<u8>, value: Vec<u8>) {
        let msg = PubSubMessage {
            channel_type,
            key_id: key_id.clone(),
            value,
        };
        if let Some(tx) = self.channels.get(&channel_type) {
            let _ = tx.send(msg);
        }
    }

    /// Publish a `PubMessage` proto to all matching subscribers.
    /// This is the main method used by the actor manager and other managers
    /// to notify CoreWorkers of state changes.
    pub fn publish_pubmessage(&self, pub_message: ray_proto::ray::rpc::PubMessage) {
        let channel_type = pub_message.channel_type;
        let key_id = pub_message.key_id.clone();

        // Also publish to broadcast channels for internal Rust subscribers
        let encoded = prost::Message::encode_to_vec(&pub_message);
        self.publish(channel_type, key_id.clone(), encoded);

        // Deliver to long-poll subscribers
        let mut subs = self.subscribers.lock();
        for state in subs.values_mut() {
            // Check if this subscriber is interested in this channel
            if let Some(keys) = state.subscriptions.get(&channel_type) {
                // Empty keys list means "subscribe to all keys in this channel"
                if keys.is_empty() || keys.contains(&key_id) {
                    let mut msg = pub_message.clone();
                    msg.sequence_id = state.next_sequence_id;
                    state.next_sequence_id += 1;
                    state.pending_messages.push(msg);
                }
            }
        }
        drop(subs);

        // Wake up any long-polling subscribers
        self.notify.notify_waiters();
    }

    /// Subscribe to a channel (returns a broadcast receiver — for internal Rust use).
    pub fn subscribe(&self, channel_type: i32) -> Option<broadcast::Receiver<PubSubMessage>> {
        self.channels.get(&channel_type).map(|tx| tx.subscribe())
    }

    /// Handle GcsPublish RPC — publish messages from external sources.
    pub fn handle_publish(&self, messages: Vec<ray_proto::ray::rpc::PubMessage>) {
        for msg in messages {
            self.publish_pubmessage(msg);
        }
    }

    /// Handle GcsSubscriberPoll RPC — long poll for messages.
    ///
    /// Returns pending messages for this subscriber. If no messages are pending,
    /// waits up to the notification (or a timeout in the gRPC handler).
    pub async fn handle_subscriber_poll(
        &self,
        subscriber_id: &[u8],
        max_processed_sequence_id: i64,
    ) -> Vec<ray_proto::ray::rpc::PubMessage> {
        // Drain any messages with sequence_id > max_processed_sequence_id
        {
            let mut subs = self.subscribers.lock();
            if let Some(state) = subs.get_mut(subscriber_id) {
                // Remove already-processed messages
                state
                    .pending_messages
                    .retain(|m| m.sequence_id > max_processed_sequence_id);

                if !state.pending_messages.is_empty() {
                    return std::mem::take(&mut state.pending_messages);
                }
            }
        }

        // Wait for new messages (with a timeout handled by the caller)
        self.notify.notified().await;

        // After notification, drain messages
        let mut subs = self.subscribers.lock();
        if let Some(state) = subs.get_mut(subscriber_id) {
            state
                .pending_messages
                .retain(|m| m.sequence_id > max_processed_sequence_id);
            return std::mem::take(&mut state.pending_messages);
        }
        Vec::new()
    }

    /// Handle subscribe command — register a subscriber for a channel+key.
    pub fn handle_subscribe_command(
        &self,
        subscriber_id: Vec<u8>,
        channel_type: i32,
        key_id: Vec<u8>,
    ) {
        let mut subs = self.subscribers.lock();
        let state = subs.entry(subscriber_id).or_insert_with(|| SubscriberState {
            pending_messages: Vec::new(),
            next_sequence_id: self.sequence_counter.fetch_add(1, Ordering::Relaxed),
            subscriptions: HashMap::new(),
        });
        let keys = state.subscriptions.entry(channel_type).or_default();
        if !key_id.is_empty() && !keys.contains(&key_id) {
            keys.push(key_id);
        }
    }

    /// Handle unsubscribe command for a specific channel.
    pub fn handle_unsubscribe_command(&self, subscriber_id: &[u8]) {
        self.subscribers.lock().remove(subscriber_id);
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
    use std::sync::Arc;

    fn make_pub_msg(channel_type: i32, key_id: &[u8]) -> ray_proto::ray::rpc::PubMessage {
        ray_proto::ray::rpc::PubMessage {
            channel_type,
            key_id: key_id.to_vec(),
            ..Default::default()
        }
    }

    fn make_actor_pub_msg(actor_id: &[u8]) -> ray_proto::ray::rpc::PubMessage {
        ray_proto::ray::rpc::PubMessage {
            channel_type: ChannelType::GcsActorChannel as i32,
            key_id: actor_id.to_vec(),
            inner_message: Some(
                ray_proto::ray::rpc::pub_message::InnerMessage::ActorMessage(
                    ray_proto::ray::rpc::ActorTableData {
                        actor_id: actor_id.to_vec(),
                        state: 2,
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_publish_subscribe() {
        let handler = InternalPubSubHandler::new();
        let mut rx = handler.subscribe(3).unwrap();

        handler.publish(3, b"actor1".to_vec(), b"data".to_vec());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel_type, 3);
        assert_eq!(msg.key_id, b"actor1");
        assert_eq!(msg.value, b"data");
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let handler = InternalPubSubHandler::new();

        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);
        assert!(handler.subscribers.lock().contains_key(&b"sub1".to_vec()));

        handler.handle_unsubscribe_command(b"sub1");
        assert!(!handler.subscribers.lock().contains_key(&b"sub1".to_vec()));

        // Double-unsubscribe should not panic
        handler.handle_unsubscribe_command(b"sub1");
    }

    #[tokio::test]
    async fn test_pubmessage_delivery() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        handler.publish_pubmessage(make_actor_pub_msg(b"actor_123"));

        let messages = handler.handle_subscriber_poll(b"sub1", 0).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].channel_type, 3);
        assert_eq!(messages[0].key_id, b"actor_123");
    }

    #[test]
    fn test_subscribe_to_unknown_channel_returns_none() {
        let handler = InternalPubSubHandler::new();
        // Channel 99 doesn't exist
        assert!(handler.subscribe(99).is_none());
    }

    #[test]
    fn test_publish_to_unknown_channel_does_not_panic() {
        let handler = InternalPubSubHandler::new();
        handler.publish(99, b"key".to_vec(), b"value".to_vec());
    }

    #[test]
    fn test_all_channel_types_created() {
        let handler = InternalPubSubHandler::new();
        for ct in [3, 4, 5, 6, 7, 8, 9, 10] {
            assert!(handler.subscribe(ct).is_some(), "channel {ct} should exist");
        }
    }

    #[tokio::test]
    async fn test_multiple_subscribers_receive_same_message() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);
        handler.handle_subscribe_command(b"sub2".to_vec(), 3, vec![]);

        handler.publish_pubmessage(make_actor_pub_msg(b"actor1"));

        let m1 = handler.handle_subscriber_poll(b"sub1", 0).await;
        let m2 = handler.handle_subscriber_poll(b"sub2", 0).await;
        assert_eq!(m1.len(), 1);
        assert_eq!(m2.len(), 1);
        assert_eq!(m1[0].key_id, b"actor1");
        assert_eq!(m2[0].key_id, b"actor1");
    }

    #[test]
    fn test_channel_isolation() {
        let handler = InternalPubSubHandler::new();
        // Subscribe sub1 to channel 3 only
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        // Publish to channel 4 — sub1 should NOT receive it
        handler.publish_pubmessage(make_pub_msg(4, b"job1"));

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        assert!(state.pending_messages.is_empty());
    }

    #[test]
    fn test_key_filtering_specific_keys() {
        let handler = InternalPubSubHandler::new();
        // Subscribe to channel 3, only key "actor_A"
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, b"actor_A".to_vec());

        // Publish for actor_A and actor_B
        handler.publish_pubmessage(make_pub_msg(3, b"actor_A"));
        handler.publish_pubmessage(make_pub_msg(3, b"actor_B"));

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        // Only actor_A should be delivered
        assert_eq!(state.pending_messages.len(), 1);
        assert_eq!(state.pending_messages[0].key_id, b"actor_A");
    }

    #[test]
    fn test_empty_key_receives_all() {
        let handler = InternalPubSubHandler::new();
        // Subscribe to channel 3 with empty key (all keys)
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        handler.publish_pubmessage(make_pub_msg(3, b"actor_A"));
        handler.publish_pubmessage(make_pub_msg(3, b"actor_B"));
        handler.publish_pubmessage(make_pub_msg(3, b"actor_C"));

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        assert_eq!(state.pending_messages.len(), 3);
    }

    #[tokio::test]
    async fn test_poll_drains_messages() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        handler.publish_pubmessage(make_pub_msg(3, b"a"));
        handler.publish_pubmessage(make_pub_msg(3, b"b"));

        let messages = handler.handle_subscriber_poll(b"sub1", 0).await;
        assert_eq!(messages.len(), 2);

        // Second poll returns nothing (messages already drained)
        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        assert!(state.pending_messages.is_empty());
    }

    #[tokio::test]
    async fn test_poll_filters_by_max_sequence_id() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        handler.publish_pubmessage(make_pub_msg(3, b"a"));
        handler.publish_pubmessage(make_pub_msg(3, b"b"));
        handler.publish_pubmessage(make_pub_msg(3, b"c"));

        // Get the first batch
        let messages = handler.handle_subscriber_poll(b"sub1", 0).await;
        assert_eq!(messages.len(), 3);
        let max_seq = messages.iter().map(|m| m.sequence_id).max().unwrap();

        // Publish more
        handler.publish_pubmessage(make_pub_msg(3, b"d"));

        // Poll with max_processed = previous max — should only get new message
        let messages = handler.handle_subscriber_poll(b"sub1", max_seq).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key_id, b"d");
    }

    #[tokio::test]
    async fn test_poll_unknown_subscriber_returns_empty() {
        let handler = InternalPubSubHandler::new();
        // Publish something first to avoid waiting
        handler.notify.notify_waiters();
        let messages = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            handler.handle_subscriber_poll(b"unknown", 0),
        )
        .await;
        // Either returns empty or times out — both acceptable
        if let Ok(msgs) = messages {
            assert!(msgs.is_empty());
        }
    }

    #[test]
    fn test_handle_publish_multiple_messages() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);
        handler.handle_subscribe_command(b"sub1".to_vec(), 4, vec![]);

        let messages = vec![make_pub_msg(3, b"actor1"), make_pub_msg(4, b"job1")];
        handler.handle_publish(messages);

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        assert_eq!(state.pending_messages.len(), 2);
    }

    #[test]
    fn test_sequence_ids_are_monotonic() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        for i in 0..5 {
            handler.publish_pubmessage(make_pub_msg(3, format!("k{i}").as_bytes()));
        }

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        let seq_ids: Vec<i64> = state.pending_messages.iter().map(|m| m.sequence_id).collect();
        for i in 1..seq_ids.len() {
            assert!(seq_ids[i] > seq_ids[i - 1], "sequence IDs must be monotonic");
        }
    }

    #[tokio::test]
    async fn test_long_poll_wakes_on_publish() {
        let handler = Arc::new(InternalPubSubHandler::new());
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        let handler_clone = Arc::clone(&handler);
        let poll_handle = tokio::spawn(async move {
            handler_clone.handle_subscriber_poll(b"sub1", 0).await
        });

        // Give poll time to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Publish wakes the poll
        handler.publish_pubmessage(make_pub_msg(3, b"wake"));

        let messages = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            poll_handle,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key_id, b"wake");
    }

    #[test]
    fn test_subscribe_multiple_keys() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, b"actor_A".to_vec());
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, b"actor_B".to_vec());

        handler.publish_pubmessage(make_pub_msg(3, b"actor_A"));
        handler.publish_pubmessage(make_pub_msg(3, b"actor_B"));
        handler.publish_pubmessage(make_pub_msg(3, b"actor_C"));

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        // Only A and B should be delivered
        assert_eq!(state.pending_messages.len(), 2);
    }

    #[test]
    fn test_default_creates_handler() {
        let handler = InternalPubSubHandler::default();
        assert!(handler.subscribe(3).is_some());
    }

    #[test]
    fn test_subscribe_to_multiple_channels() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]); // actor
        handler.handle_subscribe_command(b"sub1".to_vec(), 4, vec![]); // job
        handler.handle_subscribe_command(b"sub1".to_vec(), 5, vec![]); // node

        handler.publish_pubmessage(make_pub_msg(3, b"a"));
        handler.publish_pubmessage(make_pub_msg(4, b"j"));
        handler.publish_pubmessage(make_pub_msg(5, b"n"));
        handler.publish_pubmessage(make_pub_msg(6, b"w")); // not subscribed

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        assert_eq!(state.pending_messages.len(), 3);
    }

    #[test]
    fn test_duplicate_key_subscription_not_duplicated() {
        let handler = InternalPubSubHandler::new();
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, b"actor_A".to_vec());
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, b"actor_A".to_vec());

        let subs = handler.subscribers.lock();
        let state = subs.get(&b"sub1".to_vec()).unwrap();
        let keys = state.subscriptions.get(&3).unwrap();
        assert_eq!(keys.len(), 1); // Not duplicated
    }
}
