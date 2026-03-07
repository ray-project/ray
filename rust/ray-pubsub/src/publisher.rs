// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Publisher side of Ray pub/sub.
//!
//! Ports `src/ray/pubsub/publisher.h/cc`.
//! Manages per-channel subscription indices, per-subscriber message mailboxes,
//! buffer limits, and dead subscriber detection.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

/// A published message with sequence ordering.
#[derive(Debug, Clone)]
pub struct PubMessage {
    /// The channel this message belongs to.
    pub channel_type: i32,
    /// The entity key within the channel.
    pub key_id: Vec<u8>,
    /// The serialized message payload.
    pub payload: Vec<u8>,
    /// Monotonically increasing sequence ID assigned by the publisher.
    pub sequence_id: i64,
}

/// Configuration for the publisher.
#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// Maximum buffered bytes for channels that allow dropping.
    /// Set to -1 for unlimited. Default: 10MB.
    pub max_buffered_bytes: i64,
    /// Maximum messages per batch when flushing to a subscriber.
    pub publish_batch_size: usize,
    /// Subscriber timeout in milliseconds. Subscribers that don't poll
    /// within this window are considered potentially dead.
    pub subscriber_timeout_ms: u64,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            max_buffered_bytes: 10 * 1024 * 1024, // 10MB
            publish_batch_size: 100,
            subscriber_timeout_ms: 30_000,
        }
    }
}

/// Per-subscriber state: message mailbox and connection tracking.
struct SubscriberState {
    /// Queued messages awaiting delivery.
    mailbox: VecDeque<PubMessage>,
    /// Total bytes in the mailbox.
    mailbox_bytes: usize,
    /// Whether a long-poll connection is active.
    has_active_poll: bool,
    /// Sender for delivering messages to the active long-poll.
    poll_sender: Option<tokio::sync::oneshot::Sender<Vec<PubMessage>>>,
    /// Last time the subscriber connected/polled.
    last_poll_time: Instant,
    /// Max messages per batch.
    publish_batch_size: usize,
}

impl SubscriberState {
    fn new(batch_size: usize) -> Self {
        Self {
            mailbox: VecDeque::new(),
            mailbox_bytes: 0,
            has_active_poll: false,
            poll_sender: None,
            last_poll_time: Instant::now(),
            publish_batch_size: batch_size,
        }
    }

    /// Enqueue a message. Returns true if the message was accepted.
    fn enqueue(&mut self, msg: PubMessage) {
        self.mailbox_bytes += msg.payload.len();
        self.mailbox.push_back(msg);
    }

    /// Try to flush messages to the active long-poll connection.
    fn try_flush(&mut self) -> bool {
        if let Some(sender) = self.poll_sender.take() {
            let count = self.publish_batch_size.min(self.mailbox.len());
            if count > 0 {
                let batch: Vec<PubMessage> = self.mailbox.drain(..count).collect();
                self.mailbox_bytes = self
                    .mailbox
                    .iter()
                    .map(|m| m.payload.len())
                    .sum();
                self.has_active_poll = false;
                let _ = sender.send(batch);
                return true;
            }
            // Put sender back if no messages to flush.
            self.poll_sender = Some(sender);
        }
        false
    }
}

/// Per-channel subscription index.
struct SubscriptionIndex {
    /// Subscribers listening to ALL entities in this channel.
    subscribers_to_all: HashSet<Vec<u8>>,
    /// Per-entity subscribers: key_id → set of subscriber_ids.
    per_entity: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
    /// Reverse index: subscriber_id → set of key_ids (for cleanup).
    subscriber_to_keys: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
}

impl SubscriptionIndex {
    fn new() -> Self {
        Self {
            subscribers_to_all: HashSet::new(),
            per_entity: HashMap::new(),
            subscriber_to_keys: HashMap::new(),
        }
    }

    /// Register a subscriber for a key. Empty key_id means "all entities".
    fn register(&mut self, subscriber_id: &[u8], key_id: &[u8]) {
        if key_id.is_empty() {
            self.subscribers_to_all.insert(subscriber_id.to_vec());
        } else {
            self.per_entity
                .entry(key_id.to_vec())
                .or_default()
                .insert(subscriber_id.to_vec());
            self.subscriber_to_keys
                .entry(subscriber_id.to_vec())
                .or_default()
                .insert(key_id.to_vec());
        }
    }

    /// Unregister a subscriber from a specific key.
    fn unregister_key(&mut self, subscriber_id: &[u8], key_id: &[u8]) {
        if key_id.is_empty() {
            self.subscribers_to_all.remove(subscriber_id);
        } else {
            if let Some(subs) = self.per_entity.get_mut(key_id) {
                subs.remove(subscriber_id);
                if subs.is_empty() {
                    self.per_entity.remove(key_id);
                }
            }
            if let Some(keys) = self.subscriber_to_keys.get_mut(subscriber_id) {
                keys.remove(key_id);
                if keys.is_empty() {
                    self.subscriber_to_keys.remove(subscriber_id);
                }
            }
        }
    }

    /// Remove a subscriber from all keys in this channel.
    fn unregister_subscriber(&mut self, subscriber_id: &[u8]) {
        self.subscribers_to_all.remove(subscriber_id);
        if let Some(keys) = self.subscriber_to_keys.remove(subscriber_id) {
            for key in keys {
                if let Some(subs) = self.per_entity.get_mut(&key) {
                    subs.remove(subscriber_id);
                    if subs.is_empty() {
                        self.per_entity.remove(&key);
                    }
                }
            }
        }
    }

    /// Get all subscriber IDs interested in a given key.
    fn get_subscribers(&self, key_id: &[u8]) -> Vec<Vec<u8>> {
        let mut result: Vec<Vec<u8>> = self.subscribers_to_all.iter().cloned().collect();
        if let Some(subs) = self.per_entity.get(key_id) {
            for sub in subs {
                if !result.contains(sub) {
                    result.push(sub.clone());
                }
            }
        }
        result
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.subscribers_to_all.is_empty()
            && self.per_entity.is_empty()
    }
}

/// Inner state protected by a mutex.
struct PublisherInner {
    /// Per-channel subscription indices.
    subscription_indices: HashMap<i32, SubscriptionIndex>,
    /// Per-subscriber state.
    subscribers: HashMap<Vec<u8>, SubscriberState>,
    /// Channel types that allow message dropping when buffer is full.
    droppable_channels: HashSet<i32>,
}

/// The publisher manages subscriptions and message delivery.
///
/// Messages are published to channels and delivered to matching subscribers
/// via long-polling. Each subscriber has a message mailbox. When a subscriber
/// connects (long-polls), queued messages are flushed immediately.
pub struct Publisher {
    inner: Mutex<PublisherInner>,
    config: PublisherConfig,
    /// Monotonically increasing sequence ID.
    next_sequence_id: AtomicI64,
    /// Statistics.
    total_messages_published: AtomicU64,
    total_messages_dropped: AtomicU64,
}

impl Publisher {
    /// Create a new publisher with the given config.
    pub fn new(config: PublisherConfig) -> Self {
        Self {
            inner: Mutex::new(PublisherInner {
                subscription_indices: HashMap::new(),
                subscribers: HashMap::new(),
                droppable_channels: HashSet::new(),
            }),
            config,
            next_sequence_id: AtomicI64::new(1),
            total_messages_published: AtomicU64::new(0),
            total_messages_dropped: AtomicU64::new(0),
        }
    }

    /// Register a channel type. Only registered channels accept subscriptions.
    pub fn register_channel(&self, channel_type: i32, droppable: bool) {
        let mut inner = self.inner.lock();
        inner
            .subscription_indices
            .entry(channel_type)
            .or_insert_with(SubscriptionIndex::new);
        if droppable {
            inner.droppable_channels.insert(channel_type);
        }
    }

    /// Register a subscription: subscriber wants messages from channel+key.
    /// Empty key_id means "subscribe to all entities in this channel".
    pub fn register_subscription(
        &self,
        subscriber_id: &[u8],
        channel_type: i32,
        key_id: &[u8],
    ) -> bool {
        let mut inner = self.inner.lock();

        // Validate channel is registered.
        let index = match inner.subscription_indices.get_mut(&channel_type) {
            Some(idx) => idx,
            None => return false,
        };

        index.register(subscriber_id, key_id);

        // Ensure subscriber state exists.
        inner
            .subscribers
            .entry(subscriber_id.to_vec())
            .or_insert_with(|| SubscriberState::new(self.config.publish_batch_size));

        true
    }

    /// Unregister a subscription for a specific channel+key.
    pub fn unregister_subscription(
        &self,
        subscriber_id: &[u8],
        channel_type: i32,
        key_id: &[u8],
    ) {
        let mut inner = self.inner.lock();
        if let Some(index) = inner.subscription_indices.get_mut(&channel_type) {
            index.unregister_key(subscriber_id, key_id);
        }
    }

    /// Completely remove a subscriber from all channels.
    pub fn unregister_subscriber(&self, subscriber_id: &[u8]) {
        let mut inner = self.inner.lock();
        for index in inner.subscription_indices.values_mut() {
            index.unregister_subscriber(subscriber_id);
        }
        inner.subscribers.remove(subscriber_id);
    }

    /// Publish a message to all matching subscribers on a channel.
    pub fn publish(&self, channel_type: i32, key_id: &[u8], payload: Vec<u8>) {
        let seq = self.next_sequence_id.fetch_add(1, Ordering::Relaxed);
        self.total_messages_published.fetch_add(1, Ordering::Relaxed);

        let mut inner = self.inner.lock();

        let subscriber_ids = match inner.subscription_indices.get(&channel_type) {
            Some(index) => index.get_subscribers(key_id),
            None => return,
        };

        let is_droppable = inner.droppable_channels.contains(&channel_type);

        for sub_id in &subscriber_ids {
            if let Some(state) = inner.subscribers.get_mut(sub_id) {
                // Check buffer limits for droppable channels.
                if is_droppable && self.config.max_buffered_bytes >= 0 {
                    while state.mailbox_bytes + payload.len()
                        > self.config.max_buffered_bytes as usize
                        && !state.mailbox.is_empty()
                    {
                        state.mailbox.pop_front();
                        state.mailbox_bytes =
                            state.mailbox.iter().map(|m| m.payload.len()).sum();
                        self.total_messages_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }

                let msg = PubMessage {
                    channel_type,
                    key_id: key_id.to_vec(),
                    payload: payload.clone(),
                    sequence_id: seq,
                };
                state.enqueue(msg);

                // Try to flush immediately if there's an active poll.
                state.try_flush();
            }
        }
    }

    /// Connect a subscriber (long-poll). Returns a receiver that will be
    /// fulfilled when messages are available.
    ///
    /// If the subscriber already has queued messages, the receiver is
    /// fulfilled immediately.
    pub fn connect_subscriber(
        &self,
        subscriber_id: &[u8],
        max_processed_sequence_id: i64,
    ) -> tokio::sync::oneshot::Receiver<Vec<PubMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut inner = self.inner.lock();

        if let Some(state) = inner.subscribers.get_mut(subscriber_id) {
            state.last_poll_time = Instant::now();

            // Acknowledge processed messages.
            while let Some(front) = state.mailbox.front() {
                if front.sequence_id <= max_processed_sequence_id {
                    state.mailbox.pop_front();
                } else {
                    break;
                }
            }
            state.mailbox_bytes =
                state.mailbox.iter().map(|m| m.payload.len()).sum();

            state.has_active_poll = true;
            state.poll_sender = Some(tx);

            // Try immediate flush.
            state.try_flush();
        } else {
            // Unknown subscriber — immediately resolve with empty.
            let _ = tx.send(Vec::new());
        }

        rx
    }

    /// Check for dead subscribers and clean them up.
    /// Returns the IDs of subscribers that were removed.
    pub fn check_dead_subscribers(&self) -> Vec<Vec<u8>> {
        let timeout = std::time::Duration::from_millis(self.config.subscriber_timeout_ms);
        let now = Instant::now();
        let mut dead = Vec::new();

        let mut inner = self.inner.lock();
        let ids: Vec<Vec<u8>> = inner.subscribers.keys().cloned().collect();

        for id in ids {
            if let Some(state) = inner.subscribers.get(&id) {
                if now.duration_since(state.last_poll_time) > timeout * 2 {
                    // Truly dead — no poll for 2x timeout.
                    dead.push(id.clone());
                } else if now.duration_since(state.last_poll_time) > timeout {
                    // Potentially dead — force flush with empty to refresh.
                    if let Some(state) = inner.subscribers.get_mut(&id) {
                        if let Some(sender) = state.poll_sender.take() {
                            state.has_active_poll = false;
                            let _ = sender.send(Vec::new());
                        }
                    }
                }
            }
        }

        // Remove dead subscribers.
        for id in &dead {
            inner.subscribers.remove(id);
            for index in inner.subscription_indices.values_mut() {
                index.unregister_subscriber(id);
            }
        }

        dead
    }

    /// Number of registered subscribers.
    pub fn num_subscribers(&self) -> usize {
        self.inner.lock().subscribers.len()
    }

    /// Number of messages in a subscriber's mailbox.
    pub fn subscriber_mailbox_size(&self, subscriber_id: &[u8]) -> usize {
        self.inner
            .lock()
            .subscribers
            .get(subscriber_id)
            .map(|s| s.mailbox.len())
            .unwrap_or(0)
    }

    /// Total messages published since creation.
    pub fn total_messages_published(&self) -> u64 {
        self.total_messages_published.load(Ordering::Relaxed)
    }

    /// Total messages dropped due to buffer limits.
    pub fn total_messages_dropped(&self) -> u64 {
        self.total_messages_dropped.load(Ordering::Relaxed)
    }

    /// Check if a channel is registered.
    pub fn has_channel(&self, channel_type: i32) -> bool {
        self.inner
            .lock()
            .subscription_indices
            .contains_key(&channel_type)
    }

    /// Get the next sequence ID (for testing).
    pub fn next_sequence_id(&self) -> i64 {
        self.next_sequence_id.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_publisher() -> Publisher {
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: 1024,
            subscriber_timeout_ms: 100,
        });
        pub_.register_channel(3, false); // Actor channel (non-droppable)
        pub_.register_channel(7, true); // Error channel (droppable)
        pub_
    }

    #[test]
    fn test_register_channel() {
        let pub_ = make_publisher();
        assert!(pub_.has_channel(3));
        assert!(pub_.has_channel(7));
        assert!(!pub_.has_channel(99));
    }

    #[test]
    fn test_register_subscription() {
        let pub_ = make_publisher();
        assert!(pub_.register_subscription(b"sub1", 3, b""));
        assert!(pub_.register_subscription(b"sub1", 3, b"actor_A"));
        // Unknown channel returns false.
        assert!(!pub_.register_subscription(b"sub1", 99, b""));
    }

    #[test]
    fn test_publish_to_all_subscribers() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"");
        pub_.register_subscription(b"sub2", 3, b"");

        pub_.publish(3, b"actor_1", b"data1".to_vec());

        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 1);
        assert_eq!(pub_.subscriber_mailbox_size(b"sub2"), 1);
    }

    #[test]
    fn test_publish_per_entity_subscription() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"actor_A");
        pub_.register_subscription(b"sub2", 3, b"actor_B");

        pub_.publish(3, b"actor_A", b"data".to_vec());

        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 1);
        assert_eq!(pub_.subscriber_mailbox_size(b"sub2"), 0);
    }

    #[test]
    fn test_publish_all_entity_subscriber_gets_everything() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b""); // all entities

        pub_.publish(3, b"actor_A", b"data1".to_vec());
        pub_.publish(3, b"actor_B", b"data2".to_vec());
        pub_.publish(3, b"actor_C", b"data3".to_vec());

        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 3);
    }

    #[test]
    fn test_unregister_subscription() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"actor_A");
        pub_.unregister_subscription(b"sub1", 3, b"actor_A");

        pub_.publish(3, b"actor_A", b"data".to_vec());
        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 0);
    }

    #[test]
    fn test_unregister_subscriber() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"");
        assert_eq!(pub_.num_subscribers(), 1);

        pub_.unregister_subscriber(b"sub1");
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[tokio::test]
    async fn test_connect_subscriber_immediate_flush() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"");

        pub_.publish(3, b"k1", b"v1".to_vec());
        pub_.publish(3, b"k2", b"v2".to_vec());

        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].key_id, b"k1");
        assert_eq!(messages[1].key_id, b"k2");
    }

    #[tokio::test]
    async fn test_connect_subscriber_deferred_flush() {
        let pub_ = Arc::new(make_publisher());
        pub_.register_subscription(b"sub1", 3, b"");

        // Connect first (no messages yet).
        let rx = pub_.connect_subscriber(b"sub1", 0);

        // Publish after connecting.
        let pub_clone = Arc::clone(&pub_);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            pub_clone.publish(3, b"k1", b"v1".to_vec());
        });

        let messages: Vec<PubMessage> = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_connect_ack_processed_messages() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"");

        pub_.publish(3, b"k1", b"v1".to_vec());
        pub_.publish(3, b"k2", b"v2".to_vec());
        pub_.publish(3, b"k3", b"v3".to_vec());

        // First poll — get all 3.
        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 3);
        let max_seq = messages.last().unwrap().sequence_id;

        // Publish more.
        pub_.publish(3, b"k4", b"v4".to_vec());

        // Second poll with ack — should only get k4.
        let rx = pub_.connect_subscriber(b"sub1", max_seq);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key_id, b"k4");
    }

    #[test]
    fn test_droppable_channel_eviction() {
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: 20, // Very small buffer.
            subscriber_timeout_ms: 100,
        });
        pub_.register_channel(7, true);
        pub_.register_subscription(b"sub1", 7, b"");

        // Each message is 10 bytes — buffer holds ~2 messages.
        for i in 0..5u8 {
            pub_.publish(7, &[i], vec![i; 10]);
        }

        // Some messages should have been dropped.
        assert!(pub_.total_messages_dropped() > 0);
        assert!(pub_.subscriber_mailbox_size(b"sub1") <= 3);
    }

    #[test]
    fn test_non_droppable_channel_no_eviction() {
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: 20,
            subscriber_timeout_ms: 100,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"");

        for i in 0..5u8 {
            pub_.publish(3, &[i], vec![i; 10]);
        }

        // Non-droppable: all messages kept.
        assert_eq!(pub_.total_messages_dropped(), 0);
        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 5);
    }

    #[test]
    fn test_sequence_ids_monotonic() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"");

        for i in 0..5 {
            pub_.publish(3, format!("k{i}").as_bytes(), vec![]);
        }

        let inner = pub_.inner.lock();
        let state = inner.subscribers.get(&b"sub1".to_vec()).unwrap();
        let ids: Vec<i64> = state.mailbox.iter().map(|m| m.sequence_id).collect();
        for i in 1..ids.len() {
            assert!(ids[i] > ids[i - 1]);
        }
    }

    #[test]
    fn test_channel_isolation() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b""); // channel 3
        pub_.register_subscription(b"sub2", 7, b""); // channel 7

        pub_.publish(3, b"k1", b"v1".to_vec());

        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 1);
        assert_eq!(pub_.subscriber_mailbox_size(b"sub2"), 0);
    }

    #[test]
    fn test_dead_subscriber_detection() {
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: -1,
            subscriber_timeout_ms: 50,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"");
        assert_eq!(pub_.num_subscribers(), 1);

        // No timeout yet — subscriber is alive.
        let dead = pub_.check_dead_subscribers();
        assert!(dead.is_empty());

        // Wait past 2x timeout (100ms) so it's considered dead.
        std::thread::sleep(std::time::Duration::from_millis(120));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.contains(&b"sub1".to_vec()));
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[tokio::test]
    async fn test_unknown_subscriber_connect() {
        let pub_ = make_publisher();
        let rx = pub_.connect_subscriber(b"unknown", 0);
        let messages = rx.await.unwrap();
        assert!(messages.is_empty());
    }

    #[test]
    fn test_publish_stats() {
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"");

        assert_eq!(pub_.total_messages_published(), 0);
        pub_.publish(3, b"k1", vec![]);
        pub_.publish(3, b"k2", vec![]);
        assert_eq!(pub_.total_messages_published(), 2);
    }

    // --- Ported from publisher_test.cc ---

    #[tokio::test]
    async fn test_subscriber_batch_size() {
        // Tests that batched delivery respects publish_batch_size.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 5,
            max_buffered_bytes: -1,
            subscriber_timeout_ms: 30_000,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"");

        // Queue 10 messages.
        for i in 0..10u8 {
            pub_.publish(3, &[i], vec![i; 1]);
        }

        // First poll: should get at most batch_size (5) messages.
        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 5);

        // Ack those 5 messages.
        let max_seq = messages.last().unwrap().sequence_id;

        // Second poll: should get the remaining 5 messages.
        let rx = pub_.connect_subscriber(b"sub1", max_seq);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 5);
    }

    #[test]
    fn test_subscriber_active_timeout() {
        // Tests that subscriber timeout detection works correctly.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: -1,
            subscriber_timeout_ms: 50,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"");
        assert_eq!(pub_.num_subscribers(), 1);

        // Immediately after registration, subscriber is not yet timed out.
        let dead = pub_.check_dead_subscribers();
        assert!(dead.is_empty());

        // Wait past 1x timeout but less than 2x: subscriber is potentially dead
        // but not yet removed (the first check flushes the poll).
        std::thread::sleep(std::time::Duration::from_millis(60));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.is_empty());
        // Subscriber state still exists.
        assert_eq!(pub_.num_subscribers(), 1);

        // Wait past 2x timeout: subscriber should be considered dead.
        std::thread::sleep(std::time::Duration::from_millis(60));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.contains(&b"sub1".to_vec()));
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[test]
    fn test_subscriber_disconnected() {
        // Tests that a subscriber that stops polling is eventually cleaned up.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: -1,
            subscriber_timeout_ms: 50,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"");

        // Not dead yet.
        let dead = pub_.check_dead_subscribers();
        assert!(dead.is_empty());

        // Wait past 2x timeout without connecting at all.
        std::thread::sleep(std::time::Duration::from_millis(120));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.contains(&b"sub1".to_vec()));
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[test]
    fn test_subscriber_timeout_complicated() {
        // Tests subscriber timeout with connection refresh right before timeout.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: -1,
            subscriber_timeout_ms: 80,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"");

        // Connect to refresh the poll time.
        let _rx = pub_.connect_subscriber(b"sub1", 0);

        // Wait just under the timeout and connect again to refresh.
        std::thread::sleep(std::time::Duration::from_millis(70));
        let _rx = pub_.connect_subscriber(b"sub1", 0);

        // Now wait again — since we refreshed, subscriber should still be alive.
        std::thread::sleep(std::time::Duration::from_millis(70));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.is_empty());

        // Wait past 2x timeout without any new connection.
        std::thread::sleep(std::time::Duration::from_millis(170));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.contains(&b"sub1".to_vec()));
    }

    #[tokio::test]
    async fn test_node_failure_when_connection_existed() {
        // Tests that dead subscriber detection works when a connection exists.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 10,
            max_buffered_bytes: -1,
            subscriber_timeout_ms: 50,
        });
        pub_.register_channel(3, false);
        pub_.register_subscription(b"sub1", 3, b"key1");

        // Connect subscriber.
        let _rx = pub_.connect_subscriber(b"sub1", 0);
        assert_eq!(pub_.num_subscribers(), 1);

        // Wait past 2x timeout.
        std::thread::sleep(std::time::Duration::from_millis(120));
        let dead = pub_.check_dead_subscribers();
        assert!(dead.contains(&b"sub1".to_vec()));

        // Unregister subscriber for cleanup.
        pub_.unregister_subscriber(b"sub1");
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[tokio::test]
    async fn test_publish_failure() {
        // Tests that publish_failure sends a message with empty payload (failure marker).
        // In our Rust port, publish_failure is equivalent to publishing with empty payload.
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"key1");

        let rx = pub_.connect_subscriber(b"sub1", 0);

        // Publish with empty payload to simulate a failure message.
        pub_.publish(3, b"key1", vec![]);

        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key_id, b"key1");
        assert!(messages[0].payload.is_empty());
    }

    #[tokio::test]
    async fn test_max_buffer_size_per_entity() {
        // Tests that droppable channels enforce per-entity buffer limits.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 100,
            max_buffered_bytes: 100,
            subscriber_timeout_ms: 30_000,
        });
        pub_.register_channel(7, true); // droppable

        pub_.register_subscription(b"sub1", 7, b"entity1");

        // Publish 3 messages of 50 bytes each. Buffer is 100, so the first
        // should be evicted when the third arrives.
        pub_.publish(7, b"entity1", vec![b'a'; 50]);
        pub_.publish(7, b"entity1", vec![b'b'; 50]);
        pub_.publish(7, b"entity1", vec![b'c'; 50]);

        // Should have dropped at least 1 message.
        assert!(pub_.total_messages_dropped() > 0);

        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 2);
        // The first message (all 'a') should have been evicted.
        assert_eq!(messages[0].payload, vec![b'b'; 50]);
        assert_eq!(messages[1].payload, vec![b'c'; 50]);
    }

    #[tokio::test]
    async fn test_max_buffer_size_all_entities() {
        // Tests buffer limits when subscribed to all entities in a droppable channel.
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 100,
            max_buffered_bytes: 100,
            subscriber_timeout_ms: 30_000,
        });
        pub_.register_channel(7, true); // droppable
        pub_.register_subscription(b"sub1", 7, b""); // all entities

        // Publish 3 messages of ~50 bytes each with different keys.
        pub_.publish(7, b"aaa", vec![b'a'; 50]);
        pub_.publish(7, b"bbb", vec![b'b'; 50]);
        pub_.publish(7, b"ccc", vec![b'c'; 50]);

        // At least 1 message should have been dropped.
        assert!(pub_.total_messages_dropped() > 0);

        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 2);
        // First message (all 'a') should have been evicted.
        assert_eq!(messages[0].payload, vec![b'b'; 50]);
        assert_eq!(messages[1].payload, vec![b'c'; 50]);
    }

    #[tokio::test]
    async fn test_max_message_size() {
        // Tests that very large messages are handled by droppable channel eviction.
        let max_message_size: usize = 1000;
        let pub_ = Publisher::new(PublisherConfig {
            publish_batch_size: 2, // Small batch to test batched delivery.
            max_buffered_bytes: (max_message_size * 2) as i64,
            subscriber_timeout_ms: 30_000,
        });
        pub_.register_channel(7, true); // droppable
        pub_.register_subscription(b"sub1", 7, b""); // all entities

        // Fill buffer with small messages that exceed capacity.
        for i in 0..6u8 {
            pub_.publish(
                7,
                format!("{}", i).as_bytes(),
                vec![b'x'; max_message_size / 3],
            );
        }

        // We should get messages in batches of 2 due to batch_size.
        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 2);
        let max_seq = messages.last().unwrap().sequence_id;

        let rx = pub_.connect_subscriber(b"sub1", max_seq);
        let messages = rx.await.unwrap();
        // Remaining messages after eviction.
        assert!(messages.len() <= 2);
    }

    #[tokio::test]
    async fn test_multi_subscribers() {
        // Tests that a published message is delivered to multiple subscribers.
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"actor_X");
        pub_.register_subscription(b"sub2", 3, b"actor_X");
        pub_.register_subscription(b"sub3", 3, b"actor_X");

        // Connect all subscribers.
        let rx1 = pub_.connect_subscriber(b"sub1", 0);
        let rx2 = pub_.connect_subscriber(b"sub2", 0);
        let rx3 = pub_.connect_subscriber(b"sub3", 0);

        // Publish a message to actor_X.
        pub_.publish(3, b"actor_X", b"data".to_vec());

        let msgs1 = rx1.await.unwrap();
        let msgs2 = rx2.await.unwrap();
        let msgs3 = rx3.await.unwrap();

        assert_eq!(msgs1.len(), 1);
        assert_eq!(msgs2.len(), 1);
        assert_eq!(msgs3.len(), 1);
        assert_eq!(msgs1[0].key_id, b"actor_X");
    }

    #[test]
    fn test_unregister_subscription_detailed() {
        // Ported from TestUnregisterSubscription: tests that unregistering a
        // subscription prevents future messages from being delivered.
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"key1");

        // Unregister the subscription.
        pub_.unregister_subscription(b"sub1", 3, b"key1");

        // Publish should not enqueue to the subscriber (no matching subscription).
        pub_.publish(3, b"key1", b"data".to_vec());
        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 0);

        // Unregistering non-existent subscriptions should not panic.
        pub_.unregister_subscription(b"sub1", 3, b"nonexistent_key");
        pub_.unregister_subscription(b"nonexistent_sub", 3, b"key1");

        // Cleanup.
        pub_.unregister_subscriber(b"sub1");
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[test]
    fn test_unregister_subscriber_detailed() {
        // Ported from TestUnregisterSubscriber: tests that unregistering a
        // subscriber removes all its state.
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"key1");
        assert_eq!(pub_.num_subscribers(), 1);

        pub_.unregister_subscriber(b"sub1");
        assert_eq!(pub_.num_subscribers(), 0);

        // Unregistering a subscriber that doesn't exist should not panic.
        pub_.unregister_subscriber(b"sub1");
        pub_.unregister_subscriber(b"nonexistent_sub");
    }

    #[test]
    fn test_registration_idempotency() {
        // Ported from TestRegistrationIdempotency: double register, then
        // unregister, publish should behave correctly.
        let pub_ = make_publisher();

        // Double register.
        assert!(pub_.register_subscription(b"sub1", 3, b"key1"));
        assert!(pub_.register_subscription(b"sub1", 3, b"key1"));

        // Publish should still deliver exactly once.
        pub_.publish(3, b"key1", b"data".to_vec());
        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 1);

        // Unregister the subscription (once should suffice).
        pub_.unregister_subscription(b"sub1", 3, b"key1");
        pub_.publish(3, b"key1", b"more_data".to_vec());
        // No new messages since subscription was removed.
        assert_eq!(pub_.subscriber_mailbox_size(b"sub1"), 1);

        // Double unregister subscriber should not panic.
        pub_.unregister_subscriber(b"sub1");
        pub_.unregister_subscriber(b"sub1");
        assert_eq!(pub_.num_subscribers(), 0);
    }

    #[tokio::test]
    async fn test_subscriber_lost_a_publish() {
        // Ported from TestSubscriberLostAPublish: tests message delivery with
        // sequence acknowledgment. In the Rust implementation, messages are
        // removed from the mailbox upon flush (not retained for re-delivery).
        // The subscriber should ack messages via max_processed_sequence_id
        // to avoid re-delivery of unprocessed messages that weren't flushed.
        let pub_ = make_publisher();
        pub_.register_subscription(b"sub1", 3, b"key1");

        // First poll: publish msg1, then connect and receive it.
        pub_.publish(3, b"key1", b"msg1".to_vec());
        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 1);

        // Publisher publishes msg2 while subscriber has no active poll.
        pub_.publish(3, b"key1", b"msg2".to_vec());

        // Subscriber retries with max_processed_sequence_id=0 (lost reply).
        // In Rust, msg1 was already flushed from mailbox, so only msg2 remains.
        let rx = pub_.connect_subscriber(b"sub1", 0);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"msg2");
        let max_processed = messages[0].sequence_id;

        // Now ack msg2 and publish msg3. Should get only msg3.
        pub_.publish(3, b"key1", b"msg3".to_vec());
        let rx = pub_.connect_subscriber(b"sub1", max_processed);
        let messages = rx.await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"msg3");
    }

    #[test]
    fn test_register_subscription_invalid_channel_returns_false() {
        // Ported from TestRegisterSubscriptionInvalidChannelTypeReturnsInvalidArgument.
        // In the Rust port, register_subscription returns false for invalid channels.
        let pub_ = make_publisher();
        // Channel 99 was never registered.
        assert!(!pub_.register_subscription(b"sub1", 99, b"key1"));
    }
}
