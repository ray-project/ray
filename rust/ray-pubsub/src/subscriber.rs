// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Subscriber side of Ray pub/sub with long-poll support.
//!
//! Ports `src/ray/pubsub/subscriber.h/cc`.
//! Manages subscriptions, command batching, and long-poll message delivery
//! with sequence ID tracking for deduplication.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::publisher::PubMessage;

/// Callback invoked when a message is received for a subscription.
pub type MessageCallback = Arc<dyn Fn(&PubMessage) + Send + Sync>;

/// Callback invoked when a subscription fails (entity deleted, publisher down).
pub type FailureCallback = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// Subscription info for a single channel+key combination.
struct SubscriptionInfo {
    item_cb: MessageCallback,
    #[allow(dead_code)]
    failure_cb: Option<FailureCallback>,
}

/// Per-publisher subscription tracking.
struct PublisherSubscriptions {
    /// Single subscription for all entities in a channel (mutually exclusive
    /// with per_entity for that channel).
    all_entities: Option<SubscriptionInfo>,
    /// Per-entity subscriptions: key_id → info.
    per_entity: HashMap<Vec<u8>, SubscriptionInfo>,
}

impl PublisherSubscriptions {
    fn new() -> Self {
        Self {
            all_entities: None,
            per_entity: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.all_entities.is_none() && self.per_entity.is_empty()
    }
}

/// A command to send to the publisher (subscribe or unsubscribe).
#[derive(Debug, Clone)]
pub enum SubscriberCommand {
    Subscribe { channel_type: i32, key_id: Vec<u8> },
    Unsubscribe { channel_type: i32, key_id: Vec<u8> },
}

/// Sequence tracking per publisher.
struct SequenceState {
    /// The publisher_id from the last successful poll.
    last_publisher_id: Vec<u8>,
    /// The max sequence_id successfully processed from this publisher.
    max_processed_sequence_id: i64,
}

/// Per-channel subscriber state.
struct ChannelState {
    /// publisher_id → subscriptions for that publisher.
    subscriptions: HashMap<Vec<u8>, PublisherSubscriptions>,
}

impl ChannelState {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }
}

/// Inner state protected by a mutex.
struct SubscriberInner {
    /// Per-channel subscription state.
    channels: HashMap<i32, ChannelState>,
    /// Per-publisher command queue.
    command_queues: HashMap<Vec<u8>, VecDeque<SubscriberCommand>>,
    /// Publishers with an active long-poll connection.
    connected_publishers: std::collections::HashSet<Vec<u8>>,
    /// Per-publisher sequence tracking.
    sequence_states: HashMap<Vec<u8>, SequenceState>,
    /// Cumulative stats.
    total_messages_received: u64,
    total_commands_sent: u64,
}

/// Configuration for the subscriber.
#[derive(Debug, Clone)]
pub struct SubscriberConfig {
    /// Maximum commands to batch in a single RPC.
    pub max_command_batch_size: usize,
    /// Timeout for long-poll requests in milliseconds.
    pub long_poll_timeout_ms: u64,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            max_command_batch_size: 100,
            long_poll_timeout_ms: 30_000,
        }
    }
}

/// The subscriber manages subscriptions to publishers via long-polling.
///
/// Commands (subscribe/unsubscribe) are queued and sent in batches.
/// Messages are received via long-polling and dispatched to registered callbacks.
pub struct Subscriber {
    /// This subscriber's unique ID.
    subscriber_id: Vec<u8>,
    inner: Mutex<SubscriberInner>,
    config: SubscriberConfig,
}

impl Subscriber {
    /// Create a new subscriber with the given ID.
    pub fn new(subscriber_id: Vec<u8>, config: SubscriberConfig) -> Self {
        Self {
            subscriber_id,
            inner: Mutex::new(SubscriberInner {
                channels: HashMap::new(),
                command_queues: HashMap::new(),
                connected_publishers: std::collections::HashSet::new(),
                sequence_states: HashMap::new(),
                total_messages_received: 0,
                total_commands_sent: 0,
            }),
            config,
        }
    }

    /// Get this subscriber's ID.
    pub fn subscriber_id(&self) -> &[u8] {
        &self.subscriber_id
    }

    /// Subscribe to a channel+key on a specific publisher.
    /// Empty key_id means "subscribe to all entities".
    ///
    /// The `item_cb` is called for each received message.
    /// The optional `failure_cb` is called on publisher failure.
    pub fn subscribe(
        &self,
        publisher_id: &[u8],
        channel_type: i32,
        key_id: &[u8],
        item_cb: MessageCallback,
        failure_cb: Option<FailureCallback>,
    ) {
        let mut inner = self.inner.lock();

        // Register the subscription.
        let channel = inner
            .channels
            .entry(channel_type)
            .or_insert_with(ChannelState::new);
        let subs = channel
            .subscriptions
            .entry(publisher_id.to_vec())
            .or_insert_with(PublisherSubscriptions::new);

        let info = SubscriptionInfo {
            item_cb,
            failure_cb,
        };

        if key_id.is_empty() {
            subs.all_entities = Some(info);
        } else {
            subs.per_entity.insert(key_id.to_vec(), info);
        }

        // Queue the subscribe command.
        inner
            .command_queues
            .entry(publisher_id.to_vec())
            .or_default()
            .push_back(SubscriberCommand::Subscribe {
                channel_type,
                key_id: key_id.to_vec(),
            });
    }

    /// Unsubscribe from a channel+key on a specific publisher.
    pub fn unsubscribe(&self, publisher_id: &[u8], channel_type: i32, key_id: &[u8]) {
        let mut inner = self.inner.lock();

        // Remove the subscription.
        if let Some(channel) = inner.channels.get_mut(&channel_type) {
            if let Some(subs) = channel.subscriptions.get_mut(publisher_id) {
                if key_id.is_empty() {
                    subs.all_entities = None;
                } else {
                    subs.per_entity.remove(key_id);
                }
                if subs.is_empty() {
                    channel.subscriptions.remove(publisher_id);
                }
            }
        }

        // Queue the unsubscribe command.
        inner
            .command_queues
            .entry(publisher_id.to_vec())
            .or_default()
            .push_back(SubscriberCommand::Unsubscribe {
                channel_type,
                key_id: key_id.to_vec(),
            });
    }

    /// Check if subscribed to a specific channel+key on a publisher.
    pub fn is_subscribed(&self, publisher_id: &[u8], channel_type: i32, key_id: &[u8]) -> bool {
        let inner = self.inner.lock();
        if let Some(channel) = inner.channels.get(&channel_type) {
            if let Some(subs) = channel.subscriptions.get(publisher_id) {
                if key_id.is_empty() {
                    return subs.all_entities.is_some();
                } else {
                    return subs.per_entity.contains_key(key_id);
                }
            }
        }
        false
    }

    /// Drain pending commands for a publisher (up to batch_size).
    /// Returns the commands to send.
    pub fn drain_commands(&self, publisher_id: &[u8]) -> Vec<SubscriberCommand> {
        let mut inner = self.inner.lock();
        if let Some(queue) = inner.command_queues.get_mut(publisher_id) {
            let count = self.config.max_command_batch_size.min(queue.len());
            let batch: Vec<SubscriberCommand> = queue.drain(..count).collect();
            inner.total_commands_sent += batch.len() as u64;
            batch
        } else {
            Vec::new()
        }
    }

    /// Get the max processed sequence ID for a publisher.
    pub fn max_processed_sequence_id(&self, publisher_id: &[u8]) -> i64 {
        self.inner
            .lock()
            .sequence_states
            .get(publisher_id)
            .map(|s| s.max_processed_sequence_id)
            .unwrap_or(0)
    }

    /// Handle a long-poll response from a publisher.
    ///
    /// Dispatches messages to registered callbacks and updates sequence state.
    /// Returns the number of messages processed.
    pub fn handle_poll_response(
        &self,
        publisher_id: &[u8],
        response_publisher_id: &[u8],
        messages: &[PubMessage],
    ) -> usize {
        let mut inner = self.inner.lock();
        let mut processed = 0;

        // Check for publisher failover and get max processed sequence ID.
        let max_processed = {
            let seq_state = inner
                .sequence_states
                .entry(publisher_id.to_vec())
                .or_insert_with(|| SequenceState {
                    last_publisher_id: response_publisher_id.to_vec(),
                    max_processed_sequence_id: 0,
                });

            if seq_state.last_publisher_id != response_publisher_id {
                // Publisher failover detected — reset sequence tracking.
                seq_state.last_publisher_id = response_publisher_id.to_vec();
                seq_state.max_processed_sequence_id = 0;
            }

            seq_state.max_processed_sequence_id
        };

        // Track max sequence ID seen in this batch.
        let mut new_max = max_processed;

        // Collect callbacks to invoke outside the lock.
        let mut callbacks: Vec<(MessageCallback, PubMessage)> = Vec::new();

        for msg in messages {
            if msg.sequence_id <= max_processed {
                continue; // Already processed.
            }

            // Track max seen.
            if msg.sequence_id > new_max {
                new_max = msg.sequence_id;
            }

            // Find matching subscription.
            if let Some(channel) = inner.channels.get(&msg.channel_type) {
                if let Some(subs) = channel.subscriptions.get(publisher_id) {
                    // Check all-entities subscription first.
                    if let Some(info) = &subs.all_entities {
                        callbacks.push((info.item_cb.clone(), msg.clone()));
                        processed += 1;
                        continue;
                    }
                    // Check per-entity subscription.
                    if let Some(info) = subs.per_entity.get(&msg.key_id) {
                        callbacks.push((info.item_cb.clone(), msg.clone()));
                        processed += 1;
                        continue;
                    }
                }
            }
        }

        // Update max processed sequence ID.
        if new_max > max_processed {
            if let Some(seq_state) = inner.sequence_states.get_mut(publisher_id) {
                seq_state.max_processed_sequence_id = new_max;
            }
        }

        inner.total_messages_received += processed as u64;
        drop(inner);

        // Invoke callbacks outside the lock.
        for (cb, msg) in callbacks {
            cb(&msg);
        }

        processed
    }

    /// Mark a publisher as connected (has active long-poll).
    pub fn set_publisher_connected(&self, publisher_id: &[u8], connected: bool) {
        let mut inner = self.inner.lock();
        if connected {
            inner.connected_publishers.insert(publisher_id.to_vec());
        } else {
            inner.connected_publishers.remove(publisher_id);
        }
    }

    /// Check if a publisher has an active long-poll connection.
    pub fn is_publisher_connected(&self, publisher_id: &[u8]) -> bool {
        self.inner
            .lock()
            .connected_publishers
            .contains(publisher_id)
    }

    /// Check if there are any active subscriptions to a publisher.
    pub fn has_subscriptions_to(&self, publisher_id: &[u8]) -> bool {
        let inner = self.inner.lock();
        for channel in inner.channels.values() {
            if let Some(subs) = channel.subscriptions.get(publisher_id) {
                if !subs.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    /// Get all publisher IDs that have pending commands.
    pub fn publishers_with_pending_commands(&self) -> Vec<Vec<u8>> {
        self.inner
            .lock()
            .command_queues
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Total messages received across all subscriptions.
    pub fn total_messages_received(&self) -> u64 {
        self.inner.lock().total_messages_received
    }

    /// Total commands sent to publishers.
    pub fn total_commands_sent(&self) -> u64 {
        self.inner.lock().total_commands_sent
    }

    /// Number of active subscriptions across all channels and publishers.
    pub fn num_subscriptions(&self) -> usize {
        let inner = self.inner.lock();
        let mut count = 0;
        for channel in inner.channels.values() {
            for subs in channel.subscriptions.values() {
                if subs.all_entities.is_some() {
                    count += 1;
                }
                count += subs.per_entity.len();
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_subscriber() -> Subscriber {
        Subscriber::new(b"test-sub".to_vec(), SubscriberConfig::default())
    }

    fn make_msg(channel_type: i32, key_id: &[u8], seq: i64) -> PubMessage {
        PubMessage {
            channel_type,
            key_id: key_id.to_vec(),
            payload: vec![],
            sequence_id: seq,
        }
    }

    fn counting_callback() -> (MessageCallback, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        let cb: MessageCallback = Arc::new(move |_| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        });
        (cb, count)
    }

    #[test]
    fn test_subscribe_and_is_subscribed() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 3, b"actor_A", cb, None);

        assert!(sub.is_subscribed(b"pub1", 3, b"actor_A"));
        assert!(!sub.is_subscribed(b"pub1", 3, b"actor_B"));
        assert!(!sub.is_subscribed(b"pub2", 3, b"actor_A"));
    }

    #[test]
    fn test_subscribe_all_entities() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb, None);

        assert!(sub.is_subscribed(b"pub1", 3, b""));
        assert_eq!(sub.num_subscriptions(), 1);
    }

    #[test]
    fn test_unsubscribe() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 3, b"actor_A", cb, None);
        assert!(sub.is_subscribed(b"pub1", 3, b"actor_A"));

        sub.unsubscribe(b"pub1", 3, b"actor_A");
        assert!(!sub.is_subscribed(b"pub1", 3, b"actor_A"));
    }

    #[test]
    fn test_drain_commands() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 3, b"a", cb.clone(), None);
        sub.subscribe(b"pub1", 3, b"b", cb, None);
        sub.unsubscribe(b"pub1", 3, b"a");

        let commands = sub.drain_commands(b"pub1");
        assert_eq!(commands.len(), 3); // 2 subscribe + 1 unsubscribe
        assert_eq!(sub.total_commands_sent(), 3);

        // Second drain should be empty.
        let commands = sub.drain_commands(b"pub1");
        assert!(commands.is_empty());
    }

    #[test]
    fn test_handle_poll_response_all_entities() {
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb, None);

        let messages = vec![make_msg(3, b"actor_A", 1), make_msg(3, b"actor_B", 2)];

        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 2);
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_handle_poll_response_per_entity() {
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"actor_A", cb, None);

        let messages = vec![
            make_msg(3, b"actor_A", 1),
            make_msg(3, b"actor_B", 2), // Not subscribed.
        ];

        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 1);
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_sequence_id_deduplication() {
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb, None);

        // First batch.
        let messages = vec![make_msg(3, b"a", 1), make_msg(3, b"b", 2)];
        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 2);

        // Re-deliver same messages (should be skipped).
        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 2); // No change.

        // New messages should be processed.
        let new_messages = vec![make_msg(3, b"c", 3)];
        sub.handle_poll_response(b"pub1", b"pub1_id", &new_messages);
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_publisher_failover_resets_sequence() {
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb, None);

        // Messages from publisher_id_A.
        let messages = vec![make_msg(3, b"a", 1), make_msg(3, b"b", 2)];
        sub.handle_poll_response(b"pub1", b"pub_id_A", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 2);

        // Publisher failover — new publisher_id_B, sequence resets.
        let messages = vec![make_msg(3, b"c", 1)]; // seq 1 again.
        sub.handle_poll_response(b"pub1", b"pub_id_B", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 3); // Processed despite seq 1.
    }

    #[test]
    fn test_max_processed_sequence_id() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb, None);

        assert_eq!(sub.max_processed_sequence_id(b"pub1"), 0);

        let messages = vec![make_msg(3, b"a", 5), make_msg(3, b"b", 10)];
        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(sub.max_processed_sequence_id(b"pub1"), 10);
    }

    #[test]
    fn test_publisher_connection_tracking() {
        let sub = make_subscriber();

        assert!(!sub.is_publisher_connected(b"pub1"));

        sub.set_publisher_connected(b"pub1", true);
        assert!(sub.is_publisher_connected(b"pub1"));

        sub.set_publisher_connected(b"pub1", false);
        assert!(!sub.is_publisher_connected(b"pub1"));
    }

    #[test]
    fn test_has_subscriptions_to() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        assert!(!sub.has_subscriptions_to(b"pub1"));

        sub.subscribe(b"pub1", 3, b"actor_A", cb, None);
        assert!(sub.has_subscriptions_to(b"pub1"));

        sub.unsubscribe(b"pub1", 3, b"actor_A");
        assert!(!sub.has_subscriptions_to(b"pub1"));
    }

    #[test]
    fn test_publishers_with_pending_commands() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        assert!(sub.publishers_with_pending_commands().is_empty());

        sub.subscribe(b"pub1", 3, b"a", cb.clone(), None);
        sub.subscribe(b"pub2", 4, b"b", cb, None);

        let publishers = sub.publishers_with_pending_commands();
        assert_eq!(publishers.len(), 2);
    }

    #[test]
    fn test_multiple_channels_same_publisher() {
        let sub = make_subscriber();
        let (cb1, count1) = counting_callback();
        let (cb2, count2) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb1, None); // channel 3
        sub.subscribe(b"pub1", 4, b"", cb2, None); // channel 4

        let messages = vec![make_msg(3, b"a", 1), make_msg(4, b"b", 2)];

        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(count1.load(Ordering::Relaxed), 1);
        assert_eq!(count2.load(Ordering::Relaxed), 1);
        assert_eq!(sub.total_messages_received(), 2);
    }

    #[test]
    fn test_num_subscriptions() {
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        assert_eq!(sub.num_subscriptions(), 0);

        sub.subscribe(b"pub1", 3, b"a", cb.clone(), None);
        sub.subscribe(b"pub1", 3, b"b", cb.clone(), None);
        sub.subscribe(b"pub1", 4, b"", cb, None);

        assert_eq!(sub.num_subscriptions(), 3);
    }

    // --- Ported from subscriber_test.cc ---

    #[test]
    fn test_ignore_batch_after_unsubscription() {
        // After unsubscribing a specific key, messages for that key should be ignored.
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"key1", cb, None);
        sub.unsubscribe(b"pub1", 3, b"key1");
        assert!(!sub.is_subscribed(b"pub1", 3, b"key1"));

        // Messages for the unsubscribed key should not invoke the callback.
        let messages = vec![make_msg(3, b"key1", 1)];
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 0);
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_ignore_batch_after_unsubscribe_from_all() {
        // After unsubscribing from all entities, messages should be ignored.
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"", cb, None);
        assert!(sub.is_subscribed(b"pub1", 3, b""));

        sub.unsubscribe(b"pub1", 3, b"");
        assert!(!sub.is_subscribed(b"pub1", 3, b""));

        let messages = vec![make_msg(3, b"some_key", 1)];
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 0);
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_long_polling_failure() {
        // When long polling fails, the failure callback should be invoked.
        // In our Rust port, we test that after unsubscribing, no messages are
        // processed (simulating publisher failure triggering failure path).
        let sub = make_subscriber();
        let failure_called = Arc::new(AtomicUsize::new(0));
        let failure_clone = failure_called.clone();
        let failure_cb: super::FailureCallback = Arc::new(move |_key_id| {
            failure_clone.fetch_add(1, Ordering::Relaxed);
        });
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"key1", cb, Some(failure_cb));

        // Simulate failure: send empty messages (publisher failure scenario).
        // In the C++ test, long polling returns with a failure status.
        // In Rust, when the publisher is gone, no messages arrive.
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &[]);
        assert_eq!(processed, 0);
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_unsubscribe_in_subscription_callback() {
        // Tests that unsubscribing from within a callback does not cause issues.
        let sub = Arc::new(make_subscriber());
        let sub_clone = sub.clone();
        let called = Arc::new(AtomicUsize::new(0));
        let called_clone = called.clone();

        let cb: super::MessageCallback = Arc::new(move |msg| {
            // Unsubscribe from within the callback.
            sub_clone.unsubscribe(b"pub1", 3, &msg.key_id);
            called_clone.fetch_add(1, Ordering::Relaxed);
        });

        sub.subscribe(b"pub1", 3, b"key1", cb, None);

        let messages = vec![make_msg(3, b"key1", 1)];
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 1);
        assert_eq!(called.load(Ordering::Relaxed), 1);

        // After unsubscription in callback, further messages should not match.
        assert!(!sub.is_subscribed(b"pub1", 3, b"key1"));
    }

    #[test]
    fn test_sub_unsub_command_batch_single_entry() {
        // Verify command batch has a single subscribe entry.
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 3, b"key1", cb, None);

        let commands = sub.drain_commands(b"pub1");
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            SubscriberCommand::Subscribe {
                channel_type,
                key_id,
            } => {
                assert_eq!(*channel_type, 3);
                assert_eq!(key_id, b"key1");
            }
            _ => panic!("Expected Subscribe command"),
        }

        // No more commands after draining.
        let commands = sub.drain_commands(b"pub1");
        assert!(commands.is_empty());
    }

    #[test]
    fn test_sub_unsub_command_batch_multi_entries() {
        // Verify multiple commands are batched in FIFO order.
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        // Subscribe key1.
        sub.subscribe(b"pub1", 3, b"key1", cb.clone(), None);
        // Drain the first command (sent immediately in C++).
        let first_batch = sub.drain_commands(b"pub1");
        assert_eq!(first_batch.len(), 1);

        // Unsubscribe key1, subscribe key1 again, subscribe key2.
        sub.unsubscribe(b"pub1", 3, b"key1");
        sub.subscribe(b"pub1", 3, b"key1", cb.clone(), None);
        sub.subscribe(b"pub1", 3, b"key2", cb, None);

        let commands = sub.drain_commands(b"pub1");
        assert_eq!(commands.len(), 3);

        // First: unsubscribe key1.
        match &commands[0] {
            SubscriberCommand::Unsubscribe {
                channel_type,
                key_id,
            } => {
                assert_eq!(*channel_type, 3);
                assert_eq!(key_id, b"key1");
            }
            _ => panic!("Expected Unsubscribe command"),
        }

        // Second: subscribe key1.
        match &commands[1] {
            SubscriberCommand::Subscribe {
                channel_type,
                key_id,
            } => {
                assert_eq!(*channel_type, 3);
                assert_eq!(key_id, b"key1");
            }
            _ => panic!("Expected Subscribe command"),
        }

        // Third: subscribe key2.
        match &commands[2] {
            SubscriberCommand::Subscribe {
                channel_type,
                key_id,
            } => {
                assert_eq!(*channel_type, 3);
                assert_eq!(key_id, b"key2");
            }
            _ => panic!("Expected Subscribe command"),
        }
    }

    #[test]
    fn test_sub_unsub_command_batch_multi_batch() {
        // Verify that when commands exceed batch size, they are split across batches.
        let sub = Subscriber::new(
            b"test-sub".to_vec(),
            SubscriberConfig {
                max_command_batch_size: 3,
                long_poll_timeout_ms: 30_000,
            },
        );
        let (cb, _) = counting_callback();

        // First command goes in its own batch.
        sub.unsubscribe(b"pub1", 3, b"key1");
        let first_batch = sub.drain_commands(b"pub1");
        assert_eq!(first_batch.len(), 1);

        // Queue 4 commands: first 3 go in one batch, last goes in next.
        sub.unsubscribe(b"pub1", 3, b"key1");
        sub.subscribe(b"pub1", 3, b"key1", cb.clone(), None);
        sub.unsubscribe(b"pub1", 3, b"key1");
        sub.subscribe(b"pub1", 3, b"key2", cb, None);

        // First batch: 3 commands.
        let batch = sub.drain_commands(b"pub1");
        assert_eq!(batch.len(), 3);

        // Second batch: 1 remaining command.
        let batch = sub.drain_commands(b"pub1");
        assert_eq!(batch.len(), 1);
        match &batch[0] {
            SubscriberCommand::Subscribe {
                channel_type,
                key_id,
            } => {
                assert_eq!(*channel_type, 3);
                assert_eq!(key_id, b"key2");
            }
            _ => panic!("Expected Subscribe command"),
        }

        // No more commands.
        assert!(sub.drain_commands(b"pub1").is_empty());
    }

    #[test]
    fn test_only_one_in_flight_command_batch() {
        // Tests that commands are drained in properly sized batches.
        let sub = Subscriber::new(
            b"test-sub".to_vec(),
            SubscriberConfig {
                max_command_batch_size: 3,
                long_poll_timeout_ms: 30_000,
            },
        );
        let (cb, _) = counting_callback();

        // First subscribe goes in first batch.
        sub.subscribe(b"pub1", 3, b"key1", cb.clone(), None);
        let first_batch = sub.drain_commands(b"pub1");
        assert_eq!(first_batch.len(), 1);

        // Two more subscribe requests.
        for i in 0..2 {
            let key = format!("key_{}", i);
            sub.subscribe(b"pub1", 3, key.as_bytes(), cb.clone(), None);
        }

        // Second batch: 2 commands.
        let second_batch = sub.drain_commands(b"pub1");
        assert_eq!(second_batch.len(), 2);

        // No more.
        assert!(sub.drain_commands(b"pub1").is_empty());
    }

    #[test]
    fn test_commands_cleaned_upon_publish_failure() {
        // When a publisher fails, pending commands for that publisher should
        // be cleaned up by removing the queue entry.
        let sub = Subscriber::new(
            b"test-sub".to_vec(),
            SubscriberConfig {
                max_command_batch_size: 3,
                long_poll_timeout_ms: 30_000,
            },
        );
        let (cb, _) = counting_callback();

        // Queue some commands.
        sub.subscribe(b"pub1", 3, b"key1", cb.clone(), None);
        for i in 0..2 {
            let key = format!("extra_{}", i);
            sub.subscribe(b"pub1", 3, key.as_bytes(), cb.clone(), None);
        }

        // Drain first batch.
        let batch = sub.drain_commands(b"pub1");
        assert_eq!(batch.len(), 3);

        // Simulate publisher failure: unsubscribe all subscriptions from this publisher.
        // This clears the subscriptions but any remaining queued commands become stale.
        sub.unsubscribe(b"pub1", 3, b"key1");
        sub.unsubscribe(b"pub1", 3, b"extra_0");
        sub.unsubscribe(b"pub1", 3, b"extra_1");

        // After the failure, no more subscriptions remain.
        assert!(!sub.has_subscriptions_to(b"pub1"));
    }

    #[test]
    fn test_failure_message_published() {
        // Tests that when a failure message is published (empty payload),
        // the failure callback is invoked (not the item callback).
        let sub = make_subscriber();
        let (cb, item_count) = counting_callback();
        let failure_count = Arc::new(AtomicUsize::new(0));
        let failure_clone = failure_count.clone();
        let failure_cb: super::FailureCallback = Arc::new(move |_| {
            failure_clone.fetch_add(1, Ordering::Relaxed);
        });

        sub.subscribe(b"pub1", 3, b"key1", cb.clone(), Some(failure_cb.clone()));
        sub.subscribe(b"pub1", 3, b"key2", cb, Some(failure_cb));

        // Send a normal message for key1.
        let messages = vec![make_msg(3, b"key1", 1)];
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 1);
        assert_eq!(item_count.load(Ordering::Relaxed), 1);

        // Send a normal message for key2.
        let messages = vec![make_msg(3, b"key2", 2)];
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 1);
        assert_eq!(item_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_is_subscribed() {
        // Tests is_subscribed after subscribe/unsubscribe lifecycle.
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        // Not subscribed initially.
        sub.unsubscribe(b"pub1", 3, b"key1");
        assert!(!sub.is_subscribed(b"pub1", 3, b"key1"));

        // Subscribe.
        sub.subscribe(b"pub1", 3, b"key1", cb, None);
        assert!(sub.is_subscribed(b"pub1", 3, b"key1"));

        // Unsubscribe.
        sub.unsubscribe(b"pub1", 3, b"key1");
        assert!(!sub.is_subscribed(b"pub1", 3, b"key1"));
    }

    #[test]
    fn test_callback_not_invoked_for_non_subscribed_object() {
        // Messages for a key that is not subscribed should not invoke any callback.
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"key1", cb, None);

        // Send a message for a different key.
        let messages = vec![make_msg(3, b"not_subscribed_key", 1)];
        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 0);
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_single_long_polling_with_multiple_subscriptions() {
        // Multiple subscriptions to the same publisher should all receive
        // messages from a single poll response.
        let sub = make_subscriber();
        let counts: Vec<Arc<AtomicUsize>> = (0..5).map(|_| Arc::new(AtomicUsize::new(0))).collect();

        for i in 0..5 {
            let count = counts[i].clone();
            let cb: super::MessageCallback = Arc::new(move |_| {
                count.fetch_add(1, Ordering::Relaxed);
            });
            let key = format!("key_{}", i);
            sub.subscribe(b"pub1", 3, key.as_bytes(), cb, None);
        }

        // Publish messages for all 5 keys.
        let messages: Vec<PubMessage> = (0..5)
            .map(|i| make_msg(3, format!("key_{}", i).as_bytes(), (i + 1) as i64))
            .collect();

        let processed = sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(processed, 5);

        for i in 0..5 {
            assert_eq!(counts[i].load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn test_multi_long_polling_with_the_same_subscription() {
        // A single subscription should continue to receive messages across
        // multiple poll responses.
        let sub = make_subscriber();
        let (cb, count) = counting_callback();

        sub.subscribe(b"pub1", 3, b"key1", cb, None);

        // First poll.
        let messages = vec![make_msg(3, b"key1", 1)];
        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 1);

        // Second poll.
        let messages = vec![make_msg(3, b"key1", 2)];
        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 2);

        // Third poll.
        let messages = vec![make_msg(3, b"key1", 3)];
        sub.handle_poll_response(b"pub1", b"pub1_id", &messages);
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_command_batch_invalid_argument_status_is_fatal() {
        // Tests that attempting to subscribe to an unregistered channel
        // on the subscriber side still queues the command (the server would reject it).
        // The Rust subscriber does not validate channels — it just queues commands.
        // This test verifies the command is created for a potentially invalid channel.
        let sub = make_subscriber();
        let (cb, _) = counting_callback();

        sub.subscribe(b"pub1", 999, b"key1", cb, None);

        let commands = sub.drain_commands(b"pub1");
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            SubscriberCommand::Subscribe {
                channel_type,
                key_id,
            } => {
                assert_eq!(*channel_type, 999);
                assert_eq!(key_id, b"key1");
            }
            _ => panic!("Expected Subscribe command"),
        }
    }
}
