//! GCS publish-subscribe messaging.
//!
//! Provides `GcsPublisher` for broadcasting state changes to subscribers,
//! and `PubSubManager` implementing the per-subscriber mailbox + long-poll
//! pattern used by the C++ Publisher.
//!
//! Maps C++ `pubsub_handler.h/cc` and `pubsub/gcs_publisher.h`.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::Notify;
use tracing::{debug, trace};

use gcs_proto::ray::rpc::PubMessage;

/// A published message with channel and payload.
#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub channel: String,
    pub key_id: Vec<u8>,
    pub data: Vec<u8>,
}

/// GCS publisher that broadcasts messages to subscribers via channels.
///
/// Maps the C++ `GcsPublisher` which wraps the internal `Publisher`.
/// In Rust we use `tokio::sync::broadcast` for fan-out.
pub struct GcsPublisher {
    tx: broadcast::Sender<PubSubMessage>,
}

impl GcsPublisher {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn publish(&self, msg: PubSubMessage) {
        // Ignore send errors (no subscribers).
        let _ = self.tx.send(msg);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PubSubMessage> {
        self.tx.subscribe()
    }

    /// Publish a worker failure notification.
    pub fn publish_worker_failure(&self, worker_id: &[u8], data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "WORKER_DELTA".into(),
            key_id: worker_id.to_vec(),
            data,
        });
    }

    /// Publish a job update.
    pub fn publish_job(&self, job_id: &[u8], data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "JOB".into(),
            key_id: job_id.to_vec(),
            data,
        });
    }

    /// Publish a node info update.
    pub fn publish_node_info(&self, node_id: &[u8], data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "NODE_INFO".into(),
            key_id: node_id.to_vec(),
            data,
        });
    }

    /// Publish an actor update.
    pub fn publish_actor(&self, actor_id: &[u8], data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "ACTOR".into(),
            key_id: actor_id.to_vec(),
            data,
        });
    }

    /// Publish an error info message (RAY_ERROR_INFO_CHANNEL = 7).
    /// Maps C++ `GcsPublisher::PublishError`.
    pub fn publish_error(&self, key_id: &str, data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "ERROR_INFO".into(),
            key_id: key_id.as_bytes().to_vec(),
            data,
        });
    }

    /// Publish a log batch (RAY_LOG_CHANNEL = 8).
    /// Maps C++ `GcsPublisher::PublishLogs`.
    pub fn publish_logs(&self, data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "LOG".into(),
            key_id: vec![],
            data,
        });
    }

    /// Publish node resource usage (RAY_NODE_RESOURCE_USAGE_CHANNEL = 9).
    /// Maps C++ `GcsPublisher::PublishNodeResource`.
    pub fn publish_node_resource_usage(&self, node_id: &[u8], data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "NODE_RESOURCE_USAGE".into(),
            key_id: node_id.to_vec(),
            data,
        });
    }

    /// Publish node address and liveness (GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL = 10).
    /// Maps C++ publisher on this channel for lightweight node state updates.
    pub fn publish_node_address_and_liveness(&self, node_id: &[u8], data: Vec<u8>) {
        self.publish(PubSubMessage {
            channel: "NODE_ADDRESS_AND_LIVENESS".into(),
            key_id: node_id.to_vec(),
            data,
        });
    }
}

// ---------------------------------------------------------------------------
// PubSubManager -- per-subscriber mailbox + long-poll
// ---------------------------------------------------------------------------

/// State for a single subscriber (mailbox + notification handle).
struct SubscriberState {
    /// Queued messages waiting to be delivered via the next long-poll.
    mailbox: VecDeque<PubMessage>,
    /// Wakes up a waiting `gcs_subscriber_poll` call.
    notify: Arc<Notify>,
    /// Set of (channel_type, key_id) subscriptions.
    /// An empty key_id means wildcard (subscribe to everything on that channel).
    subscriptions: HashSet<(i32, Vec<u8>)>,
    /// Highest sequence_id acknowledged by this subscriber.
    max_processed_sequence_id: i64,
}

impl SubscriberState {
    fn new() -> Self {
        Self {
            mailbox: VecDeque::new(),
            notify: Arc::new(Notify::new()),
            subscriptions: HashSet::new(),
            max_processed_sequence_id: 0,
        }
    }

    /// Drain all messages from the mailbox.
    fn drain_mailbox(&mut self) -> Vec<PubMessage> {
        self.mailbox.drain(..).collect()
    }

    /// Acknowledge processed messages.
    fn acknowledge(&mut self, max_seq: i64) {
        if max_seq > self.max_processed_sequence_id {
            self.max_processed_sequence_id = max_seq;
            // Remove already-acknowledged messages from the front.
            while let Some(front) = self.mailbox.front() {
                if front.sequence_id <= max_seq {
                    self.mailbox.pop_front();
                } else {
                    break;
                }
            }
        }
    }
}

/// The PubSub manager that implements the C++ Publisher pattern:
/// per-subscriber mailboxes with long-poll notification.
///
/// Thread-safe -- all fields are behind concurrent maps or atomics.
pub struct PubSubManager {
    /// subscriber_id -> SubscriberState
    subscribers: DashMap<Vec<u8>, SubscriberState>,
    /// channel_type -> set of subscriber_ids that subscribed to the whole channel (wildcard).
    channel_subscribers: DashMap<i32, HashSet<Vec<u8>>>,
    /// (channel_type, key_id) -> set of subscriber_ids that subscribed with a specific key.
    key_subscribers: DashMap<(i32, Vec<u8>), HashSet<Vec<u8>>>,
    /// Unique ID for this publisher instance. Changes on failover so subscribers
    /// can detect stale connections.
    publisher_id: Vec<u8>,
    /// Monotonically increasing sequence ID for published messages.
    next_sequence_id: AtomicI64,
}

impl PubSubManager {
    /// Create a new PubSubManager with the given publisher ID.
    pub fn new(publisher_id: Vec<u8>) -> Self {
        Self {
            subscribers: DashMap::new(),
            channel_subscribers: DashMap::new(),
            key_subscribers: DashMap::new(),
            publisher_id,
            next_sequence_id: AtomicI64::new(1),
        }
    }

    /// Get the publisher ID.
    pub fn publisher_id(&self) -> &[u8] {
        &self.publisher_id
    }

    /// Publish a message to all matching subscribers.
    pub fn publish(&self, mut msg: PubMessage) {
        // Assign a sequence ID.
        let seq = self.next_sequence_id.fetch_add(1, Ordering::Relaxed);
        msg.sequence_id = seq;

        let channel = msg.channel_type;
        let key = msg.key_id.clone();

        // Collect all target subscriber IDs.
        let mut target_subs = HashSet::new();

        // Wildcard subscribers for this channel.
        if let Some(subs) = self.channel_subscribers.get(&channel) {
            for sub_id in subs.iter() {
                target_subs.insert(sub_id.clone());
            }
        }

        // Key-specific subscribers.
        if !key.is_empty() {
            if let Some(subs) = self.key_subscribers.get(&(channel, key)) {
                for sub_id in subs.iter() {
                    target_subs.insert(sub_id.clone());
                }
            }
        }

        // Enqueue message to each subscriber's mailbox and notify.
        for sub_id in target_subs {
            if let Some(mut state) = self.subscribers.get_mut(&sub_id) {
                state.mailbox.push_back(msg.clone());
                state.notify.notify_one();
            }
        }

        trace!(channel, seq, "Published message to subscribers");
    }

    /// Register a subscription for the given subscriber.
    pub fn subscribe_channel(
        &self,
        subscriber_id: &[u8],
        channel_type: i32,
        key_id: Vec<u8>,
    ) {
        // Ensure subscriber exists.
        self.subscribers
            .entry(subscriber_id.to_vec())
            .or_insert_with(SubscriberState::new)
            .subscriptions
            .insert((channel_type, key_id.clone()));

        if key_id.is_empty() {
            // Wildcard subscription.
            self.channel_subscribers
                .entry(channel_type)
                .or_default()
                .insert(subscriber_id.to_vec());
        } else {
            // Key-specific subscription.
            self.key_subscribers
                .entry((channel_type, key_id))
                .or_default()
                .insert(subscriber_id.to_vec());
        }

        debug!(
            subscriber = hex_encode(subscriber_id),
            channel_type, "Subscription registered"
        );
    }

    /// Unsubscribe a subscriber from a channel.
    pub fn unsubscribe_channel(
        &self,
        subscriber_id: &[u8],
        channel_type: i32,
        key_id: Vec<u8>,
    ) {
        if let Some(mut state) = self.subscribers.get_mut(subscriber_id) {
            state.subscriptions.remove(&(channel_type, key_id.clone()));
        }

        if key_id.is_empty() {
            if let Some(mut subs) = self.channel_subscribers.get_mut(&channel_type) {
                subs.remove(subscriber_id);
            }
        } else {
            if let Some(mut subs) = self.key_subscribers.get_mut(&(channel_type, key_id)) {
                subs.remove(subscriber_id);
            }
        }

        debug!(
            subscriber = hex_encode(subscriber_id),
            channel_type, "Subscription removed"
        );
    }

    /// Returns `true` if `subscriber_id` is registered. Primarily for tests
    /// and lifecycle-cleanup introspection (e.g. verifying that a
    /// `worker_dead` or `node_removed` listener cleaned the subscriber).
    pub fn has_subscriber(&self, subscriber_id: &[u8]) -> bool {
        self.subscribers.contains_key(subscriber_id)
    }

    /// Remove a subscriber entirely (cleanup on disconnect).
    pub fn remove_subscriber(&self, subscriber_id: &[u8]) {
        if let Some((_, state)) = self.subscribers.remove(subscriber_id) {
            // Clean up channel_subscribers and key_subscribers.
            for (channel_type, key_id) in &state.subscriptions {
                if key_id.is_empty() {
                    if let Some(mut subs) = self.channel_subscribers.get_mut(channel_type) {
                        subs.remove(subscriber_id);
                    }
                } else {
                    if let Some(mut subs) =
                        self.key_subscribers.get_mut(&(*channel_type, key_id.clone()))
                    {
                        subs.remove(subscriber_id);
                    }
                }
            }
        }
    }

    /// Long-poll: wait for messages for the given subscriber.
    /// Returns immediately if messages are already queued.
    /// Otherwise blocks until messages arrive or timeout.
    pub async fn poll(
        &self,
        subscriber_id: &[u8],
        max_processed_sequence_id: i64,
        timeout: std::time::Duration,
    ) -> Vec<PubMessage> {
        // Get or create subscriber state, acknowledge, check for messages.
        let notify = {
            let mut entry = self
                .subscribers
                .entry(subscriber_id.to_vec())
                .or_insert_with(SubscriberState::new);

            // Acknowledge processed messages.
            entry.acknowledge(max_processed_sequence_id);

            // If messages are already queued, return immediately.
            if !entry.mailbox.is_empty() {
                return entry.drain_mailbox();
            }
            entry.notify.clone()
        };

        // Wait for messages (with timeout).
        tokio::select! {
            _ = notify.notified() => {},
            _ = tokio::time::sleep(timeout) => {},
        }

        // Drain whatever messages arrived.
        self.subscribers
            .get_mut(subscriber_id)
            .map(|mut s| s.drain_mailbox())
            .unwrap_or_default()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_and_subscribe() {
        let publisher = GcsPublisher::new(16);
        let mut rx = publisher.subscribe();

        publisher.publish(PubSubMessage {
            channel: "TEST".into(),
            key_id: b"key1".to_vec(),
            data: b"data1".to_vec(),
        });

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "TEST");
        assert_eq!(msg.key_id, b"key1");
        assert_eq!(msg.data, b"data1");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let publisher = GcsPublisher::new(16);
        let mut rx1 = publisher.subscribe();
        let mut rx2 = publisher.subscribe();

        publisher.publish(PubSubMessage {
            channel: "CH".into(),
            key_id: vec![],
            data: b"hello".to_vec(),
        });

        let m1 = rx1.recv().await.unwrap();
        let m2 = rx2.recv().await.unwrap();
        assert_eq!(m1.data, b"hello");
        assert_eq!(m2.data, b"hello");
    }

    #[tokio::test]
    async fn test_no_subscriber_does_not_panic() {
        let publisher = GcsPublisher::new(16);
        // Publishing with no subscribers should not panic.
        publisher.publish_job(b"job1", b"data".to_vec());
    }

    // PubSubManager tests

    #[tokio::test]
    async fn test_pubsub_manager_basic() {
        let mgr = PubSubManager::new(b"pub1".to_vec());

        // Subscribe to channel 3 (GCS_ACTOR_CHANNEL) wildcard.
        mgr.subscribe_channel(b"sub1", 3, vec![]);

        // Publish a message.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"actor1".to_vec(),
            sequence_id: 0, // Will be overwritten.
            inner_message: None,
        });

        // Poll should return the message immediately.
        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_millis(100))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].channel_type, 3);
        assert_eq!(msgs[0].key_id, b"actor1");
        assert!(msgs[0].sequence_id > 0);
    }

    #[tokio::test]
    async fn test_pubsub_manager_long_poll_timeout() {
        let mgr = PubSubManager::new(b"pub1".to_vec());
        mgr.subscribe_channel(b"sub1", 3, vec![]);

        // Poll with no messages should timeout.
        let start = std::time::Instant::now();
        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_millis(50))
            .await;
        assert!(msgs.is_empty());
        assert!(start.elapsed() >= std::time::Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_pubsub_manager_long_poll_wakeup() {
        let mgr = Arc::new(PubSubManager::new(b"pub1".to_vec()));
        mgr.subscribe_channel(b"sub1", 3, vec![]);

        let mgr2 = mgr.clone();
        // Spawn a task that publishes after a short delay.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            mgr2.publish(PubMessage {
                channel_type: 3,
                key_id: b"actor1".to_vec(),
                sequence_id: 0,
                inner_message: None,
            });
        });

        let start = std::time::Instant::now();
        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_secs(5))
            .await;
        assert_eq!(msgs.len(), 1);
        // Should have completed well before the 5s timeout.
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_pubsub_manager_key_subscription() {
        let mgr = PubSubManager::new(b"pub1".to_vec());

        // Subscribe to a specific key.
        mgr.subscribe_channel(b"sub1", 3, b"actor1".to_vec());

        // Publish to matching key.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"actor1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        // Publish to non-matching key -- should NOT be delivered.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"actor2".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_millis(50))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].key_id, b"actor1");
    }

    #[tokio::test]
    async fn test_pubsub_manager_acknowledge() {
        let mgr = PubSubManager::new(b"pub1".to_vec());
        mgr.subscribe_channel(b"sub1", 3, vec![]);

        // Publish two messages.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"a1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"a2".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        // Poll acknowledging seq 0 (nothing acknowledged yet).
        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_millis(50))
            .await;
        assert_eq!(msgs.len(), 2);
        let seq1 = msgs[0].sequence_id;

        // Publish another.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"a3".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        // Poll acknowledging up to seq1 -- should only get a3.
        let msgs = mgr
            .poll(b"sub1", seq1, std::time::Duration::from_millis(50))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].key_id, b"a3");
    }

    #[tokio::test]
    async fn test_pubsub_manager_unsubscribe() {
        let mgr = PubSubManager::new(b"pub1".to_vec());
        mgr.subscribe_channel(b"sub1", 3, vec![]);

        mgr.unsubscribe_channel(b"sub1", 3, vec![]);

        // Publish should not deliver.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"a1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_millis(50))
            .await;
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn test_pubsub_manager_remove_subscriber() {
        let mgr = PubSubManager::new(b"pub1".to_vec());
        mgr.subscribe_channel(b"sub1", 3, vec![]);
        mgr.subscribe_channel(b"sub1", 4, vec![]);

        mgr.remove_subscriber(b"sub1");

        // Publish should not deliver.
        mgr.publish(PubMessage {
            channel_type: 3,
            key_id: b"a1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        // Poll creates a new (empty) subscriber, should timeout.
        let msgs = mgr
            .poll(b"sub1", 0, std::time::Duration::from_millis(50))
            .await;
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn test_publish_node_info() {
        let publisher = GcsPublisher::new(16);
        let mut rx = publisher.subscribe();

        publisher.publish_node_info(b"node1", b"node_data".to_vec());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "NODE_INFO");
        assert_eq!(msg.key_id, b"node1");
        assert_eq!(msg.data, b"node_data");
    }

    #[tokio::test]
    async fn test_publish_actor() {
        let publisher = GcsPublisher::new(16);
        let mut rx = publisher.subscribe();

        publisher.publish_actor(b"actor1", b"actor_data".to_vec());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "ACTOR");
        assert_eq!(msg.key_id, b"actor1");
        assert_eq!(msg.data, b"actor_data");
    }

    #[tokio::test]
    async fn test_publisher_id() {
        let mgr = PubSubManager::new(b"my_publisher_42".to_vec());
        assert_eq!(mgr.publisher_id(), b"my_publisher_42");
    }
}
